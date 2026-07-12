using System.Diagnostics;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace CoLibra.Sample.PrimeGrid;

/// <summary>Settings for the distributed prime run; all instances must use the same values.</summary>
internal sealed class PrimeGridOptions
{
    public required long RangeSize { get; init; }
    public required long Target { get; init; }

    public long RangeCount => (Target + RangeSize - 1) / RangeSize;

    /// <summary>Embeds the bucket size so mismatched instances never share (and never corrupt) a run.</summary>
    public string LeaseType => $"prime-range-{RangeSize}";
}

/// <summary>
/// Scans the bucket list and sieves every bucket CoLibra grants to this instance. A finished
/// bucket is recorded with MarkCompletedAsync, which replicates the fact to every node — so
/// if this instance dies, survivors skip its finished buckets and recompute only the bucket
/// that was in flight.
/// </summary>
internal sealed class PrimeWorker(
    ICoLibraCluster cluster,
    PrimeGridOptions options,
    ILogger<PrimeWorker> logger) : BackgroundService
{
    private readonly Dictionary<long, long> _doneBuckets = []; // bucket start -> primes found in it
    private readonly Lock _doneLock = new();
    private readonly Stopwatch _elapsed = new();
    private long _primesFound;
    private long _numbersScanned;

    private int DoneCount
    {
        get
        {
            lock (_doneLock)
            {
                return _doneBuckets.Count;
            }
        }
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        WireEventLogging();

        logger.LogInformation("Node {NodeId}: discovering peers...", cluster.LocalNodeId);
        await cluster.WaitForClusterAsync(stoppingToken);
        logger.LogInformation(
            "Prime grid up as {State}: counting primes below {Target:N0} in {Ranges:N0} buckets of {RangeSize:N0}",
            cluster.State, options.Target, options.RangeCount, options.RangeSize);

        var basePrimes = SegmentedSieve.BasePrimes(options.Target);
        var rescan = new SemaphoreSlim(0, 1);
        cluster.LeaseAvailable += (_, e) =>
        {
            // Another node's buckets were freed (it died or released); sweep again right away.
            if (e.Keys.Any(k => k.Type == options.LeaseType) && rescan.CurrentCount == 0)
                rescan.Release();
        };

        _elapsed.Start();
        var status = ReportStatusAsync(stoppingToken);
        var completeAnnounced = false;

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                var blockedByOthers = await SweepAsync(basePrimes, stoppingToken);
                var clusterDone = CountClusterDone();

                if (clusterDone == options.RangeCount && !completeAnnounced)
                {
                    completeAnnounced = true;
                    logger.LogInformation(
                        "CLUSTER COMPLETE: all {Total:N0} buckets sieved. This node contributed {Mine:N0} buckets and {Primes:N0} primes ({Elapsed:mm\\:ss} elapsed)",
                        options.RangeCount, DoneCount, Interlocked.Read(ref _primesFound), _elapsed.Elapsed);
                }
                else if (blockedByOthers > 0)
                {
                    completeAnnounced = false; // other nodes still own buckets; stay watchful
                }

                // Wait for freed buckets (or just re-check periodically; denied answers are cached).
                // Cancel the loser of the race so we don't abandon a SemaphoreSlim waiter each poll —
                // an abandoned waiter would consume the next Release, swallowing the wake-up (and they
                // accumulate).
                using var wake = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
                await Task.WhenAny(rescan.WaitAsync(wake.Token), Task.Delay(TimeSpan.FromSeconds(3), wake.Token));
                wake.Cancel();
            }
        }
        catch (OperationCanceledException)
        {
        }

        await status.ContinueWith(_ => { }, CancellationToken.None);
    }

    /// <summary>One pass over all buckets; returns how many are currently owned by other nodes.</summary>
    private async Task<long> SweepAsync(IReadOnlyList<long> basePrimes, CancellationToken ct)
    {
        long blockedByOthers = 0;
        for (long start = 0; start < options.Target && !ct.IsCancellationRequested; start += options.RangeSize)
        {
            lock (_doneLock)
            {
                if (_doneBuckets.ContainsKey(start))
                    continue;
            }

            // Finished by any node, alive or dead: the replicated registry answers locally.
            if (cluster.IsCompleted(options.LeaseType, start.ToString()))
                continue;

            // 'This' preference = work-stealing: whichever instance asks first gets the bucket,
            // so a faster machine naturally takes a bigger share of the number space.
            if (!await cluster.CanProcessAsync(options.LeaseType, start.ToString(), ProcessingPreference.This, ct))
            {
                blockedByOthers++;
                continue; // owned by another node right now; answer is cached locally
            }

            var end = Math.Min(start + options.RangeSize, options.Target);
            var (count, largest) = SegmentedSieve.SieveRange(start, end, basePrimes);
            lock (_doneLock)
            {
                _doneBuckets[start] = count;
            }

            Interlocked.Add(ref _primesFound, count);
            Interlocked.Add(ref _numbersScanned, end - start);
            logger.LogDebug("Bucket {Start:N0}: {Count:N0} primes (largest {Largest:N0})", start, count, largest);

            // Cluster-wide "done, forever" — releases the lease and replicates the completion.
            await cluster.MarkCompletedAsync(options.LeaseType, start.ToString(), ct);
        }

        return blockedByOthers;
    }

    /// <summary>Buckets finished cluster-wide: sieved locally or completed by any (live or dead) node.</summary>
    private long CountClusterDone()
    {
        long done = 0;
        for (long start = 0; start < options.Target; start += options.RangeSize)
        {
            bool mine;
            lock (_doneLock)
            {
                mine = _doneBuckets.ContainsKey(start);
            }

            if (mine || cluster.IsCompleted(options.LeaseType, start.ToString()))
                done++;
        }

        return done;
    }

    private async Task ReportStatusAsync(CancellationToken ct)
    {
        try
        {
            while (!ct.IsCancellationRequested)
            {
                await Task.Delay(TimeSpan.FromSeconds(5), ct);
                var scanned = Interlocked.Read(ref _numbersScanned);
                logger.LogInformation(
                    "[{State}] {Nodes} node(s) | cluster {Done:N0}/{Total:N0} | my buckets {Mine:N0} | my primes {Primes:N0} | {Rate:N1}M numbers/s",
                    cluster.State,
                    cluster.Members.Count,
                    CountClusterDone(),
                    options.RangeCount,
                    DoneCount,
                    Interlocked.Read(ref _primesFound),
                    scanned / Math.Max(_elapsed.Elapsed.TotalSeconds, 0.001) / 1_000_000);
            }
        }
        catch (OperationCanceledException)
        {
        }
    }

    private void WireEventLogging()
    {
        cluster.MembershipChanged += (_, e) =>
            logger.LogInformation("Cluster now has {Count} node(s){Joined}{Left}",
                e.Members.Count,
                e.Joined.Count > 0 ? $" (+{e.Joined.Count} joined)" : "",
                e.Left.Count > 0 ? $" (-{e.Left.Count} left)" : "");
        cluster.StateChanged += (_, e) =>
            logger.LogInformation("Node state: {Previous} -> {Current}", e.Previous, e.Current);
        cluster.LeaseLost += (_, e) =>
        {
            logger.LogWarning("Lost bucket {Key} ({Reason}) — another node will recompute it", e.Key.Id, e.Reason);
            // Un-count it locally: whoever recomputes the bucket owns its primes now, keeping
            // the cluster-wide sum exact even through lease losses.
            if (e.Key.Type == options.LeaseType && long.TryParse(e.Key.Id, out var start))
            {
                lock (_doneLock)
                {
                    if (_doneBuckets.Remove(start, out var count))
                    {
                        Interlocked.Add(ref _primesFound, -count);
                        Interlocked.Add(ref _numbersScanned, -(Math.Min(start + options.RangeSize, options.Target) - start));
                    }
                }
            }
        };
        cluster.SplitBrainDetected += (_, e) =>
            logger.LogWarning("SPLIT BRAIN ({Kind}): {Detail}", e.Kind, e.Detail);
    }
}
