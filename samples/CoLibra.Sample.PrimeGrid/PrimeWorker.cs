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
/// bucket's lease is never released — the held lease is the cluster-wide "done" marker. If
/// this instance dies, its leases expire and the survivors recompute those buckets.
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

                if (blockedByOthers == 0 && !completeAnnounced)
                {
                    completeAnnounced = true;
                    logger.LogInformation(
                        "COMPLETE: every remaining bucket is sieved by this node. Mine: {Mine:N0}/{Total:N0} buckets, {Primes:N0} primes, {Elapsed:mm\\:ss} elapsed",
                        DoneCount, options.RangeCount, Interlocked.Read(ref _primesFound), _elapsed.Elapsed);
                }
                else if (blockedByOthers > 0)
                {
                    completeAnnounced = false; // other nodes still own buckets; stay watchful
                }

                // Wait for freed buckets (or just re-check periodically; denied answers are cached).
                await Task.WhenAny(rescan.WaitAsync(stoppingToken), Task.Delay(TimeSpan.FromSeconds(3), stoppingToken));
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

            // 'This' preference = work-stealing: whichever instance asks first gets the bucket,
            // so a faster machine naturally takes a bigger share of the number space.
            if (!await cluster.CanProcessAsync(options.LeaseType, start.ToString(), ProcessingPreference.This, ct))
            {
                blockedByOthers++;
                continue; // owned (or already completed) by another node; answer is cached locally
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
            // No release: holding the lease marks the bucket done for the whole cluster.
        }

        return blockedByOthers;
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
                    "[{State}] {Nodes} node(s) | my buckets {Mine:N0}/{Total:N0} | my primes {Primes:N0} | {Rate:N1}M numbers/s",
                    cluster.State,
                    cluster.Members.Count,
                    DoneCount,
                    options.RangeCount,
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
