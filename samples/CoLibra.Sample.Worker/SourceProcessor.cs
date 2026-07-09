using CoLibra;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace CoLibra.Sample.Worker;

/// <summary>
/// Simulates a pool of event sources fighting over ownership: every instance of this process
/// sees the same 8 source ids, but CoLibra guarantees each source is processed by exactly one
/// instance at a time. Kill the instance that owns a source and watch another pick it up.
/// </summary>
internal sealed class SourceProcessor(ICoLibraCluster cluster, ILogger<SourceProcessor> logger) : BackgroundService
{
    private static readonly string[] Sources =
        [.. Enumerable.Range(1, 8).Select(i => $"source_{i:D5}")];

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        cluster.MembershipChanged += (_, e) =>
            logger.LogInformation("Membership: {Count} nodes ({Joined} joined, {Left} left)",
                e.Members.Count, e.Joined.Count, e.Left.Count);
        cluster.LeaseLost += (_, e) =>
            logger.LogWarning("Lost ownership of {Key} ({Reason})", e.Key, e.Reason);
        cluster.LeaseAvailable += (_, e) =>
            logger.LogInformation("Sources now available: {Keys}", string.Join(", ", e.Keys));
        cluster.SplitBrainDetected += (_, e) =>
            logger.LogWarning("Split brain detected ({Kind}): {Detail}", e.Kind, e.Detail);

        logger.LogInformation("Node {NodeId} waiting to join a cluster...", cluster.LocalNodeId);
        await cluster.WaitForClusterAsync(stoppingToken);
        logger.LogInformation("Joined as {State}", cluster.State);

        while (!stoppingToken.IsCancellationRequested)
        {
            var owned = new List<string>();
            foreach (var source in Sources)
            {
                // The single primitive: negotiate on first ask, cached local read afterwards.
                if (await cluster.CanProcessAsync("sourceid", source, ProcessingPreference.Balanced, stoppingToken))
                {
                    owned.Add(source);
                    // ... process this source's events here ...
                }
            }

            logger.LogInformation("[{State}] {Count} nodes | processing {Owned}/{Total}: {List}",
                cluster.State, cluster.Members.Count, owned.Count, Sources.Length, string.Join(", ", owned));
            await Task.Delay(TimeSpan.FromSeconds(2), stoppingToken);
        }
    }
}
