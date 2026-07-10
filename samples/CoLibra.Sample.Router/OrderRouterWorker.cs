using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace CoLibra.Sample.Router;

/// <summary>The payload routed between nodes — serialized by the configured IRoutedPayloadSerializer (System.Text.Json by default).</summary>
internal sealed record OrderUpdate(string Channel, double Value, DateTimeOffset At);

/// <summary>
/// Simulates one node behind a load balancer: an ingress loop generates order updates that
/// only this instance receives, and hands each one to the Router. A registered handler
/// processes the updates for whichever orders this node ends up owning.
/// </summary>
internal sealed class OrderRouterWorker(ICoLibraCluster cluster, ILogger<OrderRouterWorker> logger) : BackgroundService
{
    private const int OrderCount = 20;

    private long _received;
    private long _deliveredLocal;
    private long _deliveredRemote;
    private long _failed;
    private long _processed;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        cluster.MembershipChanged += (_, e) =>
            logger.LogInformation("Cluster now has {Count} node(s)", e.Members.Count);

        logger.LogInformation("Node {NodeId}: discovering peers...", cluster.LocalNodeId);
        await cluster.WaitForClusterAsync(stoppingToken);

        // Registering the handler advertises this node as an assignment candidate for "order".
        // The typed overload deserializes each payload back into an OrderUpdate.
        await using var registration = cluster.Router.RegisterHandler<OrderUpdate>("order", (delivery, _) =>
        {
            Interlocked.Increment(ref _processed);
            logger.LogDebug("Processing {Order}: {Channel}={Value} at {At} (from {Origin})",
                delivery.Key.Id, delivery.Value.Channel, delivery.Value.Value, delivery.Value.At, delivery.Origin);
            return ValueTask.CompletedTask;
        });
        logger.LogInformation("Up as {State}; ingesting updates for {Orders} orders", cluster.State, OrderCount);

        var status = ReportStatusAsync(stoppingToken);
        var random = new Random();
        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                // This update arrived at THIS node only (the "load balancer" picked us).
                var orderId = $"order_{random.Next(1, OrderCount + 1):D3}";
                var update = new OrderUpdate("channel-a", Math.Round(random.NextDouble() * 100, 2), DateTimeOffset.UtcNow);
                Interlocked.Increment(ref _received);

                var result = await cluster.Router.RouteAsync("order", orderId, update, stoppingToken);
                _ = result.Status switch
                {
                    RouteStatus.DeliveredLocal => Interlocked.Increment(ref _deliveredLocal),
                    RouteStatus.Delivered => Interlocked.Increment(ref _deliveredRemote),
                    _ => Interlocked.Increment(ref _failed),
                };
                if (!result.Delivered)
                    logger.LogWarning("Route of {Order} failed: {Status}", orderId, result.Status);

                await Task.Delay(TimeSpan.FromMilliseconds(random.Next(100, 300)), stoppingToken);
            }
        }
        catch (OperationCanceledException)
        {
        }

        await status.ContinueWith(_ => { }, CancellationToken.None);
    }

    private async Task ReportStatusAsync(CancellationToken ct)
    {
        try
        {
            while (!ct.IsCancellationRequested)
            {
                await Task.Delay(TimeSpan.FromSeconds(5), ct);
                var owned = cluster.HeldLeases.Count(k => k.Type == "order");
                logger.LogInformation(
                    "[{State}] {Nodes} node(s) | ingress {Received:N0} | routed here {Local:N0} / away {Remote:N0} / failed {Failed:N0} | owning {Owned} orders, processed {Processed:N0}",
                    cluster.State, cluster.Members.Count,
                    Interlocked.Read(ref _received),
                    Interlocked.Read(ref _deliveredLocal),
                    Interlocked.Read(ref _deliveredRemote),
                    Interlocked.Read(ref _failed),
                    owned,
                    Interlocked.Read(ref _processed));
            }
        }
        catch (OperationCanceledException)
        {
        }
    }
}
