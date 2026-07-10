using System.Collections.Concurrent;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace CoLibra.Sample.GameServer;

/// <summary>A player's periodic report to the server, sent by name over messaging.</summary>
internal sealed record ScoreReport(string Player, int ZonesOwned, long EventsProcessed, DateTimeOffset At);

internal sealed class GameWorker(
    ICoLibraCluster cluster,
    GameSettings settings,
    ILogger<GameWorker> logger) : BackgroundService
{
    private static readonly string[] Zones = [.. Enumerable.Range(1, 12).Select(i => $"zone_{i:D2}")];

    protected override Task ExecuteAsync(CancellationToken stoppingToken) =>
        settings.IsServer ? RunServerAsync(stoppingToken) : RunPlayerAsync(stoppingToken);

    // =====================================================================================
    // The authority: Forced coordinator, accepts no work, aggregates scores, rebalances.
    // =====================================================================================

    private async Task RunServerAsync(CancellationToken ct)
    {
        var scores = new ConcurrentDictionary<string, ScoreReport>();
        var membershipDirty = false;

        cluster.MembershipChanged += (_, e) =>
        {
            foreach (var joined in e.Joined)
                logger.LogInformation("* {Name} joined", e.Members.FirstOrDefault(m => m.NodeId == joined)?.Name ?? joined.ToString());
            foreach (var left in e.Left)
                logger.LogInformation("* a player left ({NodeId})", left);
            membershipDirty = true; // rebalance soon: newcomers get zones, leavers' zones re-home
        };

        await cluster.WaitForClusterAsync(ct);
        logger.LogInformation("Game server up as {State} (never accepts zone work itself)", cluster.State);

        await using var registration = cluster.Messenger.RegisterHandler<ScoreReport>("score", (message, _) =>
        {
            scores[message.Value.Player] = message.Value;
            return ValueTask.CompletedTask;
        });

        try
        {
            while (!ct.IsCancellationRequested)
            {
                await Task.Delay(TimeSpan.FromSeconds(2), ct);

                if (membershipDirty)
                {
                    membershipDirty = false;
                    // Coordinator-only; moves only the excess. On a balanced cluster it's a no-op,
                    // so reacting to every membership change is safe.
                    var result = await cluster.ForceRebalanceAsync(cancellationToken: ct);
                    if (result.LeasesRevoked > 0)
                        logger.LogInformation("Rebalanced: moved {Count} zone(s) off {Nodes} player(s)", result.LeasesRevoked, result.NodesShed);
                }

                var players = cluster.Members.Where(m => !m.IsCoordinator).ToList();
                var scoreboard = string.Join(" | ", players
                    .Select(p => scores.TryGetValue(p.Name ?? "", out var s)
                        ? $"{s.Player}: {s.ZonesOwned} zones, {s.EventsProcessed:N0} events"
                        : $"{p.Name}: (no report yet)"));
                logger.LogInformation("[{Players} player(s)] {Scoreboard}",
                    players.Count, scoreboard.Length == 0 ? "waiting for players..." : scoreboard);
            }
        }
        catch (OperationCanceledException)
        {
        }
    }

    // =====================================================================================
    // A player: Never coordinates, works zone leases, reports to the server by name.
    // =====================================================================================

    private async Task RunPlayerAsync(CancellationToken ct)
    {
        cluster.LeaseLost += (_, e) =>
        {
            if (e.Reason == LeaseLossReason.Rebalanced)
                logger.LogInformation("~ zone {Zone} moved away by the server's rebalance", e.Key.Id);
        };

        logger.LogInformation("Player '{Name}' looking for the game server...", settings.Name);
        await cluster.WaitForClusterAsync(ct);
        logger.LogInformation("Joined as {State}", cluster.State);

        long processed = 0;
        try
        {
            while (!ct.IsCancellationRequested)
            {
                var owned = 0;
                foreach (var zone in Zones)
                {
                    // Balanced steering spreads zones across players; the server never gets any
                    // (AcceptWork = false keeps it out of the candidate math entirely).
                    if (await cluster.CanProcessAsync("zone", zone, ProcessingPreference.Balanced, ct))
                    {
                        owned++;
                        processed++; // "simulate" handling this zone's events this tick
                    }
                }

                var report = new ScoreReport(settings.Name, owned, processed, DateTimeOffset.UtcNow);
                await cluster.Messenger.SendByNameAsync("game-server", "score", report, cancellationToken: ct);

                logger.LogInformation("[{State}] owning {Owned}/{Total} zones | {Processed:N0} events processed",
                    cluster.State, owned, Zones.Length, processed);
                await Task.Delay(TimeSpan.FromSeconds(2), ct);
            }
        }
        catch (OperationCanceledException)
        {
        }
    }
}
