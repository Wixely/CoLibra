using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace CoLibra.Sample.Chat;

/// <summary>The typed payload exchanged between participants.</summary>
internal sealed record ChatLine(string Text, DateTimeOffset At);

internal sealed class ChatWorker(
    ICoLibraCluster cluster,
    ChatSettings settings,
    ILogger<ChatWorker> logger,
    IHostApplicationLifetime lifetime) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        cluster.MembershipChanged += (_, e) =>
        {
            foreach (var joined in e.Joined)
            {
                var member = e.Members.FirstOrDefault(m => m.NodeId == joined);
                logger.LogInformation("* {Name} joined", member?.Name ?? joined.ToString());
            }

            foreach (var left in e.Left)
                logger.LogInformation("* a participant left ({NodeId})", left);
        };

        logger.LogInformation("Joining chat as '{Name}'...", settings.Name);
        await cluster.WaitForClusterAsync(stoppingToken);

        // The inbox: whoever sends to our id (or our name) lands here.
        await using var registration = cluster.Messenger.RegisterHandler<ChatLine>("chat", (message, _) =>
        {
            logger.LogInformation("<{From}> {Text}", message.OriginName ?? message.Origin.ToString(), message.Value.Text);
            return ValueTask.CompletedTask;
        });

        logger.LogInformation("Connected. {Count} participant(s) online. Type to chat, '@name text' for direct, /who, /quit.",
            cluster.Members.Count);

        if (Console.IsInputRedirected)
            await RunHeadlessAsync(stoppingToken);
        else
            await RunInteractiveAsync(stoppingToken);
    }

    /// <summary>Broadcast = one direct send per member; the member list is the address book.</summary>
    private async Task BroadcastAsync(string text, CancellationToken ct)
    {
        var line = new ChatLine(text, DateTimeOffset.UtcNow);
        foreach (var member in cluster.Members.Where(m => m.NodeId != cluster.LocalNodeId))
        {
            var result = await cluster.Messenger.SendAsync(member.NodeId, "chat", line, ct);
            if (!result.Delivered)
                logger.LogWarning("(to {Name}: {Status})", member.Name ?? member.NodeId.ToString(), result.Status);
        }
    }

    private async Task RunInteractiveAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            var input = await Task.Run(Console.ReadLine, ct);
            if (string.IsNullOrWhiteSpace(input))
                continue;

            if (input == "/quit")
            {
                lifetime.StopApplication();
                return;
            }

            if (input == "/who")
            {
                foreach (var member in cluster.Members)
                {
                    logger.LogInformation("  {Name} ({NodeId}){Self}",
                        member.Name ?? "<unnamed>", member.NodeId,
                        member.NodeId == cluster.LocalNodeId ? " <- you" : "");
                }

                continue;
            }

            if (input.StartsWith('@') && input.IndexOf(' ') is > 1 and var space)
            {
                var target = input[1..space];
                var text = input[(space + 1)..];
                var results = await cluster.Messenger.SendByNameAsync(
                    target, "chat", new ChatLine(text, DateTimeOffset.UtcNow), ct);
                if (results.Count == 0)
                    logger.LogWarning("(nobody here is named '{Target}' — /who lists participants)", target);
                continue;
            }

            await BroadcastAsync(input, ct);
        }
    }

    private async Task RunHeadlessAsync(CancellationToken ct)
    {
        try
        {
            while (!ct.IsCancellationRequested)
            {
                await Task.Delay(TimeSpan.FromSeconds(3), ct);
                if (settings.AutoSay is { } text)
                    await BroadcastAsync($"{text} (from {settings.Name})", ct);
            }
        }
        catch (OperationCanceledException)
        {
        }
    }
}
