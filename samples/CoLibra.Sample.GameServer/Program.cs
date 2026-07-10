using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace CoLibra.Sample.GameServer;

// CoLibra game-server sample: the asymmetric-cluster feature trio in one topology.
//
//   dotnet run -- --Server true          the authority: CoordinatorMode.Forced (IS the
//                                        coordinator, always) + AcceptWork = false (never
//                                        takes zone work) + auto ForceRebalanceAsync when
//                                        players join or leave.
//   dotnet run -- --Name player1         a player: CoordinatorMode.Never (member only,
//                                        never elected), works zone leases, reports its
//                                        score to the server by name over messaging.
//
// Watch: zones distribute across PLAYERS only; the server coordinates and aggregates but
// never owns work. Start a late player and the server's forced rebalance immediately shifts
// zones to it (steer-only alone would leave the newcomer idle until natural churn). Kill a
// player and its zones migrate to the survivors.
internal static class Program
{
    private static async Task Main(string[] args)
    {
        var builder = Host.CreateApplicationBuilder(args);

        builder.Logging.ClearProviders();
        builder.Logging.AddSimpleConsole(o =>
        {
            o.SingleLine = true;
            o.TimestampFormat = "HH:mm:ss ";
        });
        // Quieten the library's own logger (exact category "CoLibra") so the demo output stays readable.
        builder.Logging.AddFilter((category, level) =>
            category == "CoLibra" ? level >= LogLevel.Warning : level >= LogLevel.Information);

        var isServer = builder.Configuration.GetValue("Server", false);
        var name = isServer
            ? "game-server"
            : builder.Configuration.GetValue<string?>("Name", null) ?? $"player-{Guid.NewGuid().ToString("N")[..4]}";

        builder.Services.AddCoLibra(options =>
        {
            options.ServiceId = "colibra-sample-gameserver";
            options.SharedSecret = "gameserver-demo-secret-change-me";
            options.ServiceVersion = new Version(1, 0, 0);
            options.NodeName = name;
            options.Messaging.Enabled = true;
            options.MemberTimeout = TimeSpan.FromSeconds(3);
            options.LeaseTtl = TimeSpan.FromSeconds(6);
            options.LeaseRenewSafetyMargin = TimeSpan.FromSeconds(2);

            if (isServer)
            {
                options.CoordinatorMode = CoordinatorMode.Forced; // the authority IS the coordinator
                options.AcceptWork = false;                       // and never takes zone work itself
            }
            else
            {
                options.CoordinatorMode = CoordinatorMode.Never;  // players never lead
            }
        });

        builder.Services.AddSingleton(new GameSettings(isServer, name));
        builder.Services.AddHostedService<GameWorker>();

        await builder.Build().RunAsync();
    }
}

internal sealed record GameSettings(bool IsServer, string Name);
