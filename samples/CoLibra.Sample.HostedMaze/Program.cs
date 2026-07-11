using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace CoLibra.Sample.HostedMaze;

// CoLibra hosted-maze sample: the game IS the server, with seamless host migration.
//
//   dotnet run -- --Name alice
//
// The current CoLibra coordinator is the authoritative game host (crown ♔): it owns the maze
// and everyone's position, applies moves, and broadcasts the full state ~7 times a second.
// Other players send their moves to the host and render what it broadcasts. Move with
// WASD / arrows; Q quits.
//
// The point: kill whichever instance is the host and watch the crown jump to another player
// within a second or two, with no lost state and nobody dropped — CoLibra elects a new
// coordinator and, because every client already holds the last full snapshot, the newly
// elected host just keeps going. Run 3+ so there's always a successor.
//
// Needs a truecolor terminal (Windows Terminal or similar).
internal static class Program
{
    private static async Task Main(string[] args)
    {
        var builder = Host.CreateApplicationBuilder(args);
        builder.Configuration.AddJsonFile("hostedmaze.json", optional: true, reloadOnChange: false);
        builder.Configuration.AddCommandLine(args);

        builder.Logging.ClearProviders();
        builder.Logging.AddSimpleConsole(o => o.SingleLine = true);
        builder.Logging.SetMinimumLevel(
            builder.Configuration.GetValue("Verbose", false) ? LogLevel.Information : LogLevel.Warning);

        var name = builder.Configuration.GetValue<string?>("Name", null)
            ?? $"{Environment.UserName}-{Guid.NewGuid().ToString("N")[..3]}";

        builder.Services.AddCoLibra(options =>
        {
            options.ServiceId = "colibra-sample-hostedmaze";
            options.SharedSecret = "hostedmaze-demo-secret-change-me";
            options.ServiceVersion = new Version(1, 0, 0);
            options.NodeName = name;
            options.Messaging.Enabled = true;
            // Snappy host migration: detect a dead host and elect a successor within ~1-2 s.
            // AnnounceInterval must be shorter than DiscoveryWindow so a joiner hears the current
            // host and joins it cleanly instead of briefly self-promoting.
            options.AnnounceInterval = TimeSpan.FromMilliseconds(300);
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(400);
            options.MemberTimeout = TimeSpan.FromMilliseconds(1200);
            options.ElectionTimeout = TimeSpan.FromMilliseconds(800);
            options.DiscoveryWindow = TimeSpan.FromMilliseconds(900);
        });

        builder.Services.AddSingleton(new HostedMazeSettings(name));
        builder.Services.AddHostedService<HostedMazeGame>();

        await builder.Build().RunAsync();
    }
}

internal sealed record HostedMazeSettings(string Name);
