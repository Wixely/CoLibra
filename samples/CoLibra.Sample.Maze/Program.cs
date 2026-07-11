using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace CoLibra.Sample.Maze;

// CoLibra maze sample: a multiplayer ASCII maze in 24-bit color.
//
//   dotnet run -- --Name alice
//
// The first player to start generates the maze (elected by an exclusive lease); everyone who
// joins afterwards requests the map over messaging and loads it, then spawns at a random open
// cell. Move with WASD / arrow keys; you are '@', other players are colored '●' dots, and
// every move is broadcast to the whole cluster. Q quits.
//
// Needs a truecolor terminal (Windows Terminal, most modern terminals). Run several copies on
// one machine or across a LAN — they discover each other automatically.
internal static class Program
{
    private static async Task Main(string[] args)
    {
        var builder = Host.CreateApplicationBuilder(args);
        // Sample-specific config file so all demos can coexist in one folder; command line wins.
        builder.Configuration.AddJsonFile("maze.json", optional: true, reloadOnChange: false);
        builder.Configuration.AddCommandLine(args);

        // The maze redraws the screen, so keep the library's own logging off the console.
        builder.Logging.ClearProviders();
        builder.Logging.AddSimpleConsole(o => o.SingleLine = true);
        builder.Logging.SetMinimumLevel(
            builder.Configuration.GetValue("Verbose", false) ? LogLevel.Information : LogLevel.Warning);

        var name = builder.Configuration.GetValue<string?>("Name", null)
            ?? $"{Environment.UserName}-{Guid.NewGuid().ToString("N")[..3]}";

        builder.Services.AddCoLibra(options =>
        {
            options.ServiceId = "colibra-sample-maze";
            options.SharedSecret = "maze-demo-secret-change-me";
            options.ServiceVersion = new Version(1, 0, 0);
            options.NodeName = name;
            options.Messaging.Enabled = true;
            options.MemberTimeout = TimeSpan.FromSeconds(3);
        });

        builder.Services.AddSingleton(new MazeSettings(name));
        builder.Services.AddHostedService<MazeGame>();

        await builder.Build().RunAsync();
    }
}

internal sealed record MazeSettings(string Name);
