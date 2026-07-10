using CoLibra.Messaging.LiteNetLib;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace CoLibra.Sample.Chat;

// CoLibra chat sample: direct node-to-node messaging.
//
// Every instance is a chat participant. Instances discover each other automatically, the
// member list (with names) is the address book, and messages travel over the encrypted mesh.
//
//   dotnet run -- --Name alice          # pick your username (defaults to OS user + suffix)
//   dotnet run -- --Name alice --Udp true
//     Adds the LiteNetLib resilient-UDP data plane: chat lines stay ReliableOrdered while a
//     "positions" channel streams 20 Hz Sequenced (latest-wins) updates — the game-server
//     traffic shape. Instances without --Udp still interoperate via automatic TCP fallback.
//
// In the chat:
//   hello everyone        sends to every participant
//   @bob hi there         sends only to nodes named "bob"
//   /who                  lists participants (name + node id)
//   /quit                 exits
//
// Redirected/headless mode (for demos and CI): --Say "text" broadcasts the text every 3 s.
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
        // Quieten the library's own logger (exact category "CoLibra") so the chat stays readable.
        builder.Logging.AddFilter((category, level) =>
            category == "CoLibra" ? level >= LogLevel.Warning : level >= LogLevel.Information);

        var name = builder.Configuration.GetValue<string?>("Name", null)
            ?? $"{Environment.UserName}-{Guid.NewGuid().ToString("N")[..4]}";
        var udp = builder.Configuration.GetValue("Udp", false);

        builder.Services.AddCoLibra(options =>
        {
            options.ServiceId = "colibra-sample-chat";
            options.SharedSecret = "chat-demo-secret-change-me";
            options.ServiceVersion = new Version(1, 0, 0);
            options.NodeName = name;
            options.Messaging.Enabled = true;
            options.Messaging.PreferUdp = udp;
            options.MemberTimeout = TimeSpan.FromSeconds(3);
        });
        if (udp)
            builder.Services.AddCoLibraUdpMessaging();

        builder.Services.AddSingleton(new ChatSettings(name, builder.Configuration.GetValue<string?>("Say", null), udp));
        builder.Services.AddHostedService<ChatWorker>();

        await builder.Build().RunAsync();
    }
}

internal sealed record ChatSettings(string Name, string? AutoSay, bool Udp);
