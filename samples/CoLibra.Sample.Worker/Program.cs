using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace CoLibra.Sample.Worker;

internal static class Program
{
    private static async Task Main(string[] args)
    {
        var builder = Host.CreateApplicationBuilder(args);
        builder.Logging.SetMinimumLevel(LogLevel.Information);

        builder.Services.AddCoLibra(options =>
        {
            options.ServiceId = "colibra-sample-worker";
            options.SharedSecret = "sample-secret-change-me";
            options.ServiceVersion = new Version(1, 0, 0);
        });

        builder.Services.AddHostedService<SourceProcessor>();

        await builder.Build().RunAsync();
    }
}
