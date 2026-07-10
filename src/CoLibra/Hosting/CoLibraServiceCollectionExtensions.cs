using CoLibra.Runtime;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace CoLibra;

/// <summary>Dependency-injection entry points for CoLibra.</summary>
public static class CoLibraServiceCollectionExtensions
{
    /// <summary>
    /// Registers <see cref="ICoLibraCluster"/> as a singleton plus a hosted service that starts
    /// discovery and cluster participation with the host.
    /// </summary>
    public static IServiceCollection AddCoLibra(this IServiceCollection services, Action<CoLibraOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);

        services.AddOptions<CoLibraOptions>().Configure(configure);
        services.TryAddSingleton<IValidateOptions<CoLibraOptions>, CoLibraOptionsValidator>();
        services.TryAddSingleton(TimeProvider.System);
        services.TryAddSingleton(sp => new CoLibraNode(
            sp.GetRequiredService<IOptions<CoLibraOptions>>().Value,
            sp.GetRequiredService<ILoggerFactory>().CreateLogger("CoLibra"),
            sp.GetRequiredService<TimeProvider>(),
            udpEngine: sp.GetService<IUdpMessagingEngine>()));
        services.TryAddSingleton<ICoLibraCluster>(sp => sp.GetRequiredService<CoLibraNode>());
        services.AddHostedService<CoLibraHostedService>();
        return services;
    }
}

internal sealed class CoLibraHostedService(CoLibraNode node) : IHostedService
{
    public Task StartAsync(CancellationToken cancellationToken) => node.StartAsync(cancellationToken);

    public async Task StopAsync(CancellationToken cancellationToken) => await node.DisposeAsync().ConfigureAwait(false);
}
