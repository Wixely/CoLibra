using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace CoLibra.Messaging.LiteNetLib;

/// <summary>DI entry point for the LiteNetLib UDP data plane.</summary>
public static class LiteNetLibServiceCollectionExtensions
{
    /// <summary>
    /// Registers the LiteNetLib engine so CoLibra's Messenger can use the resilient-UDP data
    /// plane. Pair with <c>options.Messaging.PreferUdp = true</c>; without it (or on peers that
    /// don't run the engine) messages simply take the TCP path.
    /// </summary>
    public static IServiceCollection AddCoLibraUdpMessaging(this IServiceCollection services)
    {
        services.TryAddSingleton<IUdpMessagingEngine, LiteNetLibEngine>();
        return services;
    }
}
