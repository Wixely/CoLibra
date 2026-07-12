namespace CoLibra;

/// <summary>
/// Settings for routed delivery: the advanced mode for topologies where only one node receives
/// a given piece of data (load balancers, partitioned queues). <see cref="ICoLibraRouter.RouteAsync(string, string, ReadOnlyMemory{byte}, CancellationToken)"/>
/// delivers a payload to the key's lease owner, and when the key is unowned the coordinator
/// force-assigns the lease to the least-loaded node that registered a handler for the type.
/// </summary>
public sealed class RoutingOptions
{
    /// <summary>Enables the feature. Default false — disabled it costs nothing.</summary>
    public bool Enabled { get; set; }

    /// <summary>Maximum routed payload size. Default 1 MiB; must leave headroom under the 4 MiB frame limit.</summary>
    public int MaxPayloadBytes { get; set; } = 1_048_576;

    /// <summary>End-to-end budget for one <see cref="ICoLibraRouter.RouteAsync(string, string, ReadOnlyMemory{byte}, CancellationToken)"/> call, including one re-resolve retry. Default 5 s.</summary>
    public TimeSpan DeliveryTimeout { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>Backstop TTL on the resolver's key→owner cache (push-invalidated on ownership changes). Default 30 s.</summary>
    public TimeSpan OwnerCacheTtl { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Hard cap on resolver owner-cache entries. Routed ids are often ever-new (event/session ids),
    /// so this bounds memory: when full, expired entries are dropped and then the soonest-to-expire
    /// evicted. Default 10 000.
    /// </summary>
    public int OwnerCacheMaxEntries { get; set; } = 10_000;

    /// <summary>
    /// Open direct member↔member channels for payloads (default). When false, every payload
    /// relays through the coordinator — fewer connections, more coordinator load.
    /// </summary>
    public bool UseDirectChannels { get; set; } = true;

    /// <summary>Idle time after which a pooled direct channel is closed. Default 60 s.</summary>
    public TimeSpan IdleChannelTimeout { get; set; } = TimeSpan.FromSeconds(60);

    /// <summary>How long the coordinator waits for a force-assignee to confirm before trying the next candidate. Default 2 s.</summary>
    public TimeSpan AssignmentAckTimeout { get; set; } = TimeSpan.FromSeconds(2);

    /// <summary>
    /// Serializer used by the generic <see cref="ICoLibraRouter.RouteAsync{T}(string, string, T, CancellationToken)"/> /
    /// <see cref="ICoLibraRouter.RegisterHandler{T}"/> overloads. Defaults to System.Text.Json
    /// (<see cref="JsonPayloadSerializer"/>); swap in MessagePack, MemoryPack, etc. by
    /// implementing <see cref="IRoutedPayloadSerializer"/>. Must match across all nodes.
    /// </summary>
    public IRoutedPayloadSerializer PayloadSerializer { get; set; } = new JsonPayloadSerializer();
}
