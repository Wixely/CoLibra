namespace CoLibra;

/// <summary>
/// Settings for routed delivery: the advanced mode for topologies where only one node receives
/// a given piece of data (load balancers, partitioned queues). <see cref="ICoLibraRouter.RouteAsync"/>
/// delivers a payload to the key's lease owner, and when the key is unowned the coordinator
/// force-assigns the lease to the least-loaded node that registered a handler for the type.
/// </summary>
public sealed class RoutingOptions
{
    /// <summary>Enables the feature. Default false — disabled it costs nothing.</summary>
    public bool Enabled { get; set; }

    /// <summary>Maximum routed payload size. Default 1 MiB; must leave headroom under the 4 MiB frame limit.</summary>
    public int MaxPayloadBytes { get; set; } = 1_048_576;

    /// <summary>End-to-end budget for one <see cref="ICoLibraRouter.RouteAsync"/> call, including one re-resolve retry. Default 5 s.</summary>
    public TimeSpan DeliveryTimeout { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>Backstop TTL on the resolver's key→owner cache (push-invalidated on ownership changes). Default 30 s.</summary>
    public TimeSpan OwnerCacheTtl { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Open direct member↔member channels for payloads (default). When false, every payload
    /// relays through the coordinator — fewer connections, more coordinator load.
    /// </summary>
    public bool UseDirectChannels { get; set; } = true;

    /// <summary>Idle time after which a pooled direct channel is closed. Default 60 s.</summary>
    public TimeSpan IdleChannelTimeout { get; set; } = TimeSpan.FromSeconds(60);

    /// <summary>How long the coordinator waits for a force-assignee to confirm before trying the next candidate. Default 2 s.</summary>
    public TimeSpan AssignmentAckTimeout { get; set; } = TimeSpan.FromSeconds(2);
}
