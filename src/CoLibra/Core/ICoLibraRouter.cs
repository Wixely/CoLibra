namespace CoLibra;

/// <summary>
/// Routed delivery (<see cref="RoutingOptions"/>): lets any node hand data to the cluster and
/// have it arrive at the lease owner's handler, force-assigning an owner when the key is free.
/// Complements the primary broadcast-feed pattern, where every node sees every message and
/// <see cref="ICoLibraCluster.CanProcessAsync"/> filters.
/// </summary>
public interface ICoLibraRouter
{
    /// <summary>
    /// Registers the handler that receives routed payloads for <paramref name="type"/> on this
    /// node, and advertises this node as a force-assignment candidate for the type. One handler
    /// per type per node; dispose the registration to withdraw. Handler exceptions are logged
    /// and do not fail the delivery (the sender sees Delivered — processing errors are the
    /// application's to handle).
    /// </summary>
    IAsyncDisposable RegisterHandler(string type, Func<RoutedDelivery, CancellationToken, ValueTask> handler);

    /// <summary>
    /// Delivers the payload to the node owning (type, id) — locally when this node owns it.
    /// If the key is unowned, the coordinator first force-assigns it to the least-loaded node
    /// with a registered handler (the first route for a key pays this extra latency).
    /// Semantics are at-least-once: on <see cref="RouteStatus.Timeout"/> the caller may retry
    /// and the handler may see a duplicate; use <see cref="RoutedDelivery.Token"/> to fence.
    /// </summary>
    ValueTask<RouteResult> RouteAsync(string type, string id, ReadOnlyMemory<byte> payload,
        CancellationToken cancellationToken = default);
}

/// <summary>A payload delivered to this node because it owns (or was just assigned) the key.</summary>
public sealed class RoutedDelivery
{
    /// <summary>The key the payload was routed by.</summary>
    public required LeaseKey Key { get; init; }

    /// <summary>The application payload, exactly as passed to <see cref="ICoLibraRouter.RouteAsync"/>.</summary>
    public required ReadOnlyMemory<byte> Payload { get; init; }

    /// <summary>The node that received the data from the outside world and routed it here.</summary>
    public required NodeId Origin { get; init; }

    /// <summary>This node's current fencing token for the key, for guarding external writes.</summary>
    public required FencingToken Token { get; init; }
}

/// <summary>Outcome of a <see cref="ICoLibraRouter.RouteAsync"/> call.</summary>
public enum RouteStatus
{
    /// <summary>This node owns the key; the handler ran in-process.</summary>
    DeliveredLocal = 0,

    /// <summary>The owner acknowledged receipt.</summary>
    Delivered = 1,

    /// <summary>No node in the cluster has a handler registered for the type.</summary>
    NoHandler = 2,

    /// <summary>No acknowledgment within <see cref="RoutingOptions.DeliveryTimeout"/>; the payload may or may not have arrived.</summary>
    Timeout = 3,

    /// <summary>Resolution was refused under the current <see cref="SplitBrainPolicy"/> / quorum state.</summary>
    QuorumUnavailable = 4,

    /// <summary>The payload exceeds <see cref="RoutingOptions.MaxPayloadBytes"/>.</summary>
    PayloadTooLarge = 5,

    /// <summary>The key was marked completed (<see cref="ICoLibraCluster.MarkCompletedAsync"/>); there is nothing to route to.</summary>
    KeyCompleted = 6,
}

/// <summary>Result of a route: the status and, when known, the owner it was delivered to.</summary>
public readonly record struct RouteResult(RouteStatus Status, NodeId? Owner)
{
    /// <summary>True for <see cref="RouteStatus.Delivered"/> and <see cref="RouteStatus.DeliveredLocal"/>.</summary>
    public bool Delivered => Status is RouteStatus.Delivered or RouteStatus.DeliveredLocal;
}
