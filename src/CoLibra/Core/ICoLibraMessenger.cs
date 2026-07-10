namespace CoLibra;

/// <summary>
/// Direct node-to-node messaging (<see cref="MessagingOptions"/>): send payloads to a specific
/// node by id, or to every node bearing an application-defined name. The member list
/// (<see cref="ICoLibraCluster.Members"/>, including <see cref="ClusterMember.Name"/>) is the
/// address book. Delivery is acknowledged and at-least-once: on <see cref="SendStatus.Timeout"/>
/// the caller may retry and the receiver may see a duplicate.
/// </summary>
public interface ICoLibraMessenger
{
    /// <summary>
    /// Registers the handler for messages sent on <paramref name="channel"/> (an app-defined
    /// label such as "chat" or "control"). One handler per channel per node; dispose the
    /// registration to withdraw. Handler exceptions are logged and do not fail the delivery.
    /// </summary>
    IAsyncDisposable RegisterHandler(string channel, Func<ReceivedMessage, CancellationToken, ValueTask> handler);

    /// <summary>Typed <see cref="RegisterHandler"/>: payloads are deserialized with <see cref="MessagingOptions.PayloadSerializer"/>.</summary>
    IAsyncDisposable RegisterHandler<T>(string channel, Func<ReceivedMessage<T>, CancellationToken, ValueTask> handler);

    /// <summary>Sends raw bytes to one node. Sending to this node's own id delivers in-process.</summary>
    ValueTask<SendResult> SendAsync(NodeId target, string channel, ReadOnlyMemory<byte> payload,
        CancellationToken cancellationToken = default);

    /// <summary>Raw-bytes overload (keeps <c>byte[]</c> arguments off the generic serializer path).</summary>
    ValueTask<SendResult> SendAsync(NodeId target, string channel, byte[] payload,
        CancellationToken cancellationToken = default);

    /// <summary>Typed send: serializes <paramref name="value"/> with <see cref="MessagingOptions.PayloadSerializer"/>.</summary>
    ValueTask<SendResult> SendAsync<T>(NodeId target, string channel, T value,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Sends to every member whose <see cref="ClusterMember.Name"/> equals <paramref name="name"/>
    /// (ordinal). Returns one result per matching node; empty when no member bears the name.
    /// </summary>
    ValueTask<IReadOnlyList<SendResult>> SendByNameAsync(string name, string channel, ReadOnlyMemory<byte> payload,
        CancellationToken cancellationToken = default);

    /// <summary>Typed <see cref="SendByNameAsync"/>.</summary>
    ValueTask<IReadOnlyList<SendResult>> SendByNameAsync<T>(string name, string channel, T value,
        CancellationToken cancellationToken = default);
}

/// <summary>A message delivered to this node's channel handler.</summary>
public sealed class ReceivedMessage
{
    /// <summary>The channel the sender addressed.</summary>
    public required string Channel { get; init; }

    /// <summary>The payload, exactly as sent.</summary>
    public required ReadOnlyMemory<byte> Payload { get; init; }

    /// <summary>The sending node's id (reply address for <see cref="ICoLibraMessenger.SendAsync(NodeId, string, ReadOnlyMemory{byte}, CancellationToken)"/>).</summary>
    public required NodeId Origin { get; init; }

    /// <summary>The sending node's <see cref="CoLibraOptions.NodeName"/>, when it set one.</summary>
    public string? OriginName { get; init; }
}

/// <summary>A typed message delivered to this node's channel handler.</summary>
public sealed class ReceivedMessage<T>
{
    /// <summary>The channel the sender addressed.</summary>
    public required string Channel { get; init; }

    /// <summary>The deserialized payload.</summary>
    public required T Value { get; init; }

    /// <summary>The sending node's id (reply address).</summary>
    public required NodeId Origin { get; init; }

    /// <summary>The sending node's <see cref="CoLibraOptions.NodeName"/>, when it set one.</summary>
    public string? OriginName { get; init; }
}

/// <summary>Outcome of a send to one node.</summary>
public enum SendStatus
{
    /// <summary>The target acknowledged the delivery (its handler was invoked).</summary>
    Delivered = 0,

    /// <summary>The target has no handler registered for the channel.</summary>
    NoHandler = 1,

    /// <summary>The target id is not a current cluster member.</summary>
    UnknownTarget = 2,

    /// <summary>No acknowledgment within <see cref="MessagingOptions.DeliveryTimeout"/>; the message may or may not have arrived.</summary>
    Timeout = 3,

    /// <summary>The payload exceeds <see cref="MessagingOptions.MaxPayloadBytes"/>.</summary>
    PayloadTooLarge = 4,
}

/// <summary>Result of a send: the outcome and the node it targeted.</summary>
public readonly record struct SendResult(SendStatus Status, NodeId Target)
{
    /// <summary>True when the target acknowledged the delivery.</summary>
    public bool Delivered => Status == SendStatus.Delivered;
}
