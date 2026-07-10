namespace CoLibra;

/// <summary>
/// Settings for direct node-to-node messaging: arbitrary payloads addressed to a specific node
/// (by <see cref="NodeId"/>) or to every node bearing an application-defined name
/// (<see cref="CoLibraOptions.NodeName"/>). Built for cross-communication between instances —
/// control signals, presence, chat-style traffic — as opposed to the lease-owned work payloads
/// of routed delivery.
/// </summary>
public sealed class MessagingOptions
{
    /// <summary>Enables the feature. Default false — disabled it costs nothing.</summary>
    public bool Enabled { get; set; }

    /// <summary>Maximum message payload size. Default 1 MiB; must leave headroom under the 4 MiB frame limit.</summary>
    public int MaxPayloadBytes { get; set; } = 1_048_576;

    /// <summary>End-to-end budget for one send, including the delivery acknowledgment. Default 5 s.</summary>
    public TimeSpan DeliveryTimeout { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Open direct member↔member channels for messages (default; pooled and shared with routed
    /// delivery). When false, every message relays through the coordinator.
    /// </summary>
    public bool UseDirectChannels { get; set; } = true;

    /// <summary>
    /// Serializer used by the generic <see cref="ICoLibraMessenger.SendAsync{T}"/> /
    /// <see cref="ICoLibraMessenger.RegisterHandler{T}"/> overloads. Defaults to System.Text.Json
    /// (<see cref="JsonPayloadSerializer"/>). Must match across all nodes.
    /// </summary>
    public IRoutedPayloadSerializer PayloadSerializer { get; set; } = new JsonPayloadSerializer();
}
