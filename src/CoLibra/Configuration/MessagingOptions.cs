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

    /// <summary>
    /// Prefer the resilient-UDP data plane for node-to-node messages when a UDP engine is
    /// registered (e.g. the CoLibra.Messaging.LiteNetLib package) and the peer advertises a UDP
    /// port. Falls back to the TCP path automatically per peer/message when UDP is unavailable.
    /// Default false. The TCP control plane is unaffected either way.
    /// </summary>
    public bool PreferUdp { get; set; }

    /// <summary>UDP data-plane listen port. 0 (default) binds an OS-assigned port, advertised via membership.</summary>
    public int UdpPort { get; set; }

    /// <summary>Budget for establishing a UDP link (key exchange over TCP + connect). Default 2 s.</summary>
    public TimeSpan LinkHandshakeTimeout { get; set; } = TimeSpan.FromSeconds(2);

    /// <summary>
    /// Messages larger than this silently use the TCP path even when UDP is preferred
    /// (game-state payloads should be small; large blobs belong on TCP). Default 8 KiB.
    /// </summary>
    public int MaxUdpPayloadBytes { get; set; } = 8 * 1024;

    /// <summary>
    /// When a direct UDP connect fails (peers behind NATs), attempt coordinator-mediated hole
    /// punching before falling back to TCP. Requires the coordinator to run the UDP engine
    /// (it is the rendezvous). Works through cone-type NATs; symmetric NATs still defeat
    /// punching, in which case the TCP fallback carries the traffic. Default true.
    /// </summary>
    public bool EnableNatPunch { get; set; } = true;
}
