namespace CoLibra;

/// <summary>
/// Delivery guarantee for a node-to-node message. On the default TCP transport every mode is
/// carried reliably-in-order under the hood, but the result contract is identical on every
/// transport: reliable modes complete with an acknowledged <see cref="SendResult"/>, while
/// <see cref="Sequenced"/> and <see cref="Unreliable"/> return <see cref="SendStatus.Sent"/>
/// immediately (fire-and-forget). On the UDP transport the modes map to real wire behavior.
/// </summary>
public enum MessageDelivery
{
    /// <summary>Retransmitted until delivered, in send order. The default; TCP-equivalent semantics.</summary>
    ReliableOrdered = 0,

    /// <summary>Retransmitted until delivered; may arrive out of order.</summary>
    Reliable = 1,

    /// <summary>Latest-wins: no retransmission, packets arriving late (behind a newer one) are dropped.</summary>
    Sequenced = 2,

    /// <summary>Fire-and-forget: no retransmission, no ordering.</summary>
    Unreliable = 3,
}
