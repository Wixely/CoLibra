using System.Net;

namespace CoLibra;

/// <summary>
/// Pluggable resilient-UDP socket engine for the messaging data plane. CoLibra core owns the
/// protocol (link handshake over the TLS mesh, per-link AEAD, compact headers, fallback); the
/// engine only moves opaque datagrams with the requested delivery guarantee. The
/// CoLibra.Messaging.LiteNetLib package provides the standard implementation.
/// </summary>
public interface IUdpMessagingEngine : IAsyncDisposable
{
    /// <summary>Binds the listener; returns the actual bound port (advertised via membership).</summary>
    ValueTask<int> StartAsync(UdpEngineCallbacks callbacks, int port, CancellationToken cancellationToken);

    /// <summary>
    /// Connects to a peer. <paramref name="connectionKey"/> authenticates the attempt to the
    /// acceptor before any datagram flows. Null on timeout/refusal.
    /// </summary>
    ValueTask<IUdpPeer?> ConnectAsync(IPEndPoint endpoint, string connectionKey, TimeSpan timeout,
        CancellationToken cancellationToken);
}

/// <summary>An engine-level connection to one remote peer.</summary>
public interface IUdpPeer
{
    /// <summary>The peer's UDP endpoint.</summary>
    IPEndPoint RemoteEndPoint { get; }

    /// <summary>Opaque association slot for the owner (CoLibra stores its link state here).</summary>
    object? Tag { get; set; }

    /// <summary>Sends one datagram with the requested delivery guarantee.</summary>
    ValueTask SendAsync(MessageDelivery delivery, ReadOnlyMemory<byte> datagram);

    /// <summary>Closes the connection.</summary>
    ValueTask DisconnectAsync();
}

/// <summary>Callbacks the engine raises; may be invoked from engine threads.</summary>
public sealed class UdpEngineCallbacks
{
    /// <summary>Validates an inbound connection key; returns the tag to attach to the peer, or null to reject.</summary>
    public required Func<string, object?> AcceptConnection { get; init; }

    /// <summary>A datagram arrived from a connected peer.</summary>
    public required Action<IUdpPeer, ReadOnlyMemory<byte>> DatagramReceived { get; init; }

    /// <summary>The peer disconnected or timed out at the engine level.</summary>
    public required Action<IUdpPeer> PeerDisconnected { get; init; }
}

/// <summary>
/// Optional engine capability: NAT hole punching. CoLibra uses the coordinator (which holds
/// authenticated TCP to both peers and a UDP socket) as the rendezvous "master": both peers
/// send an introduce request to it; it observes their public endpoints and introduces them;
/// punch packets open the NAT mappings; the normal authenticated connect then succeeds.
/// </summary>
public interface INatPunchCapable
{
    /// <summary>Enables punching and installs the callbacks. Call before <see cref="IUdpMessagingEngine.StartAsync"/>.</summary>
    void EnableNatPunch(NatPunchCallbacks callbacks);

    /// <summary>Peer side: asks the master to introduce us to whoever presents the same token.</summary>
    void SendIntroduceRequest(IPEndPoint master, string token);

    /// <summary>Master side: sends punch packets to both parties (their internal and observed-external endpoints).</summary>
    void Introduce(IPEndPoint hostInternal, IPEndPoint hostExternal,
        IPEndPoint clientInternal, IPEndPoint clientExternal, string token);
}

/// <summary>NAT punch callbacks; may be invoked from engine threads.</summary>
public sealed class NatPunchCallbacks
{
    /// <summary>Master side: a peer asked to be introduced (its internal endpoint, its observed external endpoint, the token).</summary>
    public required Action<IPEndPoint, IPEndPoint, string> IntroductionRequested { get; init; }

    /// <summary>Peer side: punching produced a candidate endpoint for the other party (true = internal-network candidate).</summary>
    public required Action<IPEndPoint, bool, string> IntroductionSucceeded { get; init; }
}
