using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using LiteNetLib;

namespace CoLibra.Messaging.LiteNetLib;

/// <summary>
/// The standard <see cref="IUdpMessagingEngine"/>: a thin binding over LiteNetLib's NetManager.
/// CoLibra core owns keys, framing and link semantics; this engine only moves opaque datagrams
/// with the requested delivery guarantee.
/// </summary>
public sealed class LiteNetLibEngine : IUdpMessagingEngine, INatPunchCapable, INetEventListener
{
    private readonly NetManager _manager;
    private UdpEngineCallbacks? _callbacks;
    private NatPunchCallbacks? _punchCallbacks;
    private readonly ConcurrentDictionary<NetPeer, LiteNetLibPeer> _peers = new();
    private readonly ConcurrentDictionary<IPEndPoint, TaskCompletionSource<LiteNetLibPeer?>> _pendingConnects = new();
    private volatile bool _disposed;

    /// <summary>Creates the engine. One instance per CoLibra node.</summary>
    public LiteNetLibEngine()
    {
        _manager = new NetManager(this)
        {
            UnsyncedEvents = true,     // raise events from the socket thread; core marshals onto its actor
            AutoRecycle = true,
            IPv6Enabled = false,       // CoLibra is IPv4-first
            DisconnectTimeout = 15_000,
        };
    }

    /// <summary>Test hook: simulate packet loss (percentage 0-100) for soak tests.</summary>
    public int SimulatePacketLossChance
    {
        get => _manager.SimulatePacketLoss ? _manager.SimulationPacketLossChance : 0;
        set
        {
            _manager.SimulationPacketLossChance = value;
            _manager.SimulatePacketLoss = value > 0;
        }
    }

    /// <inheritdoc />
    public void EnableNatPunch(NatPunchCallbacks callbacks)
    {
        ArgumentNullException.ThrowIfNull(callbacks);
        _punchCallbacks = callbacks;
        _manager.NatPunchEnabled = true;
        _manager.NatPunchModule.Init(new PunchListener(this));
        _manager.NatPunchModule.UnsyncedEvents = true;
    }

    /// <inheritdoc />
    public void SendIntroduceRequest(IPEndPoint master, string token) =>
        _manager.NatPunchModule.SendNatIntroduceRequest(master, token);

    /// <inheritdoc />
    public void Introduce(IPEndPoint hostInternal, IPEndPoint hostExternal,
        IPEndPoint clientInternal, IPEndPoint clientExternal, string token) =>
        _manager.NatPunchModule.NatIntroduce(hostInternal, hostExternal, clientInternal, clientExternal, token);

    private sealed class PunchListener(LiteNetLibEngine engine) : INatPunchListener
    {
        public void OnNatIntroductionRequest(IPEndPoint localEndPoint, IPEndPoint remoteEndPoint, string token) =>
            engine._punchCallbacks?.IntroductionRequested(localEndPoint, remoteEndPoint, token);

        public void OnNatIntroductionSuccess(IPEndPoint targetEndPoint, NatAddressType type, string token) =>
            engine._punchCallbacks?.IntroductionSucceeded(targetEndPoint, type == NatAddressType.Internal, token);
    }

    /// <inheritdoc />
    public ValueTask<int> StartAsync(UdpEngineCallbacks callbacks, int port, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(callbacks);
        _callbacks = callbacks;
        if (!_manager.Start(port))
            throw new SocketException((int)SocketError.AddressAlreadyInUse);
        return ValueTask.FromResult(_manager.LocalPort);
    }

    /// <inheritdoc />
    public async ValueTask<IUdpPeer?> ConnectAsync(
        IPEndPoint endpoint, string connectionKey, TimeSpan timeout, CancellationToken cancellationToken)
    {
        var tcs = new TaskCompletionSource<LiteNetLibPeer?>(TaskCreationOptions.RunContinuationsAsynchronously);
        _pendingConnects[endpoint] = tcs;
        try
        {
            var peer = _manager.Connect(endpoint, connectionKey);
            if (peer is null)
                return null;
            if (peer.ConnectionState == ConnectionState.Connected)
                return _peers.GetOrAdd(peer, p => new LiteNetLibPeer(p));

            using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            cts.CancelAfter(timeout);
            try
            {
                return await tcs.Task.WaitAsync(cts.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                peer.Disconnect();
                return null;
            }
        }
        finally
        {
            _pendingConnects.TryRemove(new KeyValuePair<IPEndPoint, TaskCompletionSource<LiteNetLibPeer?>>(endpoint, tcs));
        }
    }

    /// <inheritdoc />
    public ValueTask DisposeAsync()
    {
        if (!_disposed)
        {
            _disposed = true;
            _manager.Stop(sendDisconnectMessages: true);
        }

        return ValueTask.CompletedTask;
    }

    // ---- INetEventListener (LiteNetLib socket-thread callbacks) ----

    void INetEventListener.OnConnectionRequest(ConnectionRequest request)
    {
        var callbacks = _callbacks;
        if (callbacks is null || _disposed)
        {
            request.Reject();
            return;
        }

        // The key is CoLibra's HMAC connection proof; core validates and returns the link tag.
        var key = request.Data.GetString();
        var tag = callbacks.AcceptConnection(key);
        if (tag is null)
        {
            request.Reject();
            return;
        }

        var netPeer = request.Accept();
        var peer = _peers.GetOrAdd(netPeer, p => new LiteNetLibPeer(p));
        peer.Tag = tag;
    }

    void INetEventListener.OnPeerConnected(NetPeer netPeer)
    {
        var peer = _peers.GetOrAdd(netPeer, p => new LiteNetLibPeer(p));
        if (_pendingConnects.TryRemove(netPeer.Address is { } addr ? new IPEndPoint(addr, netPeer.Port) : peer.RemoteEndPoint, out var tcs))
            tcs.TrySetResult(peer);
    }

    void INetEventListener.OnPeerDisconnected(NetPeer netPeer, DisconnectInfo disconnectInfo)
    {
        if (_peers.TryRemove(netPeer, out var peer))
            _callbacks?.PeerDisconnected(peer);
    }

    void INetEventListener.OnNetworkReceive(NetPeer netPeer, NetPacketReader reader, byte channelNumber, DeliveryMethod deliveryMethod)
    {
        if (_peers.TryGetValue(netPeer, out var peer))
            _callbacks?.DatagramReceived(peer, reader.GetRemainingBytesSegment().AsMemory());
    }

    void INetEventListener.OnNetworkError(IPEndPoint endPoint, SocketError socketError)
    {
        if (_pendingConnects.TryRemove(endPoint, out var tcs))
            tcs.TrySetResult(null);
    }

    void INetEventListener.OnNetworkReceiveUnconnected(IPEndPoint remoteEndPoint, NetPacketReader reader, UnconnectedMessageType messageType)
    {
    }

    void INetEventListener.OnNetworkLatencyUpdate(NetPeer peer, int latency)
    {
    }

    private sealed class LiteNetLibPeer(NetPeer peer) : IUdpPeer
    {
        public IPEndPoint RemoteEndPoint { get; } = new(peer.Address, peer.Port);

        public object? Tag { get; set; }

        public ValueTask SendAsync(MessageDelivery delivery, ReadOnlyMemory<byte> datagram)
        {
            peer.Send(datagram.Span, delivery switch
            {
                MessageDelivery.Reliable => DeliveryMethod.ReliableUnordered,
                MessageDelivery.Sequenced => DeliveryMethod.Sequenced,
                MessageDelivery.Unreliable => DeliveryMethod.Unreliable,
                _ => DeliveryMethod.ReliableOrdered,
            });
            return ValueTask.CompletedTask;
        }

        public ValueTask DisconnectAsync()
        {
            peer.Disconnect();
            return ValueTask.CompletedTask;
        }
    }
}
