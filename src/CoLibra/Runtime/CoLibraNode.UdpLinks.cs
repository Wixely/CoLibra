using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Net;
using System.Security.Cryptography;
using CoLibra.Protocol;
using CoLibra.Security;
using Microsoft.Extensions.Logging;

namespace CoLibra.Runtime;

internal sealed partial class CoLibraNode
{
    // ---- UDP data plane (links are actor-owned; engine callbacks post onto the actor) ----
    private IUdpMessagingEngine? _udpEngine;
    private volatile int _udpListenPort;
    private volatile int _myWireId;
    private readonly Dictionary<NodeId, UdpLink> _udpLinks = [];
    private readonly ConcurrentDictionary<string, UdpLink> _pendingUdpAccepts = new(StringComparer.Ordinal);

    private static readonly TimeSpan UdpLinkFailureBackoff = TimeSpan.FromSeconds(30);

    private enum UdpLinkStatus
    {
        Handshaking = 0,
        Active = 1,
        Failed = 2,
    }

    private sealed class UdpLink
    {
        public required Guid LinkId { get; init; }
        public required NodeId Peer { get; init; }
        public required long Term { get; init; }
        public required byte[] MyNonce { get; init; }
        public required Dictionary<byte, string> MyChannelsById { get; init; }
        public UdpLinkStatus Status { get; set; }
        public UdpLinkCrypto? Crypto { get; set; }
        public IUdpPeer? EnginePeer { get; set; }
        public Dictionary<string, byte> PeerChannels { get; set; } = [];
        public int PeerWireId { get; set; }
        public long FailedUntilTs { get; set; }
        public string? ExpectedConnectionKey { get; set; }
        public List<TaskCompletionSource<bool>> ActivationWaiters { get; } = [];
        public Dictionary<long, TaskCompletionSource<DirectAckStatus>> PendingAcks { get; } = [];

        public void FailPending()
        {
            foreach (var tcs in ActivationWaiters)
                tcs.TrySetResult(false);
            ActivationWaiters.Clear();
            foreach (var tcs in PendingAcks.Values)
                tcs.TrySetResult(DirectAckStatus.Unreachable);
            PendingAcks.Clear();
        }
    }

    // =====================================================================================
    // Engine lifecycle
    // =====================================================================================

    private async Task StartUdpEngineAsync(CancellationToken cancellationToken)
    {
        if (_udpEngine is null || !_options.Messaging.Enabled || !_options.Messaging.PreferUdp)
            return;

        var callbacks = new UdpEngineCallbacks
        {
            AcceptConnection = key => _pendingUdpAccepts.TryRemove(key, out var link) ? link : null,
            DatagramReceived = (peer, datagram) =>
            {
                var copy = datagram.ToArray(); // engine may reuse its buffer after the callback
                Post(() =>
                {
                    HandleUdpDatagram(peer, copy);
                    return ValueTask.CompletedTask;
                });
            },
            PeerDisconnected = peer => Post(() =>
            {
                if (peer.Tag is UdpLink link && ReferenceEquals(link.EnginePeer, peer))
                    CloseUdpLink(link, "engine peer disconnected");
                return ValueTask.CompletedTask;
            }),
        };

        _udpListenPort = await _udpEngine.StartAsync(callbacks, _options.Messaging.UdpPort, cancellationToken)
            .ConfigureAwait(false);
        _logger.LogInformation("UDP messaging data plane listening on port {Port}", _udpListenPort);
    }

    private void CloseUdpLink(UdpLink link, string reason)
    {
        if (_udpLinks.TryGetValue(link.Peer, out var current) && ReferenceEquals(current, link))
            _udpLinks.Remove(link.Peer);
        link.Status = UdpLinkStatus.Failed;
        link.FailPending();
        link.Crypto?.Dispose();
        if (link.EnginePeer is { } peer)
            _ = peer.DisconnectAsync().AsTask();
        _logger.LogDebug("UDP link to {Peer} closed: {Reason}", link.Peer, reason);
    }

    private void CloseAllUdpLinks(string reason)
    {
        foreach (var link in _udpLinks.Values.ToList())
            CloseUdpLink(link, reason);
        _udpLinks.Clear();
        _pendingUdpAccepts.Clear();
    }

    /// <summary>Test hook: whether an active UDP link to the peer exists.</summary>
    internal Task<bool> HasActiveUdpLinkAsync(NodeId peer) =>
        PostWithResult<bool>(tcs =>
        {
            tcs.TrySetResult(_udpLinks.TryGetValue(peer, out var link) && link.Status == UdpLinkStatus.Active);
            return ValueTask.CompletedTask;
        });

    /// <summary>Closes links to nodes that have left the membership (actor tick).</summary>
    private void TickUdpLinks()
    {
        if (_udpLinks.Count == 0)
            return;

        foreach (var link in _udpLinks.Values.Where(l => l.Status == UdpLinkStatus.Active).ToList())
        {
            if (!_members.Any(m => m.NodeId == link.Peer))
                CloseUdpLink(link, "peer left the cluster");
        }
    }

    // =====================================================================================
    // Send path (called from SendDirectMessageAsync; null result = fall back to TCP)
    // =====================================================================================

    private bool UdpEligible(ClusterMember target, ReadOnlyMemory<byte> payload) =>
        _udpEngine is not null &&
        _options.Messaging.PreferUdp &&
        _udpListenPort > 0 &&
        target.UdpPort > 0 &&
        payload.Length <= _options.Messaging.MaxUdpPayloadBytes;

    private async Task<DirectAckStatus?> TrySendUdpAsync(
        ClusterMember target, string channel, ReadOnlyMemory<byte> payload, MessageDelivery delivery,
        bool wantAck, CancellationToken ct)
    {
        var link = await GetOrEstablishLinkAsync(target, ct).ConfigureAwait(false);
        if (link is null)
            return null;

        var tcs = wantAck ? new TaskCompletionSource<DirectAckStatus>(TaskCreationOptions.RunContinuationsAsynchronously) : null;
        var sent = await PostWithResult<bool>(inner =>
        {
            if (link.Status != UdpLinkStatus.Active || link.Crypto is null || link.EnginePeer is null ||
                !link.PeerChannels.TryGetValue(channel, out var channelId))
            {
                inner.TrySetResult(false); // channel unknown to the peer (or link died) — TCP carries it
                return ValueTask.CompletedTask;
            }

            var flags = wantAck ? UdpLinkCrypto.FlagWantAck : (byte)0;
            var datagram = link.Crypto.Seal(flags, (ushort)link.Term, (ushort)_myWireId, channelId, payload.Span, out var counter);
            if (tcs is not null)
                link.PendingAcks[counter] = tcs;
            _ = SendUdpSafeAsync(link, delivery, datagram);
            inner.TrySetResult(true);
            return ValueTask.CompletedTask;
        }).WaitAsync(ct).ConfigureAwait(false);

        if (!sent)
            return null;
        if (tcs is null)
            return DirectAckStatus.Delivered; // fire-and-forget: mapped to Sent upstream

        try
        {
            return await tcs.Task.WaitAsync(ct).ConfigureAwait(false);
        }
        finally
        {
            Post(() =>
            {
                foreach (var (counter, pending) in link.PendingAcks.Where(kv => ReferenceEquals(kv.Value, tcs)).ToList())
                    link.PendingAcks.Remove(counter);
                return ValueTask.CompletedTask;
            });
        }
    }

    private async Task SendUdpSafeAsync(UdpLink link, MessageDelivery delivery, byte[] datagram)
    {
        try
        {
            await link.EnginePeer!.SendAsync(delivery, datagram).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogDebug(ex, "UDP send to {Peer} failed", link.Peer);
        }
    }

    private async Task<UdpLink?> GetOrEstablishLinkAsync(ClusterMember target, CancellationToken ct)
    {
        var waiter = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var link = await PostWithResult<UdpLink?>(tcs =>
        {
            if (_udpLinks.TryGetValue(target.NodeId, out var existing))
            {
                switch (existing.Status)
                {
                    case UdpLinkStatus.Active:
                        waiter.TrySetResult(true);
                        tcs.TrySetResult(existing);
                        return ValueTask.CompletedTask;
                    case UdpLinkStatus.Handshaking:
                        existing.ActivationWaiters.Add(waiter);
                        tcs.TrySetResult(existing);
                        return ValueTask.CompletedTask;
                    case UdpLinkStatus.Failed when Now() < existing.FailedUntilTs:
                        tcs.TrySetResult(null); // recent failure: stay on TCP, retry later
                        return ValueTask.CompletedTask;
                    default:
                        _udpLinks.Remove(target.NodeId);
                        break;
                }
            }

            var fresh = InitiateUdpLink(target);
            if (fresh is null)
            {
                tcs.TrySetResult(null);
                return ValueTask.CompletedTask;
            }

            fresh.ActivationWaiters.Add(waiter);
            tcs.TrySetResult(fresh);
            return ValueTask.CompletedTask;
        }).WaitAsync(ct).ConfigureAwait(false);

        if (link is null)
            return null;
        if (link.Status == UdpLinkStatus.Active)
            return link;

        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct, _stopping.Token);
        timeout.CancelAfter(_options.Messaging.LinkHandshakeTimeout);
        try
        {
            return await waiter.Task.WaitAsync(timeout.Token).ConfigureAwait(false) ? link : null;
        }
        catch (OperationCanceledException)
        {
            Post(() =>
            {
                if (link.Status == UdpLinkStatus.Handshaking)
                {
                    link.FailedUntilTs = Now() + ToTicks(UdpLinkFailureBackoff);
                    CloseUdpLink(link, "handshake timed out");
                }

                return ValueTask.CompletedTask;
            });
            return null;
        }
    }

    /// <summary>Actor-only: creates the link record and sends the offer over the TCP mesh.</summary>
    private UdpLink? InitiateUdpLink(ClusterMember target)
    {
        var link = new UdpLink
        {
            LinkId = Guid.NewGuid(),
            Peer = target.NodeId,
            Term = _highestTerm,
            MyNonce = RandomNumberGenerator.GetBytes(32),
            MyChannelsById = BuildMyChannelTable(out var byName),
            Status = UdpLinkStatus.Handshaking,
        };
        _udpLinks[target.NodeId] = link;

        var offer = new UdpLinkOfferMessage(
            link.LinkId, LocalNodeId.Value, RelayToNodeId: null, link.Term, link.MyNonce, _udpListenPort, byName);
        if (!RouteControlMessage(target.NodeId, relayTo => offer with { RelayToNodeId = relayTo }))
        {
            _udpLinks.Remove(target.NodeId);
            return null;
        }

        return link;
    }

    /// <summary>My registered handler channels, numbered from 1 (0 is the control channel).</summary>
    private Dictionary<byte, string> BuildMyChannelTable(out Dictionary<string, byte> byName)
    {
        byName = new Dictionary<string, byte>(StringComparer.Ordinal);
        var byId = new Dictionary<byte, string>();
        byte next = 1;
        foreach (var channel in _messageHandlers.Keys.OrderBy(c => c, StringComparer.Ordinal))
        {
            if (next == byte.MaxValue)
                break; // >254 channels: the rest ride TCP
            byName[channel] = next;
            byId[next] = channel;
            next++;
        }

        return byId;
    }

    /// <summary>Routes a control message to a node over the TCP mesh (direct, session, or relay). Actor-only.</summary>
    private bool RouteControlMessage(NodeId target, Func<Guid?, Message> build)
    {
        if (_coordinator is { } coordinator)
        {
            if (!coordinator.Sessions.TryGetValue(target, out var session))
                return false;
            _ = SendSafeAsync(session.Connection, build(target.Value));
            return true;
        }

        if (_directChannels.TryGetValue(target, out var pooled))
        {
            pooled.LastUsedTs = Now();
            _ = SendSafeAsync(pooled.Channel, build(null));
            return true;
        }

        if (_member is { Connection: { } conn })
        {
            _ = SendSafeAsync(conn, build(target.Value));
            return true;
        }

        return false;
    }

    // =====================================================================================
    // Handshake handling (actor)
    // =====================================================================================

    private void HandleUdpLinkOffer(PeerConn peer, UdpLinkOfferMessage offer)
    {
        if (offer.RelayToNodeId is { } relayTarget && relayTarget != LocalNodeId.Value)
        {
            if (_coordinator is { } coordinator && coordinator.Sessions.TryGetValue(new NodeId(relayTarget), out var session))
                _ = SendSafeAsync(session.Connection, offer);
            return;
        }

        var origin = new NodeId(offer.OriginNodeId);
        var declined = _udpEngine is null || !_options.Messaging.PreferUdp || _udpListenPort <= 0;
        var myTable = BuildMyChannelTable(out var byName);
        var accept = new UdpLinkAcceptMessage(
            offer.LinkId, LocalNodeId.Value, RelayToNodeId: null,
            Accepted: !declined, RandomNumberGenerator.GetBytes(32), _udpListenPort, byName);

        if (!declined)
        {
            if (_udpLinks.TryGetValue(origin, out var existing))
                CloseUdpLink(existing, "superseded by a new inbound offer");

            var link = new UdpLink
            {
                LinkId = offer.LinkId,
                Peer = origin,
                Term = offer.Term,
                MyNonce = accept.Nonce,
                MyChannelsById = myTable,
                Status = UdpLinkStatus.Handshaking,
                PeerChannels = offer.Channels,
                PeerWireId = _members.FirstOrDefault(m => m.NodeId == origin)?.WireId ?? 0,
                Crypto = UdpLinkCrypto.Derive(_keys, offer.LinkId, offer.Nonce, accept.Nonce, offer.Term, isOfferer: false),
            };
            link.ExpectedConnectionKey = UdpLinkCrypto.ConnectionProof(_keys, offer.LinkId, offer.Nonce, accept.Nonce);
            _udpLinks[origin] = link;
            _pendingUdpAccepts[link.ExpectedConnectionKey] = link;
        }

        RouteControlMessage(origin, relayTo => accept with { RelayToNodeId = relayTo });
    }

    private void HandleUdpLinkAccept(PeerConn peer, UdpLinkAcceptMessage accept)
    {
        if (accept.RelayToNodeId is { } relayTarget && relayTarget != LocalNodeId.Value)
        {
            if (_coordinator is { } coordinator && coordinator.Sessions.TryGetValue(new NodeId(relayTarget), out var session))
                _ = SendSafeAsync(session.Connection, accept);
            return;
        }

        var origin = new NodeId(accept.OriginNodeId);
        if (!_udpLinks.TryGetValue(origin, out var link) || link.LinkId != accept.LinkId ||
            link.Status != UdpLinkStatus.Handshaking)
        {
            return; // stale or unknown accept
        }

        var member = _members.FirstOrDefault(m => m.NodeId == origin);
        if (!accept.Accepted || accept.UdpPort <= 0 || member is null)
        {
            link.FailedUntilTs = Now() + ToTicks(UdpLinkFailureBackoff);
            CloseUdpLink(link, "peer declined UDP");
            return;
        }

        link.PeerChannels = accept.Channels;
        link.PeerWireId = member.WireId;
        link.Crypto = UdpLinkCrypto.Derive(_keys, link.LinkId, link.MyNonce, accept.Nonce, link.Term, isOfferer: true);
        var connectionKey = UdpLinkCrypto.ConnectionProof(_keys, link.LinkId, link.MyNonce, accept.Nonce);
        // 127.x.x.x aliases reply from 127.0.0.1, and the engine matches packets by exact
        // remote endpoint — normalize so loopback (same-machine) clusters connect cleanly.
        var address = IPAddress.IsLoopback(member.Endpoint.Address) ? IPAddress.Loopback : member.Endpoint.Address;
        var endpoint = new IPEndPoint(address, accept.UdpPort);

        _ = Task.Run(async () =>
        {
            IUdpPeer? enginePeer = null;
            try
            {
                enginePeer = await _udpEngine!.ConnectAsync(
                    endpoint, connectionKey, _options.Messaging.LinkHandshakeTimeout, _stopping.Token).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                _logger.LogDebug(ex, "UDP connect to {Peer} at {Endpoint} failed", origin, endpoint);
            }

            Post(() =>
            {
                if (link.Status != UdpLinkStatus.Handshaking)
                {
                    _ = enginePeer?.DisconnectAsync().AsTask();
                    return ValueTask.CompletedTask;
                }

                if (enginePeer is null)
                {
                    link.FailedUntilTs = Now() + ToTicks(UdpLinkFailureBackoff);
                    CloseUdpLink(link, "engine connect failed");
                    return ValueTask.CompletedTask;
                }

                enginePeer.Tag = link;
                link.EnginePeer = enginePeer;
                link.Status = UdpLinkStatus.Active;
                foreach (var waiter in link.ActivationWaiters)
                    waiter.TrySetResult(true);
                link.ActivationWaiters.Clear();
                _logger.LogDebug("UDP link to {Peer} active ({Endpoint})", origin, endpoint);
                return ValueTask.CompletedTask;
            });
        }, CancellationToken.None);
    }

    // =====================================================================================
    // Datagram handling (actor)
    // =====================================================================================

    private void HandleUdpDatagram(IUdpPeer peer, byte[] datagram)
    {
        if (peer.Tag is not UdpLink link || link.Crypto is null)
            return;

        // The acceptor side learns its engine peer from the first datagram that arrives.
        if (link.EnginePeer is null && link.Status == UdpLinkStatus.Handshaking)
        {
            peer.Tag = link;
            link.EnginePeer = peer;
            link.Status = UdpLinkStatus.Active;
            foreach (var waiter in link.ActivationWaiters)
                waiter.TrySetResult(true);
            link.ActivationWaiters.Clear();
        }

        if (!link.Crypto.TryOpen(datagram, out var flags, out var termLow, out var srcWireId, out var channelId,
                out var counter, out var payload))
        {
            return; // tampered / replayed / mis-keyed
        }

        if (termLow != (ushort)link.Term || (link.PeerWireId != 0 && srcWireId != link.PeerWireId))
            return; // stale term or impersonated wire id

        if ((flags & UdpLinkCrypto.FlagAck) != 0 && channelId == UdpLinkCrypto.ControlChannel)
        {
            if (payload.Length >= 9)
            {
                var ackedCounter = BinaryPrimitives.ReadInt64LittleEndian(payload);
                var status = (DirectAckStatus)payload[8];
                if (link.PendingAcks.Remove(ackedCounter, out var tcs))
                    tcs.TrySetResult(status);
            }

            return;
        }

        var originName = _members.FirstOrDefault(m => m.NodeId == link.Peer)?.Name;
        var status2 = link.MyChannelsById.TryGetValue(channelId, out var channelName)
            ? DeliverMessageLocal(channelName, payload, link.Peer, originName)
            : DirectAckStatus.NoHandler;

        if ((flags & UdpLinkCrypto.FlagWantAck) != 0)
        {
            Span<byte> ackPayload = stackalloc byte[9];
            BinaryPrimitives.WriteInt64LittleEndian(ackPayload, counter);
            ackPayload[8] = (byte)status2;
            var ack = link.Crypto.Seal(UdpLinkCrypto.FlagAck, (ushort)link.Term, (ushort)_myWireId,
                UdpLinkCrypto.ControlChannel, ackPayload, out _);
            _ = SendUdpSafeAsync(link, MessageDelivery.Reliable, ack);
        }
    }
}
