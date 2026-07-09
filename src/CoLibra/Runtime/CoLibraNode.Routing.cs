using System.Collections.Concurrent;
using System.Net;
using CoLibra.Protocol;
using CoLibra.Security;
using CoLibra.Transport;
using Microsoft.Extensions.Logging;

namespace CoLibra.Runtime;

internal sealed partial class CoLibraNode : ICoLibraRouter
{
    // ---- handler table (written from any thread via RegisterHandler; read everywhere) ----
    private readonly ConcurrentDictionary<string, Func<RoutedDelivery, CancellationToken, ValueTask>> _routedHandlers =
        new(StringComparer.Ordinal);
    private volatile IReadOnlyList<string> _routedTypesSnapshot = [];

    // ---- actor-owned routing state ----
    private readonly Dictionary<Guid, (LeaseKey Key, TaskCompletionSource<OwnerResolveReplyMessage> Tcs)> _pendingResolves = [];
    private readonly Dictionary<Guid, TaskCompletionSource<RouteAckStatus>> _pendingRouteAcks = [];
    private readonly Dictionary<LeaseKey, CachedOwner> _ownerCache = [];
    private readonly Dictionary<NodeId, DirectChannel> _directChannels = [];

    private sealed record CachedOwner(NodeId Owner, IPEndPoint? Endpoint, long ExpiresTs);

    private sealed class DirectChannel
    {
        public required IMessageChannel Channel { get; init; }
        public long LastUsedTs { get; set; }
    }

    private sealed class PendingAssignment
    {
        public required LeaseKey Key { get; init; }
        public required NodeId Assignee { get; set; }
        public required FencingToken Token { get; set; }
        public required long DeadlineTs { get; set; }
        public List<(NodeId Requester, Guid RequestId)> Waiters { get; } = [];
        public HashSet<NodeId> Tried { get; } = [];
    }

    // =====================================================================================
    // Public router API
    // =====================================================================================

    public ICoLibraRouter Router => _options.Routing.Enabled
        ? this
        : throw new InvalidOperationException(
            "Routed delivery is disabled; set CoLibraOptions.Routing.Enabled = true to use the Router.");

    IAsyncDisposable ICoLibraRouter.RegisterHandler(
        string type, Func<RoutedDelivery, CancellationToken, ValueTask> handler)
    {
        ArgumentException.ThrowIfNullOrEmpty(type);
        ArgumentNullException.ThrowIfNull(handler);
        if (!_routedHandlers.TryAdd(type, handler))
            throw new InvalidOperationException($"A routed handler for type '{type}' is already registered on this node.");

        _routedTypesSnapshot = [.. _routedHandlers.Keys];
        NudgeAdvertisement();
        return new HandlerRegistration(this, type);
    }

    /// <summary>Advertises handler changes on the next tick instead of waiting a full heartbeat.</summary>
    private void NudgeAdvertisement() =>
        Post(() =>
        {
            if (_member is { } member)
                member.LastHeartbeatSentTs = 0;
            return ValueTask.CompletedTask;
        });

    /// <summary>Test hook: how many nodes (including this one) the coordinator sees advertising the type.</summary>
    internal Task<int> CountRoutedAdvertisersAsync(string type) =>
        PostWithResult<int>(tcs =>
        {
            tcs.TrySetResult(_coordinator is { } coordinator ? AdvertisersOf(coordinator, type).Count : 0);
            return ValueTask.CompletedTask;
        });

    private sealed class HandlerRegistration(CoLibraNode node, string type) : IAsyncDisposable
    {
        public ValueTask DisposeAsync()
        {
            if (node._routedHandlers.TryRemove(type, out _))
            {
                node._routedTypesSnapshot = [.. node._routedHandlers.Keys];
                node.NudgeAdvertisement();
            }

            return ValueTask.CompletedTask;
        }
    }

    async ValueTask<RouteResult> ICoLibraRouter.RouteAsync(
        string type, string id, ReadOnlyMemory<byte> payload, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrEmpty(type);
        ArgumentException.ThrowIfNullOrEmpty(id);
        if (payload.Length > _options.Routing.MaxPayloadBytes)
            return new RouteResult(RouteStatus.PayloadTooLarge, null);

        var key = new LeaseKey(type, id);
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _stopping.Token);
        timeout.CancelAfter(_options.Routing.DeliveryTimeout);

        try
        {
            var freshResolve = false;
            while (true)
            {
                timeout.Token.ThrowIfCancellationRequested();

                // Local fast path: we own it (or just got assigned it).
                if (IsHeldAndFresh(key))
                {
                    var localStatus = await PostWithResult<RouteAckStatus>(tcs =>
                    {
                        tcs.TrySetResult(DeliverLocal(key, payload.ToArray(), LocalNodeId));
                        return ValueTask.CompletedTask;
                    }).WaitAsync(timeout.Token).ConfigureAwait(false);
                    if (localStatus == RouteAckStatus.Delivered)
                        return new RouteResult(RouteStatus.DeliveredLocal, LocalNodeId);
                    if (localStatus == RouteAckStatus.NoHandler)
                        return new RouteResult(RouteStatus.NoHandler, LocalNodeId);
                    // NotOwner: fell out of freshness between the check and delivery — resolve.
                }

                var reply = await ResolveOwnerAsync(key, freshResolve, timeout.Token).ConfigureAwait(false);
                switch (reply.Outcome)
                {
                    case ResolveOutcome.NoHandler:
                        return new RouteResult(RouteStatus.NoHandler, null);
                    case ResolveOutcome.Unavailable:
                        return new RouteResult(RouteStatus.QuorumUnavailable, null);
                    case ResolveOutcome.Completed:
                        return new RouteResult(RouteStatus.KeyCompleted, null);
                    case ResolveOutcome.Retry:
                        await Task.Delay(TimeSpan.FromMilliseconds(100), timeout.Token).ConfigureAwait(false);
                        freshResolve = true;
                        continue;
                }

                var owner = new NodeId(reply.OwnerNodeId!.Value);
                if (owner == LocalNodeId)
                {
                    // Assigned (or already owned) here: install the lease, then loop into the local path.
                    await PostWithResult<bool>(tcs =>
                    {
                        AddHeld(key, new FencingToken(reply.TokenTerm, reply.TokenSequence));
                        tcs.TrySetResult(true);
                        return ValueTask.CompletedTask;
                    }).WaitAsync(timeout.Token).ConfigureAwait(false);
                    continue;
                }

                var status = await SendRoutedAsync(owner, ParseHint(reply.OwnerHost, reply.OwnerPort), key, payload, timeout.Token)
                    .ConfigureAwait(false);
                switch (status)
                {
                    case RouteAckStatus.Delivered:
                        return new RouteResult(RouteStatus.Delivered, owner);
                    case RouteAckStatus.NoHandler:
                        return new RouteResult(RouteStatus.NoHandler, owner);
                    case RouteAckStatus.NotOwner:
                        await InvalidateOwnerCacheAsync(key).ConfigureAwait(false);
                        freshResolve = true;
                        continue;
                }
            }
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            return new RouteResult(RouteStatus.Timeout, null);
        }
    }

    // =====================================================================================
    // Local delivery (actor)
    // =====================================================================================

    private RouteAckStatus DeliverLocal(LeaseKey key, byte[] payload, NodeId origin)
    {
        if (!_heldSnapshot.TryGetValue(key, out var token) || !IsHeldAndFresh(key))
            return RouteAckStatus.NotOwner;
        if (!_routedHandlers.TryGetValue(key.Type, out var handler))
            return RouteAckStatus.NoHandler;

        var delivery = new RoutedDelivery { Key = key, Payload = payload, Origin = origin, Token = token };
        _ = Task.Run(async () =>
        {
            try
            {
                await handler(delivery, _stopping.Token).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                _logger.LogError(ex, "Routed handler for {Key} threw; delivery was already acknowledged", key);
            }
        }, CancellationToken.None);
        return RouteAckStatus.Delivered;
    }

    // =====================================================================================
    // Owner resolution (client side)
    // =====================================================================================

    private Task<OwnerResolveReplyMessage> ResolveOwnerAsync(LeaseKey key, bool skipCache, CancellationToken ct)
    {
        var tcs = new TaskCompletionSource<OwnerResolveReplyMessage>(TaskCreationOptions.RunContinuationsAsynchronously);
        Post(() =>
        {
            if (!skipCache && _ownerCache.TryGetValue(key, out var cached))
            {
                if (cached.ExpiresTs > Now() && _members.Any(m => m.NodeId == cached.Owner))
                {
                    tcs.TrySetResult(new OwnerResolveReplyMessage(
                        Guid.Empty, ResolveOutcome.Resolved, cached.Owner.Value,
                        cached.Endpoint?.Address.ToString(), cached.Endpoint?.Port ?? 0, false));
                    return ValueTask.CompletedTask;
                }

                _ownerCache.Remove(key);
            }

            var requestId = Guid.NewGuid();
            _pendingResolves[requestId] = (key, tcs);
            if (_coordinator is { } coordinator)
            {
                HandleOwnerResolveAsCoordinator(coordinator, LocalNodeId, new OwnerResolveMessage(requestId, key.Type, key.Id));
            }
            else if (_member is { Connection: { } conn })
            {
                _ = SendSafeAsync(conn, new OwnerResolveMessage(requestId, key.Type, key.Id));
            }
            else
            {
                _pendingResolves.Remove(requestId);
                tcs.TrySetResult(new OwnerResolveReplyMessage(requestId, ResolveOutcome.Retry, null, null, 0, false));
            }

            return ValueTask.CompletedTask;
        });
        return tcs.Task.WaitAsync(ct);
    }

    private void CompleteResolve(OwnerResolveReplyMessage reply)
    {
        if (!_pendingResolves.Remove(reply.RequestId, out var pending))
            return;

        if (reply.Outcome == ResolveOutcome.Resolved && reply.OwnerNodeId is { } ownerId && new NodeId(ownerId) != LocalNodeId)
        {
            _ownerCache[pending.Key] = new CachedOwner(
                new NodeId(ownerId), ParseHint(reply.OwnerHost, reply.OwnerPort),
                Now() + ToTicks(_options.Routing.OwnerCacheTtl));
        }

        pending.Tcs.TrySetResult(reply);
    }

    private Task InvalidateOwnerCacheAsync(LeaseKey key) =>
        PostWithResult<bool>(tcs =>
        {
            _ownerCache.Remove(key);
            tcs.TrySetResult(true);
            return ValueTask.CompletedTask;
        });

    private void FailAllPendingResolves()
    {
        foreach (var (id, pending) in _pendingResolves.ToList())
            pending.Tcs.TrySetResult(new OwnerResolveReplyMessage(id, ResolveOutcome.Retry, null, null, 0, false));
        _pendingResolves.Clear();
        _ownerCache.Clear();
    }

    // =====================================================================================
    // Owner resolution + forced assignment (coordinator side, actor)
    // =====================================================================================

    private void HandleOwnerResolveAsCoordinator(CoordinatorRole coordinator, NodeId requester, OwnerResolveMessage request)
    {
        var key = new LeaseKey(request.LeaseType, request.LeaseId);

        if (_state == ClusterState.QuorumLost && _options.SplitBrainPolicy != SplitBrainPolicy.Continue)
        {
            ReplyResolve(coordinator, requester, new OwnerResolveReplyMessage(
                request.RequestId, ResolveOutcome.Unavailable, null, null, 0, false));
            return;
        }

        if (_completions?.Contains(key) == true)
        {
            ReplyResolve(coordinator, requester, new OwnerResolveReplyMessage(
                request.RequestId, ResolveOutcome.Completed, null, null, 0, false));
            return;
        }

        if (coordinator.Table.TryGetOwner(key, out var owner, out var token))
        {
            ReplyResolve(coordinator, requester, BuildResolved(coordinator, request.RequestId, requester, owner, token, wasAssigned: false));
            return;
        }

        if (coordinator.RebuildDeadlineTs > 0 && Now() < coordinator.RebuildDeadlineTs)
        {
            ReplyResolve(coordinator, requester, new OwnerResolveReplyMessage(
                request.RequestId, ResolveOutcome.Retry, null, null, 0, false));
            return;
        }

        if (coordinator.PendingAssignments.TryGetValue(key, out var pending))
        {
            pending.Waiters.Add((requester, request.RequestId));
            return;
        }

        BeginAssignment(coordinator, key, requester, request.RequestId, tried: []);
    }

    private void BeginAssignment(CoordinatorRole coordinator, LeaseKey key, NodeId requester, Guid requestId, HashSet<NodeId> tried)
    {
        var candidates = AdvertisersOf(coordinator, key.Type).Where(c => !tried.Contains(c)).ToList();
        var assignee = coordinator.Table.PickLeastLoaded(key.Type, candidates);
        if (assignee is null)
        {
            var reply = new OwnerResolveReplyMessage(requestId, ResolveOutcome.NoHandler, null, null, 0, false);
            ReplyResolve(coordinator, requester, reply);
            return;
        }

        var token = coordinator.Table.NextToken();
        if (assignee == LocalNodeId || assignee == requester)
        {
            // Self- or requester-assignment needs no two-step ack: the owner learns via this
            // same actor (self) or via the reply carrying the token (requester).
            coordinator.Table.Assign(assignee.Value, key, token, Now());
            if (assignee == LocalNodeId)
                AddHeld(key, token);
            ReplyResolve(coordinator, requester, BuildResolved(coordinator, requestId, requester, assignee.Value, token, wasAssigned: true));
            return;
        }

        tried.Add(assignee.Value);
        var pending = new PendingAssignment
        {
            Key = key,
            Assignee = assignee.Value,
            Token = token,
            DeadlineTs = Now() + ToTicks(_options.Routing.AssignmentAckTimeout),
        };
        pending.Waiters.Add((requester, requestId));
        foreach (var t in tried)
            pending.Tried.Add(t);
        coordinator.PendingAssignments[key] = pending;

        if (coordinator.Sessions.TryGetValue(assignee.Value, out var session))
        {
            _ = SendSafeAsync(session.Connection, new LeaseAssignMessage(
                key.Type, key.Id, token.Term, token.Sequence, _options.LeaseTtl.TotalSeconds));
        }
        // else: the session vanished this instant; the deadline sweep retries the next candidate.
    }

    private void HandleLeaseAssignAck(CoordinatorRole coordinator, NodeId from, LeaseAssignAckMessage ack)
    {
        var key = new LeaseKey(ack.LeaseType, ack.LeaseId);
        if (!coordinator.PendingAssignments.TryGetValue(key, out var pending) ||
            pending.Assignee != from ||
            pending.Token != new FencingToken(ack.Term, ack.Sequence))
        {
            return; // stale ack from a superseded attempt
        }

        coordinator.PendingAssignments.Remove(key);
        if (!ack.Accepted)
        {
            RetryOrFailAssignment(coordinator, pending);
            return;
        }

        // Commit: the assignee installed the lease before acking, so there is no unaware owner.
        coordinator.Table.Assign(pending.Assignee, key, pending.Token, Now());
        foreach (var (requester, requestId) in pending.Waiters)
            ReplyResolve(coordinator, requester, BuildResolved(coordinator, requestId, requester, pending.Assignee, pending.Token, wasAssigned: true));
    }

    private void RetryOrFailAssignment(CoordinatorRole coordinator, PendingAssignment pending)
    {
        var (firstRequester, firstRequestId) = pending.Waiters[0];
        BeginAssignment(coordinator, pending.Key, firstRequester, firstRequestId, pending.Tried);

        // Re-attach any additional waiters to the new pending attempt (or fan out its failure).
        if (pending.Waiters.Count > 1 && coordinator.PendingAssignments.TryGetValue(pending.Key, out var next))
        {
            next.Waiters.AddRange(pending.Waiters.Skip(1));
        }
        else if (pending.Waiters.Count > 1)
        {
            foreach (var (requester, requestId) in pending.Waiters.Skip(1))
            {
                // The retry resolved instantly (self/requester assignment) or failed with NoHandler;
                // resolve the stragglers by asking again from scratch — the key is now decided.
                HandleOwnerResolveAsCoordinator(coordinator, requester,
                    new OwnerResolveMessage(requestId, pending.Key.Type, pending.Key.Id));
            }
        }
    }

    private void TickPendingAssignments(CoordinatorRole coordinator, long now)
    {
        if (coordinator.PendingAssignments.Count == 0)
            return;

        foreach (var pending in coordinator.PendingAssignments.Values.Where(p => p.DeadlineTs <= now).ToList())
        {
            coordinator.PendingAssignments.Remove(pending.Key);
            _logger.LogWarning("Forced assignment of {Key} to {Assignee} timed out; trying the next candidate",
                pending.Key, pending.Assignee);
            RetryOrFailAssignment(coordinator, pending);
        }
    }

    private void HandleAssigneeDeparted(CoordinatorRole coordinator, NodeId departed)
    {
        foreach (var pending in coordinator.PendingAssignments.Values.Where(p => p.Assignee == departed).ToList())
        {
            coordinator.PendingAssignments.Remove(pending.Key);
            RetryOrFailAssignment(coordinator, pending);
        }
    }

    private List<NodeId> AdvertisersOf(CoordinatorRole coordinator, string type)
    {
        var advertisers = coordinator.Sessions.Values
            .Where(s => s.RoutedTypes.Contains(type, StringComparer.Ordinal))
            .Select(s => s.Id)
            .ToList();
        if (_options.Routing.Enabled && _routedHandlers.ContainsKey(type))
            advertisers.Add(LocalNodeId);
        return advertisers;
    }

    private OwnerResolveReplyMessage BuildResolved(
        CoordinatorRole coordinator, Guid requestId, NodeId requester, NodeId owner, FencingToken token, bool wasAssigned)
    {
        string? host = null;
        var port = 0;
        if (owner == LocalNodeId)
        {
            host = _transport.MeshEndpoint.Address.ToString();
            port = _transport.MeshEndpoint.Port;
        }
        else if (coordinator.Sessions.TryGetValue(owner, out var session))
        {
            host = session.Dto.Host;
            port = session.Dto.Port;
        }

        // The requester only needs the token when it is the owner (to install the lease).
        var includeToken = owner == requester;
        return new OwnerResolveReplyMessage(
            requestId, ResolveOutcome.Resolved, owner.Value, host, port, wasAssigned,
            includeToken ? token.Term : 0, includeToken ? token.Sequence : 0);
    }

    private void ReplyResolve(CoordinatorRole coordinator, NodeId requester, OwnerResolveReplyMessage reply)
    {
        if (requester == LocalNodeId)
        {
            CompleteResolve(reply);
            return;
        }

        if (coordinator.Sessions.TryGetValue(requester, out var session))
            _ = SendSafeAsync(session.Connection, reply);
    }

    // =====================================================================================
    // Assignee side (member, actor)
    // =====================================================================================

    private void HandleLeaseAssign(PeerConn peer, LeaseAssignMessage assign)
    {
        var key = new LeaseKey(assign.LeaseType, assign.LeaseId);
        var accepted = _options.Routing.Enabled && _routedHandlers.ContainsKey(assign.LeaseType);
        if (accepted)
            AddHeld(key, new FencingToken(assign.Term, assign.Sequence));

        _ = SendSafeAsync(peer.Channel, new LeaseAssignAckMessage(
            assign.LeaseType, assign.LeaseId, assign.Term, assign.Sequence, accepted));
    }

    // =====================================================================================
    // Payload transport
    // =====================================================================================

    private async Task<RouteAckStatus> SendRoutedAsync(
        NodeId owner, IPEndPoint? ownerEndpoint, LeaseKey key, ReadOnlyMemory<byte> payload, CancellationToken ct)
    {
        var routeId = Guid.NewGuid();
        var tcs = new TaskCompletionSource<RouteAckStatus>(TaskCreationOptions.RunContinuationsAsynchronously);
        try
        {
            IMessageChannel? direct = null;
            if (_options.Routing.UseDirectChannels && !_isCoordinatorRole && ownerEndpoint is not null)
                direct = await GetOrCreateDirectChannelAsync(owner, ownerEndpoint, ct).ConfigureAwait(false);

            await PostWithResult<bool>(inner =>
            {
                _pendingRouteAcks[routeId] = tcs;
                var message = new RoutedPayloadMessage(routeId, key.Type, key.Id, LocalNodeId.Value, RelayToNodeId: null)
                {
                    Payload = payload.ToArray(),
                };

                if (_coordinator is { } coordinator)
                {
                    if (coordinator.Sessions.TryGetValue(owner, out var session))
                        _ = SendSafeAsync(session.Connection, message with { RelayToNodeId = owner.Value });
                    else
                        CompleteRouteAck(new RoutedAckMessage(routeId, RouteAckStatus.NotOwner, null));
                }
                else if (direct is not null)
                {
                    _ = SendSafeAsync(direct, message);
                }
                else if (_member is { Connection: { } conn })
                {
                    _ = SendSafeAsync(conn, message with { RelayToNodeId = owner.Value });
                }
                else
                {
                    CompleteRouteAck(new RoutedAckMessage(routeId, RouteAckStatus.NotOwner, null));
                }

                inner.TrySetResult(true);
                return ValueTask.CompletedTask;
            }).ConfigureAwait(false);

            return await tcs.Task.WaitAsync(ct).ConfigureAwait(false);
        }
        finally
        {
            Post(() =>
            {
                _pendingRouteAcks.Remove(routeId);
                return ValueTask.CompletedTask;
            });
        }
    }

    private void HandleRoutedPayload(PeerConn peer, RoutedPayloadMessage message)
    {
        var key = new LeaseKey(message.LeaseType, message.LeaseId);
        var origin = new NodeId(message.OriginNodeId);

        if (message.RelayToNodeId is { } target && target != LocalNodeId.Value)
        {
            // Relay hop: forward to the target member, preserving RelayTo so it acks through us.
            if (_coordinator is { } coordinator && coordinator.Sessions.TryGetValue(new NodeId(target), out var session))
                _ = SendSafeAsync(session.Connection, message);
            else
                _ = SendSafeAsync(peer.Channel, new RoutedAckMessage(message.RouteId, RouteAckStatus.NotOwner, origin.Value));
            return;
        }

        var status = DeliverLocal(key, message.Payload, origin);
        // If it came relayed, the ack goes back the same way, addressed to the origin.
        var ackRelay = message.RelayToNodeId is not null && origin != LocalNodeId ? origin.Value : (Guid?)null;
        _ = SendSafeAsync(peer.Channel, new RoutedAckMessage(message.RouteId, status, ackRelay));
    }

    private void HandleRoutedAck(PeerConn peer, RoutedAckMessage ack)
    {
        if (ack.RelayToNodeId is { } target && target != LocalNodeId.Value)
        {
            if (_coordinator is { } coordinator && coordinator.Sessions.TryGetValue(new NodeId(target), out var session))
                _ = SendSafeAsync(session.Connection, ack with { RelayToNodeId = null });
            return;
        }

        CompleteRouteAck(ack);
    }

    private void CompleteRouteAck(RoutedAckMessage ack)
    {
        if (_pendingRouteAcks.Remove(ack.RouteId, out var tcs))
            tcs.TrySetResult(ack.Status);
    }

    // =====================================================================================
    // Direct member↔member channels (payloads only)
    // =====================================================================================

    private async Task<IMessageChannel?> GetOrCreateDirectChannelAsync(NodeId peerId, IPEndPoint endpoint, CancellationToken ct)
    {
        var existing = await PostWithResult<IMessageChannel?>(tcs =>
        {
            if (_directChannels.TryGetValue(peerId, out var pooled))
            {
                pooled.LastUsedTs = Now();
                tcs.TrySetResult(pooled.Channel);
            }
            else
            {
                tcs.TrySetResult(null);
            }

            return ValueTask.CompletedTask;
        }).WaitAsync(ct).ConfigureAwait(false);
        if (existing is not null)
            return existing;

        IMessageChannel? channel = null;
        try
        {
            using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct, _stopping.Token);
            timeout.CancelAfter(ConnectTimeout);
            channel = await _transport.ConnectAsync(endpoint, timeout.Token).ConfigureAwait(false);
            var (remoteId, remoteIncarnation) = await Handshake
                .AsClientAsync(channel, _keys, LocalNodeId, _incarnation, timeout.Token).ConfigureAwait(false);

            var ch = channel;
            channel = null; // ownership moves to the pool (or is closed by the race loser below)
            return await PostWithResult<IMessageChannel?>(tcs =>
            {
                if (_directChannels.TryGetValue(peerId, out var raced))
                {
                    _ = ch.DisposeAsync(); // another route won the connect race
                    raced.LastUsedTs = Now();
                    tcs.TrySetResult(raced.Channel);
                    return ValueTask.CompletedTask;
                }

                _directChannels[peerId] = new DirectChannel { Channel = ch, LastUsedTs = Now() };
                StartReadPump(new PeerConn
                {
                    Channel = ch,
                    PeerId = remoteId,
                    PeerIncarnation = remoteIncarnation,
                    IsCoordinatorLink = false,
                });
                tcs.TrySetResult(ch);
                return ValueTask.CompletedTask;
            }).WaitAsync(ct).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogDebug(ex, "Direct routing channel to {Peer} failed; falling back to coordinator relay", peerId);
            return null; // caller falls back to the relay path
        }
        finally
        {
            if (channel is not null)
                await channel.DisposeAsync().ConfigureAwait(false);
        }
    }

    private void TickDirectChannels(long now)
    {
        if (_directChannels.Count == 0)
            return;

        foreach (var (peerId, pooled) in _directChannels.Where(kv => Since(kv.Value.LastUsedTs) > _options.Routing.IdleChannelTimeout).ToList())
        {
            _directChannels.Remove(peerId);
            _ = pooled.Channel.DisposeAsync();
        }
    }

    private void HandleDirectChannelClosed(PeerConn peer)
    {
        if (_directChannels.TryGetValue(peer.PeerId, out var pooled) && ReferenceEquals(pooled.Channel, peer.Channel))
            _directChannels.Remove(peer.PeerId);
    }
}
