using System.Net;
using CoLibra.Leasing;
using CoLibra.Protocol;
using CoLibra.Security;
using CoLibra.Transport;
using Microsoft.Extensions.Logging;

namespace CoLibra.Runtime;

internal sealed partial class CoLibraNode
{
    // =====================================================================================
    // Peer message routing (actor)
    // =====================================================================================

    private void HandlePeerMessage(PeerConn peer, Message message)
    {
        switch (message)
        {
            case JoinRequestMessage m:
                HandleJoinRequest(peer, m);
                break;

            case HeartbeatMessage m:
                HandleHeartbeat(peer, m);
                break;

            case LeaseAcquireMessage m when _coordinator is { } coordinator:
                if (coordinator.Sessions.TryGetValue(peer.PeerId, out var session))
                    session.LastSeenTs = Now();
                HandleAcquireAsCoordinator(coordinator, peer.PeerId, m);
                break;

            case LeaseReleaseMessage m when _coordinator is { } coordinator:
                var releasedKey = new LeaseKey(m.LeaseType, m.LeaseId);
                if (m.AsCompleted && _completions is not null)
                {
                    // Completed, not available: release without notifying waiters, then replicate.
                    coordinator.Table.Release(peer.PeerId, releasedKey, out _);
                    EnqueueCompletionBroadcast(coordinator, [releasedKey]);
                }
                else if (coordinator.Table.Release(peer.PeerId, releasedKey, out var interested))
                {
                    NotifyAvailable(coordinator, [(releasedKey, interested)]);
                }

                break;

            case CompletionSyncMessage m:
                HandleCompletionSync(peer, m);
                break;

            case ElectionStartMessage m:
                HandleElectionStart(peer, m);
                break;

            case HeartbeatAckMessage m when peer.IsCoordinatorLink && IsCurrentCoordinatorLink(peer):
                HandleHeartbeatAck(m);
                break;

            case MembershipUpdateMessage m when peer.IsCoordinatorLink && IsCurrentCoordinatorLink(peer):
                _highestTerm = Math.Max(_highestTerm, m.Term);
                if (_member is { } member)
                {
                    member.LastCoordinatorSignalTs = Now();
                    ApplyMembership(m.Members, member.CoordinatorId, member.CoordinatorEndpoint.Address);
                }

                break;

            case LeaseGrantResultMessage m when peer.IsCoordinatorLink && IsCurrentCoordinatorLink(peer):
                CompletePending(m.RequestId, new GrantResult(
                    m.Outcome == LeaseOutcome.Granted,
                    new FencingToken(m.Term, m.Sequence),
                    m.DenialReason,
                    m.CurrentOwner is { } owner ? new NodeId(owner) : null));
                break;

            case LeaseAvailableNotifyMessage m when peer.IsCoordinatorLink && IsCurrentCoordinatorLink(peer):
                HandleLeaseAvailable(m.Keys.Select(k => k.ToKey()).ToList());
                break;

            // ---- routed delivery ----
            case OwnerResolveMessage m when _coordinator is { } coordinator:
                HandleOwnerResolveAsCoordinator(coordinator, peer.PeerId, m);
                break;

            case OwnerResolveReplyMessage m when peer.IsCoordinatorLink && IsCurrentCoordinatorLink(peer):
                CompleteResolve(m);
                break;

            case LeaseAssignMessage m when peer.IsCoordinatorLink && IsCurrentCoordinatorLink(peer):
                HandleLeaseAssign(peer, m);
                break;

            case LeaseAssignAckMessage m when _coordinator is { } coordinator:
                HandleLeaseAssignAck(coordinator, peer.PeerId, m);
                break;

            case RoutedPayloadMessage m:
                HandleRoutedPayload(peer, m);
                break;

            case RoutedAckMessage m:
                HandleRoutedAck(peer, m);
                break;

            // ---- direct node-to-node messaging ----
            case DirectMessageMessage m:
                HandleDirectMessage(peer, m);
                break;

            case DirectMessageAckMessage m:
                HandleDirectMessageAck(peer, m);
                break;
        }
    }

    private bool IsCurrentCoordinatorLink(PeerConn peer) => _member is { } member && ReferenceEquals(member.Connection, peer.Channel);

    private void HandlePeerClosed(PeerConn peer)
    {
        _ = peer.Channel.DisposeAsync();
        HandleDirectChannelClosed(peer);

        if (peer.IsCoordinatorLink && IsCurrentCoordinatorLink(peer) && _state == ClusterState.Member)
        {
            _logger.LogWarning("Connection to coordinator {Coordinator} lost; starting election", peer.PeerId);
            StartElection();
            return;
        }

        if (_coordinator is { } coordinator &&
            coordinator.Sessions.TryGetValue(peer.PeerId, out var session) &&
            ReferenceEquals(session.Connection, peer.Channel))
        {
            RemoveMember(coordinator, peer.PeerId, "connection closed");
        }
    }

    private void HandleLeaseAvailable(List<LeaseKey> keys)
    {
        _negativeCache.Invalidate(keys);
        Raise(LeaseAvailable, new LeaseAvailableEventArgs { Keys = keys });
    }

    // =====================================================================================
    // Completion tracking: union-merge replication
    // =====================================================================================

    /// <summary>Records completions in the local registry and queues the newly-learned ones for broadcast.</summary>
    private void EnqueueCompletionBroadcast(CoordinatorRole coordinator, IReadOnlyList<LeaseKey> keys)
    {
        if (_completions is null)
            return;

        foreach (var key in _completions.AddRange(keys, Now()))
            coordinator.PendingCompletionSync.Add(LeaseKeyDto.From(key));
    }

    private void HandleCompletionSync(PeerConn peer, CompletionSyncMessage message)
    {
        if (_completions is null)
            return;

        var keys = message.Entries.Select(e => e.ToKey()).ToList();
        if (_coordinator is { } coordinator)
        {
            // A member's snapshot upload (fresh join or post-failover rejoin): union it and
            // re-broadcast whatever was news, so every copy converges to the same set.
            if (coordinator.Sessions.ContainsKey(peer.PeerId))
                EnqueueCompletionBroadcast(coordinator, keys);
        }
        else if (peer.IsCoordinatorLink && IsCurrentCoordinatorLink(peer))
        {
            _completions.AddRange(keys, Now());
        }
    }

    private void FlushCompletionSync(CoordinatorRole coordinator)
    {
        if (coordinator.PendingCompletionSync.Count == 0)
            return;

        foreach (var chunk in coordinator.PendingCompletionSync.Chunk(CompletionSyncMessage.ChunkSize))
        {
            var message = new CompletionSyncMessage(chunk);
            foreach (var session in coordinator.Sessions.Values.Where(s => s.SupportsCompletionSync))
                _ = SendSafeAsync(session.Connection, message);
        }

        coordinator.PendingCompletionSync.Clear();
    }

    /// <summary>Streams the full local registry to one peer in frame-safe chunks (join-time exchange).</summary>
    private void SendCompletionSnapshot(IMessageChannel connection)
    {
        if (_completions is null)
            return;

        foreach (var chunk in _completions.Snapshot().Chunk(CompletionSyncMessage.ChunkSize))
            _ = SendSafeAsync(connection, new CompletionSyncMessage([.. chunk.Select(LeaseKeyDto.From)]));
    }

    // =====================================================================================
    // Coordinator: join / heartbeat / leases / membership
    // =====================================================================================

    private void HandleJoinRequest(PeerConn peer, JoinRequestMessage request)
    {
        if (_coordinator is not { } coordinator)
        {
            var hint = _member?.CoordinatorEndpoint;
            _ = SendSafeAsync(peer.Channel, new JoinRejectedMessage(
                JoinRejectionReason.NotCoordinator, "This node is not the coordinator.",
                hint?.Address.ToString(), hint?.Port ?? 0));
            return;
        }

        if (request.ProtocolVersion != ProtocolConstants.ProtocolVersion)
        {
            _ = SendSafeAsync(peer.Channel, new JoinRejectedMessage(
                JoinRejectionReason.ProtocolIncompatible,
                $"Protocol version {request.ProtocolVersion} is incompatible with {ProtocolConstants.ProtocolVersion}.",
                null, 0));
            return;
        }

        if (!Version.TryParse(request.ServiceVersion, out var peerVersion) ||
            !_options.VersionCompatibility.IsCompatible(_serviceVersion, peerVersion))
        {
            _ = SendSafeAsync(peer.Channel, new JoinRejectedMessage(
                JoinRejectionReason.VersionMismatch,
                $"Service version {request.ServiceVersion} is incompatible with {_serviceVersion} under rule {_options.VersionCompatibility}.",
                null, 0));
            return;
        }

        if (peer.PeerId == LocalNodeId)
        {
            _ = SendSafeAsync(peer.Channel, new JoinRejectedMessage(
                JoinRejectionReason.DuplicateNodeId,
                $"NodeId {peer.PeerId} is the coordinator's own identity.", null, 0));
            return;
        }

        if (coordinator.Sessions.TryGetValue(peer.PeerId, out var existing))
        {
            // A session that is still heartbeating is a live duplicate; only a stale (ghost)
            // session may be superseded by a restart with a newer incarnation.
            var existingIsLive = Since(existing.LastSeenTs) < _options.MemberTimeout;
            if (existingIsLive || peer.PeerIncarnation <= existing.Incarnation)
            {
                _ = SendSafeAsync(peer.Channel, new JoinRejectedMessage(
                    JoinRejectionReason.DuplicateNodeId,
                    $"NodeId {peer.PeerId} is already an active cluster member.", null, 0));
                return;
            }

            _ = existing.Connection.DisposeAsync();
            coordinator.Sessions.Remove(peer.PeerId);
            NotifyAvailable(coordinator, coordinator.Table.NodeDown(peer.PeerId));
        }

        var now = Now();
        var remoteAddress = peer.Channel.RemoteEndPoint is IPEndPoint ip ? ip.Address : IPAddress.None;
        var session = new MemberSession
        {
            Id = peer.PeerId,
            Incarnation = peer.PeerIncarnation,
            Connection = peer.Channel,
            Dto = new MemberDto(peer.PeerId.Value, peer.PeerIncarnation, remoteAddress.ToString(),
                request.MeshPort, request.ServiceVersion, request.Weight, false, request.NodeName),
            LastSeenTs = now,
            SupportsCompletionSync = request.SupportsCompletionSync,
            RoutedTypes = request.RoutedTypes ?? [],
        };
        coordinator.Sessions[peer.PeerId] = session;
        coordinator.RecentlyDeparted.Remove(peer.PeerId);
        coordinator.Table.NodeUp(peer.PeerId, request.Weight);

        var rejected = coordinator.Table.AssertHeld(
            peer.PeerId,
            [.. request.HeldLeases.Select(h => (h.ToKey(), h.ToToken()))],
            now);

        _ = SendSafeAsync(peer.Channel, new JoinResponseMessage(
            coordinator.Term,
            BuildMemberDtos(coordinator),
            [.. rejected.Select(LeaseKeyDto.From)],
            _options.LeaseTtl.TotalSeconds));

        if (session.SupportsCompletionSync)
            SendCompletionSnapshot(peer.Channel);

        UpdateCoordinatorMembership(coordinator);
        _logger.LogInformation("Node {NodeId} joined ({Count} members)", peer.PeerId, coordinator.Sessions.Count + 1);
    }

    private void HandleHeartbeat(PeerConn peer, HeartbeatMessage heartbeat)
    {
        if (_coordinator is not { } coordinator || !coordinator.Sessions.TryGetValue(peer.PeerId, out var session))
            return;

        var now = Now();
        session.LastSeenTs = now;
        session.Dto = session.Dto with { Weight = heartbeat.Weight };
        if (heartbeat.RoutedTypes is not null)
            session.RoutedTypes = heartbeat.RoutedTypes;
        coordinator.Table.SetWeight(peer.PeerId, heartbeat.Weight);

        var lost = coordinator.Table.Renew(
            peer.PeerId,
            [.. heartbeat.HeldLeases.Select(h => (h.ToKey(), h.ToToken()))],
            now);

        _ = SendSafeAsync(peer.Channel, new HeartbeatAckMessage(
            coordinator.Term, _options.LeaseTtl.TotalSeconds, [.. lost.Select(LeaseKeyDto.From)]));
    }

    private void HandleAcquireAsCoordinator(CoordinatorRole coordinator, NodeId requester, LeaseAcquireMessage request)
    {
        if (_state == ClusterState.QuorumLost && _options.SplitBrainPolicy != SplitBrainPolicy.Continue)
        {
            DispatchDecision(coordinator, new GrantDecision(
                requester, request.RequestId, new LeaseKey(request.LeaseType, request.LeaseId),
                false, default, LeaseDenialReason.SplitBrain, null));
            return;
        }

        if (coordinator.RebuildDeadlineTs > 0 && Now() < coordinator.RebuildDeadlineTs)
        {
            coordinator.RebuildQueue.Add((requester, request));
            return;
        }

        var outcome = coordinator.Table.Acquire(
            requester, request.RequestId, new LeaseKey(request.LeaseType, request.LeaseId),
            request.Preference, Now());
        if (outcome.Immediate is { } decision)
            DispatchDecision(coordinator, decision);
        foreach (var resolved in outcome.Resolved)
            DispatchDecision(coordinator, resolved);
    }

    private void DispatchDecision(CoordinatorRole coordinator, GrantDecision decision)
    {
        if (decision.Requester == LocalNodeId)
        {
            CompletePending(decision.RequestId, new GrantResult(
                decision.Granted, decision.Token, decision.Reason, decision.CurrentOwner));
            return;
        }

        if (coordinator.Sessions.TryGetValue(decision.Requester, out var session))
        {
            _ = SendSafeAsync(session.Connection, new LeaseGrantResultMessage(
                decision.RequestId,
                decision.Granted ? LeaseOutcome.Granted : LeaseOutcome.Denied,
                decision.Token.Term, decision.Token.Sequence,
                _options.LeaseTtl.TotalSeconds,
                decision.Reason,
                decision.CurrentOwner?.Value));
        }
    }

    private void NotifyAvailable(CoordinatorRole coordinator, List<(LeaseKey Key, List<NodeId> Interested)> freed)
    {
        if (freed.Count == 0)
            return;

        var perNode = new Dictionary<NodeId, List<LeaseKeyDto>>();
        foreach (var (key, interested) in freed)
        {
            foreach (var node in interested)
            {
                if (!perNode.TryGetValue(node, out var list))
                    perNode[node] = list = [];
                list.Add(LeaseKeyDto.From(key));
            }
        }

        foreach (var (node, keys) in perNode)
        {
            if (node == LocalNodeId)
                HandleLeaseAvailable([.. keys.Select(k => k.ToKey())]);
            else if (coordinator.Sessions.TryGetValue(node, out var session))
                _ = SendSafeAsync(session.Connection, new LeaseAvailableNotifyMessage(keys));
        }
    }

    private void RemoveMember(CoordinatorRole coordinator, NodeId nodeId, string reason)
    {
        if (!coordinator.Sessions.Remove(nodeId, out var session))
            return;

        _logger.LogWarning("Member {NodeId} removed: {Reason}", nodeId, reason);
        _ = session.Connection.DisposeAsync();
        coordinator.RecentlyDeparted[nodeId] = Now() + ToTicks(_options.MemberTimeout) * 6;
        NotifyAvailable(coordinator, coordinator.Table.NodeDown(nodeId));
        HandleAssigneeDeparted(coordinator, nodeId);
        UpdateCoordinatorMembership(coordinator);
    }

    private List<MemberDto> BuildMemberDtos(CoordinatorRole coordinator)
    {
        var dtos = new List<MemberDto>(coordinator.Sessions.Count + 1)
        {
            new(LocalNodeId.Value, _incarnation, _transport.MeshEndpoint.Address.ToString(),
                _transport.MeshEndpoint.Port, _serviceVersion.ToString(), _options.Weight, true, _options.NodeName),
        };
        dtos.AddRange(coordinator.Sessions.Values.Select(s => s.Dto));
        return dtos;
    }

    private void UpdateCoordinatorMembership(CoordinatorRole coordinator)
    {
        var dtos = BuildMemberDtos(coordinator);
        var previous = _members;
        var next = new List<ClusterMember>(dtos.Count);
        foreach (var dto in dtos)
        {
            if (!Version.TryParse(dto.ServiceVersion, out var version))
                version = new Version(0, 0);
            next.Add(new ClusterMember
            {
                NodeId = new NodeId(dto.NodeId),
                Incarnation = dto.Incarnation,
                Endpoint = new IPEndPoint(
                    IPAddress.TryParse(dto.Host, out var parsed) ? parsed : IPAddress.None, dto.Port),
                ServiceVersion = version,
                Weight = dto.Weight,
                IsCoordinator = dto.IsCoordinator,
                Name = dto.Name,
            });
        }

        _members = next;
        _lastKnownClusterSize = next.Count + coordinator.RecentlyDeparted.Count;
        RaiseMembershipDiff(previous, next);

        var update = new MembershipUpdateMessage(coordinator.Term, dtos);
        foreach (var session in coordinator.Sessions.Values)
            _ = SendSafeAsync(session.Connection, update);
    }

    // =====================================================================================
    // Member: coordinator link handling
    // =====================================================================================

    private void HandleHeartbeatAck(HeartbeatAckMessage ack)
    {
        if (_member is not { } member)
            return;

        var now = Now();
        member.LastCoordinatorSignalTs = now;
        Volatile.Write(ref _lastAckTimestamp, now);
        _highestTerm = Math.Max(_highestTerm, ack.Term);
        foreach (var lost in ack.LostKeys)
            RemoveHeld(lost.ToKey(), LeaseLossReason.OwnedElsewhere);
    }

    // =====================================================================================
    // Election: bully with terms
    // =====================================================================================

    private void StartElection()
    {
        if (_state is ClusterState.Electing or ClusterState.Faulted or ClusterState.Stopped || _coordinator is not null)
            return;
        StartElectionCore();
    }

    private void StartElectionCore()
    {
        _member?.Dispose();
        _member = null;
        foreach (var pending in _pendingAcquires.Values)
            pending.Sent = false; // re-dispatched after the election settles
        FailAllPendingResolves(); // resolvers retry against whichever coordinator emerges

        SetState(ClusterState.Electing);
        var proposedTerm = ++_highestTerm;
        _election = new ElectionRound { ProposedTerm = proposedTerm };
        var peers = _members.Where(m => m.NodeId != LocalNodeId).ToList();
        _logger.LogInformation("Starting election for term {Term} ({PeerCount} known peers)", proposedTerm, peers.Count);

        if (peers.Count == 0)
        {
            ResolveElection(proposedTerm, []);
            return;
        }

        _ = Task.Run(async () =>
        {
            var contacts = await Task.WhenAll(peers.Select(p => ContactPeerAsync(p, proposedTerm))).ConfigureAwait(false);
            Post(() =>
            {
                ResolveElection(proposedTerm, contacts);
                return ValueTask.CompletedTask;
            });
        }, CancellationToken.None);
    }

    private async Task<ElectionContact> ContactPeerAsync(ClusterMember peer, long proposedTerm)
    {
        IMessageChannel? channel = null;
        try
        {
            using var timeout = CancellationTokenSource.CreateLinkedTokenSource(_stopping.Token);
            timeout.CancelAfter(_options.ElectionTimeout);
            var ct = timeout.Token;

            channel = await _transport.ConnectAsync(peer.Endpoint, ct).ConfigureAwait(false);
            await Handshake.AsClientAsync(channel, _keys, LocalNodeId, _incarnation, ct).ConfigureAwait(false);
            await channel.SendAsync(new ElectionStartMessage(proposedTerm, LocalNodeId.Value), ct).ConfigureAwait(false);

            while (true)
            {
                var reply = await channel.ReceiveAsync(ct).ConfigureAwait(false);
                if (reply is null)
                    return new ElectionContact(false, false, false, peer.Endpoint, null);
                if (reply is ElectionAliveMessage alive)
                {
                    return new ElectionContact(
                        true, alive.WillContest, alive.IsCoordinator, peer.Endpoint,
                        ParseHint(alive.KnownCoordinatorHost, alive.KnownCoordinatorPort));
                }
            }
        }
        catch (Exception)
        {
            return new ElectionContact(false, false, false, peer.Endpoint, null);
        }
        finally
        {
            if (channel is not null)
                await channel.DisposeAsync().ConfigureAwait(false);
        }
    }

    private void ResolveElection(long proposedTerm, ElectionContact[] contacts)
    {
        if (_state != ClusterState.Electing || _election?.ProposedTerm != proposedTerm)
            return;

        var coordinatorContact = contacts.FirstOrDefault(c => c.IsCoordinator);
        if (coordinatorContact is { PeerEndpoint: { } coordinatorEndpoint })
        {
            BeginJoin(coordinatorEndpoint);
            return;
        }

        var hint = contacts.FirstOrDefault(c => c.CoordinatorHint is not null)?.CoordinatorHint;
        if (hint is not null)
        {
            BeginJoin(hint);
            return;
        }

        if (contacts.Any(c => c.WillContest))
        {
            // A higher node is contesting; wait for it to claim, retry if it never does.
            _election.RetryDeadlineTs = Now() + 2 * ToTicks(_options.ElectionTimeout);
            return;
        }

        var reachable = contacts.Count(c => c.Reachable) + 1;
        var required = Quorum(Math.Max(_lastKnownClusterSize, reachable));
        if (_options.QuorumPolicy == QuorumPolicy.Off || reachable >= required)
        {
            BecomeCoordinator(proposedTerm);
            return;
        }

        _logger.LogWarning("Election for term {Term}: only {Reachable}/{Required} nodes reachable; quorum lost",
            proposedTerm, reachable, required);
        Raise(SplitBrainDetected, new SplitBrainDetectedEventArgs
        {
            Kind = SplitBrainKind.QuorumLost,
            Detail = $"Only {reachable} of a required {required} nodes reachable during election.",
        });
        SetState(ClusterState.QuorumLost);
        _election.RetryDeadlineTs = Now() + ToTicks(_options.ElectionTimeout);
    }

    private void HandleElectionStart(PeerConn peer, ElectionStartMessage message)
    {
        _highestTerm = Math.Max(_highestTerm, message.Term);

        var isCoordinator = _coordinator is not null;
        var coordinatorHint = !isCoordinator && _member is { } member &&
            Since(member.LastCoordinatorSignalTs) < _options.MemberTimeout
                ? member.CoordinatorEndpoint
                : null;

        var orphaned = !isCoordinator && coordinatorHint is null;
        var willContest = orphaned && LocalNodeId.Value.CompareTo(message.CandidateNodeId) > 0;

        _ = SendSafeAsync(peer.Channel, new ElectionAliveMessage(
            _highestTerm, LocalNodeId.Value, willContest, isCoordinator,
            coordinatorHint?.Address.ToString(), coordinatorHint?.Port ?? 0));

        if (willContest && _state is not ClusterState.Electing and not ClusterState.Joining)
            StartElection();
    }

    private void BecomeCoordinator(long term)
    {
        _election = null;
        _member?.Dispose();
        _member = null;

        var now = Now();
        var table = new CoordinatorLeaseTable(term, _options, _time, _completions);
        var coordinator = new CoordinatorRole
        {
            Term = term,
            Table = table,
            RebuildDeadlineTs = _lastKnownClusterSize > 1 ? now + ToTicks(_options.RebuildWindow) : 0,
            LastSelfRenewTs = now,
        };

        table.NodeUp(LocalNodeId, _options.Weight);
        table.AssertHeld(LocalNodeId, [.. _held.Select(kv => (kv.Key, kv.Value.Token))], now);

        // Keep pre-election members in the quorum denominator until they rejoin or decay.
        foreach (var previous in _members.Where(m => m.NodeId != LocalNodeId))
            coordinator.RecentlyDeparted[previous.NodeId] = now + ToTicks(_options.MemberTimeout) * 6;

        _coordinator = coordinator;
        _highestTerm = Math.Max(_highestTerm, term);
        _negativeCache.Clear();
        Volatile.Write(ref _lastAckTimestamp, now);
        SetState(ClusterState.Coordinator);
        _logger.LogInformation("Became coordinator for term {Term}", term);

        UpdateCoordinatorMembership(coordinator);
        SendAnnounce();
        coordinator.LastAnnounceTs = now;

        foreach (var pending in _pendingAcquires.Values.ToList())
            DispatchPendingAcquire(pending);
    }

    // =====================================================================================
    // Tick
    // =====================================================================================

    private void HandleTick()
    {
        var now = Now();
        switch (_state)
        {
            case ClusterState.Discovering:
                if (Since(_lastProbeTs) >= _options.AnnounceInterval)
                {
                    _lastProbeTs = now;
                    SendProbes();
                }

                if (now >= _discoveryDeadlineTs)
                {
                    if (_lastKnownClusterSize <= 1)
                        BecomeCoordinator(_highestTerm + 1);
                    else
                        StartElectionCore(); // rejoin scenarios go through quorum-checked election
                }

                break;

            case ClusterState.Member:
                TickMember(now);
                break;

            case ClusterState.Coordinator:
                TickCoordinator(now);
                break;

            case ClusterState.QuorumLost when _coordinator is not null:
                TickCoordinator(now);
                break;

            case ClusterState.Electing or ClusterState.QuorumLost:
                if (Since(_lastProbeTs) >= _options.AnnounceInterval)
                {
                    _lastProbeTs = now;
                    SendProbes(); // keep listening for a coordinator we can join
                }

                if (_election is { RetryDeadlineTs: > 0 } round && now >= round.RetryDeadlineTs)
                    StartElectionCore();
                break;
        }

        TickPendingTimeouts(now);
        TickLocalLeaseExpiry();
        TickDirectChannels(now);

        if (_completions is not null && Since(_lastCompletionTrimTs) >= CompletionTrimInterval)
        {
            _lastCompletionTrimTs = now;
            _completions.TrimExpired(now);
        }
    }

    private static readonly TimeSpan CompletionTrimInterval = TimeSpan.FromSeconds(5);
    private long _lastCompletionTrimTs;

    private void TickMember(long now)
    {
        if (_member is not { } member)
            return;

        if (Since(member.LastHeartbeatSentTs) >= _options.HeartbeatInterval)
        {
            member.LastHeartbeatSentTs = now;
            _ = SendSafeAsync(member.Connection, new HeartbeatMessage(
                BuildHeldDtos(), BuildTypeCounts(), _options.Weight, _routedTypesSnapshot));
        }

        if (Since(member.LastCoordinatorSignalTs) > _options.MemberTimeout)
        {
            _logger.LogWarning("Coordinator {Coordinator} silent for over {Timeout}; starting election",
                member.CoordinatorId, _options.MemberTimeout);
            StartElection();
        }
    }

    private void TickCoordinator(long now)
    {
        if (_coordinator is not { } coordinator)
            return;

        if (Since(coordinator.LastAnnounceTs) >= _options.AnnounceInterval)
        {
            coordinator.LastAnnounceTs = now;
            SendAnnounce();
        }

        if (Since(coordinator.LastSelfRenewTs) >= _options.HeartbeatInterval)
        {
            coordinator.LastSelfRenewTs = now;
            var lost = coordinator.Table.Renew(LocalNodeId, [.. _held.Select(kv => (kv.Key, kv.Value.Token))], now);
            Volatile.Write(ref _lastAckTimestamp, now);
            foreach (var key in lost)
                RemoveHeld(key, LeaseLossReason.OwnedElsewhere);
        }

        foreach (var session in coordinator.Sessions.Values.Where(s => Since(s.LastSeenTs) > _options.MemberTimeout).ToList())
            RemoveMember(coordinator, session.Id, "heartbeat timeout");

        var sweep = coordinator.Table.Sweep(now);
        NotifyAvailable(coordinator, sweep.Freed);
        foreach (var matured in sweep.MaturedGrants)
            DispatchDecision(coordinator, matured);

        FlushCompletionSync(coordinator);
        TickPendingAssignments(coordinator, now);

        if (coordinator.RebuildDeadlineTs > 0 && now >= coordinator.RebuildDeadlineTs)
        {
            coordinator.RebuildDeadlineTs = 0;
            var queued = coordinator.RebuildQueue.ToList();
            coordinator.RebuildQueue.Clear();
            foreach (var (requester, message) in queued)
                HandleAcquireAsCoordinator(coordinator, requester, message);
        }

        var expiredDeparted = coordinator.RecentlyDeparted.Where(kv => kv.Value <= now).Select(kv => kv.Key).ToList();
        foreach (var nodeId in expiredDeparted)
            coordinator.RecentlyDeparted.Remove(nodeId);

        var alive = coordinator.Sessions.Count + 1;
        var knownSize = alive + coordinator.RecentlyDeparted.Count;
        _lastKnownClusterSize = knownSize;
        var quorumOk = _options.QuorumPolicy == QuorumPolicy.Off || alive >= Quorum(knownSize);
        if (!quorumOk && _state == ClusterState.Coordinator)
        {
            Raise(SplitBrainDetected, new SplitBrainDetectedEventArgs
            {
                Kind = SplitBrainKind.QuorumLost,
                Detail = $"Coordinator sees {alive} of {knownSize} known nodes; below quorum {Quorum(knownSize)}.",
            });
            SetState(ClusterState.QuorumLost);
        }
        else if (quorumOk && _state == ClusterState.QuorumLost)
        {
            SetState(ClusterState.Coordinator);
        }
    }

    private void TickPendingTimeouts(long now)
    {
        if (_pendingAcquires.Count == 0)
            return;

        var timeoutTicks = ToTicks(_options.ElectionTimeout + _options.RebuildWindow + _options.MemberTimeout);
        foreach (var pending in _pendingAcquires.Values.Where(p => now - p.CreatedTs > timeoutTicks).ToList())
            CompletePending(pending.RequestId, new GrantResult(false, default, LeaseDenialReason.NoCoordinator, null));
    }

    private void TickLocalLeaseExpiry()
    {
        if (_isCoordinatorRole || _held.Count == 0)
            return;

        if (Since(Volatile.Read(ref _lastAckTimestamp)) >= _options.LeaseTtl - _options.LeaseRenewSafetyMargin)
        {
            _logger.LogWarning("Lease renewals unacknowledged beyond the safety margin; releasing {Count} held leases locally",
                _held.Count);
            foreach (var key in _held.Keys.ToList())
                RemoveHeld(key, LeaseLossReason.RenewalTimedOut);
        }
    }
}
