using System.Net;
using CoLibra.Leasing;
using CoLibra.Protocol;
using CoLibra.Security;
using CoLibra.Transport;
using Microsoft.Extensions.Logging;

namespace CoLibra.Runtime;

internal sealed partial class CoLibraNode
{
    private static int Quorum(int clusterSize) => clusterSize <= 2 ? 1 : clusterSize / 2 + 1;

    // =====================================================================================
    // Roles
    // =====================================================================================

    private sealed class CoordinatorRole
    {
        public required long Term { get; set; } // set only by EscalateTerm (Forced-mode defense)
        public required CoordinatorLeaseTable Table { get; init; }
        public Dictionary<NodeId, MemberSession> Sessions { get; } = [];
        public List<(NodeId Requester, LeaseAcquireMessage Message)> RebuildQueue { get; } = [];
        public long RebuildDeadlineTs { get; set; }
        public long LastAnnounceTs { get; set; }
        public long LastSelfRenewTs { get; set; }
        public Dictionary<NodeId, long> RecentlyDeparted { get; } = [];
        public List<LeaseKeyDto> PendingCompletionSync { get; } = [];
        public Dictionary<LeaseKey, PendingAssignment> PendingAssignments { get; } = [];
        public Dictionary<string, PendingPunch> PendingPunches { get; } = new(StringComparer.Ordinal);
        public int NextWireId { get; set; } = 2; // 1 = the coordinator itself, 0 = unassigned

        public void DisposeAllSessions()
        {
            foreach (var session in Sessions.Values)
                _ = session.Connection.DisposeAsync();
            Sessions.Clear();
        }
    }

    private sealed class MemberSession
    {
        public required NodeId Id { get; init; }
        public required long Incarnation { get; init; }
        public required IMessageChannel Connection { get; init; }
        public required MemberDto Dto { get; set; }
        public long LastSeenTs { get; set; }
        public bool SupportsCompletionSync { get; init; }
        public IReadOnlyList<string> RoutedTypes { get; set; } = [];
    }

    private sealed class MemberRole
    {
        public required NodeId CoordinatorId { get; init; }
        public required IPEndPoint CoordinatorEndpoint { get; init; }
        public required IMessageChannel Connection { get; init; }
        public long LastCoordinatorSignalTs { get; set; }
        public long LastHeartbeatSentTs { get; set; }

        public void Dispose() => _ = Connection.DisposeAsync();
    }

    private sealed class ElectionRound
    {
        public required long ProposedTerm { get; init; }
        public long RetryDeadlineTs { get; set; }
    }

    private sealed class PeerConn
    {
        public required IMessageChannel Channel { get; init; }
        public required NodeId PeerId { get; init; }
        public required long PeerIncarnation { get; init; }
        public bool IsCoordinatorLink { get; set; }
    }

    private sealed record ElectionContact(
        bool Reachable, bool WillContest, bool IsCoordinator, IPEndPoint? PeerEndpoint, IPEndPoint? CoordinatorHint);

    // =====================================================================================
    // Pumps
    // =====================================================================================

    private async Task PumpDatagramsAsync(CancellationToken ct)
    {
        try
        {
            await foreach (var datagram in _transport.Datagrams.ReadAllAsync(ct).ConfigureAwait(false))
            {
                var message = _discoveryCodec.TryDecode(datagram.Payload);
                if (message is null)
                    continue;
                var source = datagram.Source;
                Post(() =>
                {
                    HandleDiscoveryMessage(message, source);
                    return ValueTask.CompletedTask;
                });
            }
        }
        catch (OperationCanceledException)
        {
        }
    }

    private async Task PumpInboundAsync(CancellationToken ct)
    {
        try
        {
            await foreach (var channel in _transport.Inbound.ReadAllAsync(ct).ConfigureAwait(false))
            {
                _ = AuthenticateInboundAsync(channel, ct);
            }
        }
        catch (OperationCanceledException)
        {
        }
    }

    private async Task AuthenticateInboundAsync(IMessageChannel channel, CancellationToken ct)
    {
        try
        {
            var (peerId, incarnation) = await Handshake.AsServerAsync(channel, _keys, LocalNodeId, _incarnation, ct)
                .ConfigureAwait(false);
            var peer = new PeerConn { Channel = channel, PeerId = peerId, PeerIncarnation = incarnation };
            StartReadPump(peer);
        }
        catch (Exception ex) when (ex is not OperationCanceledException || !ct.IsCancellationRequested)
        {
            _logger.LogDebug(ex, "Inbound peer failed authentication");
            await channel.DisposeAsync().ConfigureAwait(false);
        }
    }

    private void StartReadPump(PeerConn peer)
    {
        _ = Task.Run(async () =>
        {
            try
            {
                while (true)
                {
                    var message = await peer.Channel.ReceiveAsync(_stopping.Token).ConfigureAwait(false);
                    if (message is null)
                        break;
                    Post(() =>
                    {
                        HandlePeerMessage(peer, message);
                        return ValueTask.CompletedTask;
                    });
                }
            }
            catch (OperationCanceledException)
            {
                return;
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "Peer read pump ended with error");
            }

            Post(() =>
            {
                HandlePeerClosed(peer);
                return ValueTask.CompletedTask;
            });
        }, CancellationToken.None);
    }

    // =====================================================================================
    // Discovery
    // =====================================================================================

    private void SendProbes()
    {
        var probe = _discoveryCodec.Encode(new ProbeMessage(LocalNodeId.Value, _serviceVersion.ToString()));
        var knownEndpoints = _members
            .Where(m => m.NodeId != LocalNodeId)
            .Select(m => m.Endpoint)
            .ToList();
        _ = Task.Run(async () =>
        {
            try
            {
                await _transport.SendDatagramAsync(probe, null, _stopping.Token).ConfigureAwait(false);
                foreach (var seed in _options.StaticSeeds)
                {
                    var colon = seed.LastIndexOf(':');
                    var host = seed[..colon];
                    var port = int.Parse(seed[(colon + 1)..]);
                    try
                    {
                        var addresses = await Dns.GetHostAddressesAsync(host, System.Net.Sockets.AddressFamily.InterNetwork, _stopping.Token)
                            .ConfigureAwait(false);
                        foreach (var address in addresses)
                            await _transport.SendDatagramAsync(probe, new IPEndPoint(address, port), _stopping.Token).ConfigureAwait(false);
                    }
                    catch (Exception ex) when (ex is not OperationCanceledException)
                    {
                        _logger.LogDebug(ex, "Seed probe to {Seed} failed", seed);
                    }
                }

                // Also probe last-known members directly (their discovery socket shares the mesh host).
                foreach (var endpoint in knownEndpoints)
                {
                    await _transport.SendDatagramAsync(probe, new IPEndPoint(endpoint.Address, _options.DiscoveryPort), _stopping.Token)
                        .ConfigureAwait(false);
                }
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                _logger.LogDebug(ex, "Probe send failed");
            }
        }, CancellationToken.None);
    }

    private bool IsForced => _options.CoordinatorMode == CoordinatorMode.Forced;

    private void SendAnnounce()
    {
        var announce = _discoveryCodec.Encode(new AnnounceMessage(
            LocalNodeId.Value, _incarnation, _coordinator is not null,
            _coordinator?.Term ?? _highestTerm, _serviceVersion.ToString(), _transport.MeshEndpoint.Port,
            Forced: IsForced));
        _ = Task.Run(async () =>
        {
            try
            {
                await _transport.SendDatagramAsync(announce, null, _stopping.Token).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                _logger.LogDebug(ex, "Announce send failed");
            }
        }, CancellationToken.None);
    }

    private void HandleDiscoveryMessage(Message message, IPEndPoint source)
    {
        switch (message)
        {
            case AnnounceMessage a when a.NodeId != LocalNodeId.Value:
                HandlePresence(new NodeId(a.NodeId), a.IsCoordinator, a.Term, a.ServiceVersion,
                    new IPEndPoint(source.Address, a.MeshPort), null, a.Forced);
                break;

            case ProbeReplyMessage r when r.NodeId != LocalNodeId.Value:
                HandlePresence(new NodeId(r.NodeId), r.IsCoordinator, r.Term, r.ServiceVersion,
                    new IPEndPoint(source.Address, r.MeshPort), ParseHint(r.CoordinatorHost, r.CoordinatorPort), r.Forced);
                break;

            case ProbeMessage p when p.NodeId != LocalNodeId.Value:
                HandleProbe(p, source);
                break;
        }
    }

    private static IPEndPoint? ParseHint(string? host, int port) =>
        host is not null && IPAddress.TryParse(host, out var address) && port > 0 ? new IPEndPoint(address, port) : null;

    private void HandleProbe(ProbeMessage probe, IPEndPoint source)
    {
        if (!Version.TryParse(probe.ServiceVersion, out var peerVersion) ||
            !_options.VersionCompatibility.IsCompatible(_serviceVersion, peerVersion))
            return;

        IPEndPoint? coordinatorEndpoint = _coordinator is not null
            ? _transport.MeshEndpoint
            : _member?.CoordinatorEndpoint;
        if (coordinatorEndpoint is null)
            return; // nothing useful to say while discovering/electing

        var reply = _discoveryCodec.Encode(new ProbeReplyMessage(
            LocalNodeId.Value, _incarnation, _coordinator is not null,
            _coordinator?.Term ?? _highestTerm, _serviceVersion.ToString(), _transport.MeshEndpoint.Port,
            _coordinator is not null ? null : coordinatorEndpoint.Address.ToString(),
            _coordinator is not null ? 0 : coordinatorEndpoint.Port,
            Forced: IsForced));
        _ = Task.Run(async () =>
        {
            try
            {
                await _transport.SendDatagramAsync(reply, source, _stopping.Token).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                _logger.LogDebug(ex, "Probe reply failed");
            }
        }, CancellationToken.None);

        // Same-machine processes share the discovery port, so a unicast reply may land in the
        // wrong process; an early multicast announce covers them.
        if (_coordinator is { } coordinator && Since(coordinator.LastAnnounceTs) > TimeSpan.FromMilliseconds(500))
        {
            coordinator.LastAnnounceTs = Now();
            SendAnnounce();
        }
    }

    private void HandlePresence(NodeId nodeId, bool isCoordinator, long term, string versionString,
        IPEndPoint meshEndpoint, IPEndPoint? coordinatorHint, bool forced = false)
    {
        if (!Version.TryParse(versionString, out var peerVersion) ||
            !_options.VersionCompatibility.IsCompatible(_serviceVersion, peerVersion))
        {
            _logger.LogDebug("Ignoring node {NodeId} with incompatible version {Version}", nodeId, versionString);
            return;
        }

        if (_incompatibleCoordinators.Contains(nodeId.Value))
            return;

        _highestTerm = Math.Max(_highestTerm, term);
        var coordinatorEndpoint = isCoordinator ? meshEndpoint : coordinatorHint;

        // A Forced node never joins a non-forced coordinator: it takes over at its discovery
        // deadline instead. It does yield to a FORCED coordinator (first forced node wins).
        if (IsForced && _coordinator is null && !(isCoordinator && forced))
            return;

        switch (_state)
        {
            case ClusterState.Discovering when coordinatorEndpoint is not null:
                BeginJoin(coordinatorEndpoint);
                break;

            case ClusterState.Electing or ClusterState.QuorumLost when _coordinator is null && isCoordinator:
                BeginJoin(coordinatorEndpoint!);
                break;

            case ClusterState.Member when isCoordinator && _member is { } member && nodeId == member.CoordinatorId:
                member.LastCoordinatorSignalTs = Now();
                break;

            case ClusterState.Coordinator or ClusterState.QuorumLost when _coordinator is { } coordinator && isCoordinator:
                HandleRivalCoordinator(coordinator, nodeId, term, meshEndpoint, forced);
                break;
        }
    }

    private void HandleRivalCoordinator(CoordinatorRole coordinator, NodeId rival, long rivalTerm, IPEndPoint rivalEndpoint,
        bool rivalForced)
    {
        Raise(SplitBrainDetected, new SplitBrainDetectedEventArgs
        {
            Kind = SplitBrainKind.RivalCoordinator,
            Detail = $"Rival coordinator {rival} (term {rivalTerm}) discovered; local term {coordinator.Term}.",
        });

        if (IsForced && !rivalForced)
        {
            // A Forced coordinator never yields to a non-forced rival: it out-terms it instead
            // and the rival's own step-down logic does the rest.
            if (rivalTerm >= coordinator.Term)
            {
                var escalated = rivalTerm + 1;
                _logger.LogWarning("Forced coordinator escalating term {Ours} -> {Escalated} over rival {Rival} (term {Term})",
                    coordinator.Term, escalated, rival, rivalTerm);
                EscalateTerm(coordinator, escalated);
            }

            SendAnnounce();
            return;
        }

        var rivalWins = rivalTerm > coordinator.Term ||
            (rivalTerm == coordinator.Term && rival > LocalNodeId);
        if (!rivalWins)
        {
            _logger.LogWarning("Rival coordinator {Rival} (term {Term}) should step down to us (term {Ours})",
                rival, rivalTerm, coordinator.Term);
            SendAnnounce(); // make sure the rival hears us and steps down
            return;
        }

        _logger.LogWarning("Stepping down: rival coordinator {Rival} (term {Term}) supersedes local term {Ours}",
            rival, rivalTerm, coordinator.Term);
        StepDownAndRejoin(rivalEndpoint);
    }

    /// <summary>
    /// Raises the coordinator's term in place (Forced-mode takeover defense). Lease state and
    /// sequence numbering carry over; fencing stays monotonic because the term only increases.
    /// </summary>
    private void EscalateTerm(CoordinatorRole coordinator, long newTerm)
    {
        coordinator.Term = newTerm;
        coordinator.Table.EscalateTerm(newTerm);
        _highestTerm = Math.Max(_highestTerm, newTerm);
        UpdateCoordinatorMembership(coordinator); // members learn the new term immediately
    }

    private void StepDownAndRejoin(IPEndPoint rivalEndpoint)
    {
        var coordinator = _coordinator!;
        // Re-queue local acquires that were parked for the rebuild window.
        foreach (var (_, message) in coordinator.RebuildQueue)
        {
            if (_pendingAcquires.TryGetValue(message.RequestId, out var pending))
                pending.Sent = false;
        }

        coordinator.DisposeAllSessions();
        _coordinator = null;
        _isCoordinatorRole = false;
        // We were authoritative until this instant; restart the renewal clock so held leases
        // stay valid through the rejoin (they are re-asserted in the JoinRequest).
        Volatile.Write(ref _lastAckTimestamp, Now());
        _negativeCache.Clear();
        SetState(ClusterState.Discovering);
        BeginJoin(rivalEndpoint);
    }

    // =====================================================================================
    // Join flow
    // =====================================================================================

    private void BeginJoin(IPEndPoint coordinatorEndpoint)
    {
        if (_state is ClusterState.Joining or ClusterState.Faulted or ClusterState.Stopped)
            return;

        _election = null;
        SetState(ClusterState.Joining);
        var heldDtos = BuildHeldDtos();
        _ = Task.Run(() => JoinFlowAsync(coordinatorEndpoint, heldDtos), CancellationToken.None);
    }

    private async Task JoinFlowAsync(IPEndPoint endpoint, List<HeldLeaseDto> heldDtos)
    {
        IMessageChannel? channel = null;
        try
        {
            using var timeout = CancellationTokenSource.CreateLinkedTokenSource(_stopping.Token);
            timeout.CancelAfter(ConnectTimeout + _options.ElectionTimeout);
            var ct = timeout.Token;

            channel = await _transport.ConnectAsync(endpoint, ct).ConfigureAwait(false);
            var (peerId, peerIncarnation) = await Handshake.AsClientAsync(channel, _keys, LocalNodeId, _incarnation, ct)
                .ConfigureAwait(false);
            await channel.SendAsync(new JoinRequestMessage(
                ProtocolConstants.ProtocolVersion, _serviceVersion.ToString(), _options.Weight,
                _transport.MeshEndpoint.Port, heldDtos,
                SupportsCompletionSync: _completions is not null,
                RoutedTypes: _routedTypesSnapshot,
                NodeName: _options.NodeName,
                AcceptsWork: _acceptWork,
                UdpPort: _udpListenPort), ct).ConfigureAwait(false);

            while (true)
            {
                var reply = await channel.ReceiveAsync(ct).ConfigureAwait(false);
                switch (reply)
                {
                    case JoinResponseMessage response:
                        var ch = channel;
                        channel = null; // ownership moves to the member role
                        Post(() =>
                        {
                            CompleteJoin(ch, peerId, peerIncarnation, endpoint, response);
                            return ValueTask.CompletedTask;
                        });
                        return;

                    case JoinRejectedMessage rejection:
                        Post(() =>
                        {
                            HandleJoinRejected(peerId, rejection);
                            return ValueTask.CompletedTask;
                        });
                        return;

                    case null:
                        throw new IOException("Connection closed during join.");

                    default:
                        continue; // unrelated message before the response; keep waiting
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Join to {Endpoint} failed", endpoint);
            Post(() =>
            {
                if (_state == ClusterState.Joining)
                {
                    SetState(ClusterState.Discovering);
                    _discoveryDeadlineTs = Now() + ToTicks(_options.DiscoveryWindow);
                }

                return ValueTask.CompletedTask;
            });
        }
        finally
        {
            if (channel is not null)
                await channel.DisposeAsync().ConfigureAwait(false);
        }
    }

    private void CompleteJoin(IMessageChannel channel, NodeId coordinatorId, long coordinatorIncarnation,
        IPEndPoint endpoint, JoinResponseMessage response)
    {
        if (_state != ClusterState.Joining)
        {
            _ = channel.DisposeAsync();
            return;
        }

        var now = Now();
        _member = new MemberRole
        {
            CoordinatorId = coordinatorId,
            CoordinatorEndpoint = endpoint,
            Connection = channel,
            LastCoordinatorSignalTs = now,
        };
        _highestTerm = Math.Max(_highestTerm, response.Term);
        _joinRedirects = 0;

        foreach (var rejected in response.RejectedAsserts)
            RemoveHeld(rejected.ToKey(), LeaseLossReason.ConflictLost);

        Volatile.Write(ref _lastAckTimestamp, now);
        Volatile.Write(ref _coordinatorLeaseTtlMs, (long)(response.LeaseTtlSeconds * 1000));
        _negativeCache.Clear();
        _ownerCache.Clear(); // owners resolved under the previous coordinator may be stale
        CloseAllUdpLinks("joined a coordinator (term/wire-id scope changed)");
        ApplyMembership(response.Members, coordinatorId, endpoint.Address);
        SetState(ClusterState.Member);

        StartReadPump(new PeerConn
        {
            Channel = channel,
            PeerId = coordinatorId,
            PeerIncarnation = coordinatorIncarnation,
            IsCoordinatorLink = true,
        });

        // Upload our completion set: this is what makes the registry survive coordinator death —
        // every member carries a full copy and rejoining unions it into the new coordinator.
        SendCompletionSnapshot(channel);

        foreach (var pending in _pendingAcquires.Values.ToList())
        {
            pending.Sent = false;
            DispatchPendingAcquire(pending);
        }
    }

    private void HandleJoinRejected(NodeId coordinatorId, JoinRejectedMessage rejection)
    {
        if (_state != ClusterState.Joining)
            return;

        switch (rejection.Reason)
        {
            case JoinRejectionReason.NotCoordinator
                when ParseHint(rejection.CoordinatorHost, rejection.CoordinatorPort) is { } redirect && _joinRedirects < 5:
                _joinRedirects++;
                SetState(ClusterState.Discovering);
                BeginJoin(redirect);
                break;

            case JoinRejectionReason.DuplicateNodeId:
                _logger.LogCritical("Cluster rejected this node: duplicate NodeId {NodeId}. {Detail}",
                    LocalNodeId, rejection.Detail);
                FailAllPending(LeaseDenialReason.NoCoordinator);
                SetState(ClusterState.Faulted);
                break;

            case JoinRejectionReason.VersionMismatch or JoinRejectionReason.ProtocolIncompatible:
                _logger.LogError("Coordinator {Coordinator} rejected join: {Detail}", coordinatorId, rejection.Detail);
                _incompatibleCoordinators.Add(coordinatorId.Value);
                SetState(ClusterState.Discovering);
                _discoveryDeadlineTs = Now() + ToTicks(_options.DiscoveryWindow);
                break;

            default:
                SetState(ClusterState.Discovering);
                _discoveryDeadlineTs = Now() + ToTicks(_options.DiscoveryWindow);
                break;
        }
    }

    private void ApplyMembership(IReadOnlyList<MemberDto> dtos, NodeId coordinatorId, IPAddress coordinatorAddress)
    {
        var previous = _members;
        var next = new List<ClusterMember>(dtos.Count);
        foreach (var dto in dtos)
        {
            if (!Version.TryParse(dto.ServiceVersion, out var version))
                version = new Version(0, 0);
            // The coordinator can't know its own outward-facing address; we do (it's the one we dialed).
            var address = dto.NodeId == coordinatorId.Value
                ? coordinatorAddress
                : IPAddress.TryParse(dto.Host, out var parsed) ? parsed : IPAddress.None;
            next.Add(new ClusterMember
            {
                NodeId = new NodeId(dto.NodeId),
                Incarnation = dto.Incarnation,
                Endpoint = new IPEndPoint(address, dto.Port),
                ServiceVersion = version,
                Weight = dto.Weight,
                IsCoordinator = dto.IsCoordinator,
                Name = dto.Name,
                AcceptsWork = dto.AcceptsWork,
                WireId = dto.WireId,
                UdpPort = dto.UdpPort,
            });
        }

        _members = next;
        _myWireId = next.FirstOrDefault(m => m.NodeId == LocalNodeId)?.WireId ?? 0;
        _lastKnownClusterSize = Math.Max(next.Count, 1);
        RaiseMembershipDiff(previous, next);
    }

    private void RaiseMembershipDiff(IReadOnlyList<ClusterMember> previous, IReadOnlyList<ClusterMember> next)
    {
        var previousIds = previous.Select(m => m.NodeId).ToHashSet();
        var nextIds = next.Select(m => m.NodeId).ToHashSet();
        var joined = nextIds.Except(previousIds).ToList();
        var left = previousIds.Except(nextIds).ToList();
        if (joined.Count > 0 || left.Count > 0)
        {
            Raise(MembershipChanged, new MembershipChangedEventArgs
            {
                Members = next,
                Joined = joined,
                Left = left,
            });
        }
    }
}
