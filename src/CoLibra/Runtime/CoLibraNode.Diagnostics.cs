namespace CoLibra.Runtime;

internal sealed partial class CoLibraNode
{
    public ValueTask<DiagnosticsSnapshot> GetDiagnosticsAsync(CancellationToken cancellationToken = default) =>
        new(PostWithResult<DiagnosticsSnapshot>(tcs =>
        {
            tcs.TrySetResult(BuildDiagnostics());
            return ValueTask.CompletedTask;
        }).WaitAsync(cancellationToken));

    // Built on the actor loop so every count is a consistent read of the same instant.
    private DiagnosticsSnapshot BuildDiagnostics()
    {
        var members = _members;
        var coordinator = members.FirstOrDefault(m => m.IsCoordinator);
        var completedByType = _completions?.CountsByType() ?? [];

        return new DiagnosticsSnapshot
        {
            LocalNodeId = LocalNodeId,
            NodeName = _options.NodeName,
            ServiceId = _options.ServiceId,
            ServiceVersion = _serviceVersion,
            State = _state,
            IsCoordinator = _state == ClusterState.Coordinator,
            IsAcceptingWork = _acceptWork,
            Incarnation = _incarnation,
            Term = _coordinator?.Term ?? _highestTerm,

            CoordinatorId = coordinator?.NodeId,
            CoordinatorName = coordinator?.Name,
            MemberCount = members.Count,
            Members = [.. members.Select(m => new MemberDiagnostics
            {
                NodeId = m.NodeId,
                Name = m.Name,
                IsCoordinator = m.IsCoordinator,
                IsSelf = m.NodeId == LocalNodeId,
                AcceptsWork = m.AcceptsWork,
                Weight = m.Weight,
                Endpoint = m.Endpoint,
                ServiceVersion = m.ServiceVersion,
                Incarnation = m.Incarnation,
                WireId = m.WireId,
            })],

            HeldLeaseCount = _held.Count,
            HeldLeasesByType = BuildTypeCounts(),
            PendingAcquireCount = _pendingAcquires.Count,
            DeniedDecisionCacheCount = _negativeCache.Count,
            CompletionTrackingEnabled = _completions is not null,
            CompletedCount = completedByType.Values.Sum(),
            CompletedByType = completedByType,

            AsCoordinator = _coordinator is { } coord
                ? new CoordinatorDiagnostics
                {
                    Term = coord.Term,
                    TrackedLeaseCount = coord.Table.LeaseCount,
                    SessionCount = coord.Sessions.Count,
                    NotAcceptingNodeCount = coord.Table.NotAcceptingCount,
                    LeasesByTypePerNode = coord.Table.CountsByType.ToDictionary(
                        kv => kv.Key,
                        kv => (IReadOnlyDictionary<string, int>)kv.Value.ToDictionary(
                            n => n.Key.ToString(), n => n.Value, StringComparer.Ordinal),
                        StringComparer.Ordinal),
                }
                : null,

            Transport = new TransportDiagnostics
            {
                MessagingEnabled = _options.Messaging.Enabled,
                RoutingEnabled = _options.Routing.Enabled,
                MessageHandlerCount = _messageHandlers.Count,
                RoutedHandlerCount = _routedHandlers.Count,
                ActiveUdpLinkCount = _udpLinks.Values.Count(l => l.Status == UdpLinkStatus.Active),
                DirectChannelCount = _directChannels.Count,
                OwnerCacheCount = _ownerCache.Count,
            },

            Configuration = new ConfigurationDiagnostics
            {
                HeartbeatInterval = _options.HeartbeatInterval,
                MemberTimeout = _options.MemberTimeout,
                ElectionTimeout = _options.ElectionTimeout,
                DiscoveryWindow = _options.DiscoveryWindow,
                AnnounceInterval = _options.AnnounceInterval,
                LeaseTtl = _options.LeaseTtl,
                LeaseIdleExpiry = _options.LeaseIdleExpiry,
                CoordinatorMode = _options.CoordinatorMode,
                QuorumPolicy = _options.QuorumPolicy,
                SplitBrainPolicy = _options.SplitBrainPolicy,
                DefaultLoadBalance = _options.DefaultLoadBalance,
                LoadBalanceTolerance = _options.LoadBalanceTolerance,
                DiscoveryPort = _options.DiscoveryPort,
                MulticastAddress = _options.MulticastAddress,
                EnableMulticast = _options.EnableMulticast,
                StaticSeedCount = _options.StaticSeeds.Count,
                PreferUdp = _options.Messaging.PreferUdp,
            },
        };
    }
}
