using System.Net;

namespace CoLibra;

/// <summary>
/// A point-in-time, read-only picture of what this node currently knows: its own identity and
/// lifecycle, the members it sees, who the coordinator is, how much work is in flight, and the
/// (non-secret) configuration it is running with. Produced by
/// <see cref="ICoLibraCluster.GetDiagnosticsAsync"/> for logging, health endpoints and dashboards.
/// The shared secret and any key material are deliberately never included.
/// </summary>
public sealed record DiagnosticsSnapshot
{
    // ---- local node + lifecycle ----

    /// <summary>This node's stable identity.</summary>
    public required NodeId LocalNodeId { get; init; }

    /// <summary>This node's application-defined name (<see cref="CoLibraOptions.NodeName"/>), if any.</summary>
    public required string? NodeName { get; init; }

    /// <summary>The service scope shared by every node in this cluster (<see cref="CoLibraOptions.ServiceId"/>).</summary>
    public required string ServiceId { get; init; }

    /// <summary>This node's advertised service version.</summary>
    public required Version ServiceVersion { get; init; }

    /// <summary>This node's current lifecycle state.</summary>
    public required ClusterState State { get; init; }

    /// <summary>True when this node is the current coordinator (the authoritative lease grantor).</summary>
    public required bool IsCoordinator { get; init; }

    /// <summary>Whether this node currently accepts work leases.</summary>
    public required bool IsAcceptingWork { get; init; }

    /// <summary>This node's incarnation (restart marker; higher means a more recent start).</summary>
    public required long Incarnation { get; init; }

    /// <summary>The coordinator term this node currently recognizes (monotonic; rises on each election).</summary>
    public required long Term { get; init; }

    // ---- cluster view ----

    /// <summary>The current coordinator's id, or null while one is being elected.</summary>
    public required NodeId? CoordinatorId { get; init; }

    /// <summary>The current coordinator's name, if it advertised one.</summary>
    public required string? CoordinatorName { get; init; }

    /// <summary>Number of members this node currently sees (including itself).</summary>
    public required int MemberCount { get; init; }

    /// <summary>Every member this node currently sees, including itself.</summary>
    public required IReadOnlyList<MemberDiagnostics> Members { get; init; }

    // ---- work / progress ----

    /// <summary>How many leases this node currently owns.</summary>
    public required int HeldLeaseCount { get; init; }

    /// <summary>Held-lease counts broken down by lease type.</summary>
    public required IReadOnlyDictionary<string, int> HeldLeasesByType { get; init; }

    /// <summary>Acquire requests this node has sent that are still awaiting a coordinator answer.</summary>
    public required int PendingAcquireCount { get; init; }

    /// <summary>Keys currently cached as "owned elsewhere" (the negative-decision fast path).</summary>
    public required int DeniedDecisionCacheCount { get; init; }

    /// <summary>Whether the replicated completion registry is enabled.</summary>
    public required bool CompletionTrackingEnabled { get; init; }

    /// <summary>Total completed ids known locally (0 when completion tracking is off).</summary>
    public required int CompletedCount { get; init; }

    /// <summary>Completed-id counts broken down by type.</summary>
    public required IReadOnlyDictionary<string, int> CompletedByType { get; init; }

    /// <summary>Coordinator-only view of the authoritative lease table; null unless this node is the coordinator.</summary>
    public required CoordinatorDiagnostics? AsCoordinator { get; init; }

    /// <summary>Messaging / routing / link statistics.</summary>
    public required TransportDiagnostics Transport { get; init; }

    /// <summary>The non-secret configuration this node is running with.</summary>
    public required ConfigurationDiagnostics Configuration { get; init; }
}

/// <summary>One member as seen by the local node.</summary>
public sealed record MemberDiagnostics
{
    /// <summary>The member's stable identity.</summary>
    public required NodeId NodeId { get; init; }

    /// <summary>The member's application-defined name, if any.</summary>
    public required string? Name { get; init; }

    /// <summary>True if this member is the current coordinator.</summary>
    public required bool IsCoordinator { get; init; }

    /// <summary>True if this member is the local node.</summary>
    public required bool IsSelf { get; init; }

    /// <summary>Whether this member accepts work leases.</summary>
    public required bool AcceptsWork { get; init; }

    /// <summary>The member's relative capacity for weighted load balancing.</summary>
    public required double Weight { get; init; }

    /// <summary>The member's TCP mesh endpoint.</summary>
    public required IPEndPoint Endpoint { get; init; }

    /// <summary>The member's advertised service version.</summary>
    public required Version ServiceVersion { get; init; }

    /// <summary>The member's incarnation (restart marker).</summary>
    public required long Incarnation { get; init; }

    /// <summary>The member's compact per-term wire id (0 = none assigned).</summary>
    public required int WireId { get; init; }
}

/// <summary>The coordinator's authoritative view; present only when the local node is the coordinator.</summary>
public sealed record CoordinatorDiagnostics
{
    /// <summary>The term this coordinator is serving.</summary>
    public required long Term { get; init; }

    /// <summary>Total leases the coordinator is tracking across the whole cluster.</summary>
    public required int TrackedLeaseCount { get; init; }

    /// <summary>Number of active member sessions the coordinator holds.</summary>
    public required int SessionCount { get; init; }

    /// <summary>Number of member nodes currently draining (not accepting new leases).</summary>
    public required int NotAcceptingNodeCount { get; init; }

    /// <summary>
    /// Live lease distribution: lease type → (node id, 8-char hex) → count. Lets you see how
    /// balanced the cluster is at a glance.
    /// </summary>
    public required IReadOnlyDictionary<string, IReadOnlyDictionary<string, int>> LeasesByTypePerNode { get; init; }
}

/// <summary>Messaging, routing and data-plane link statistics.</summary>
public sealed record TransportDiagnostics
{
    /// <summary>Whether direct node-to-node messaging is enabled.</summary>
    public required bool MessagingEnabled { get; init; }

    /// <summary>Whether routed delivery is enabled.</summary>
    public required bool RoutingEnabled { get; init; }

    /// <summary>Registered messaging channel handlers on this node.</summary>
    public required int MessageHandlerCount { get; init; }

    /// <summary>Registered routed-delivery handlers on this node.</summary>
    public required int RoutedHandlerCount { get; init; }

    /// <summary>Active UDP data-plane links to peers.</summary>
    public required int ActiveUdpLinkCount { get; init; }

    /// <summary>Pooled direct TCP channels currently open to peers.</summary>
    public required int DirectChannelCount { get; init; }

    /// <summary>Cached key→owner resolutions (routed delivery).</summary>
    public required int OwnerCacheCount { get; init; }
}

/// <summary>The subset of configuration that is safe to surface (no secrets or key material).</summary>
public sealed record ConfigurationDiagnostics
{
    /// <summary>Member heartbeat and lease-renewal cadence.</summary>
    public required TimeSpan HeartbeatInterval { get; init; }

    /// <summary>Silence threshold before a member is considered dead.</summary>
    public required TimeSpan MemberTimeout { get; init; }

    /// <summary>Election round wait.</summary>
    public required TimeSpan ElectionTimeout { get; init; }

    /// <summary>Startup listen window before claiming coordinatorship.</summary>
    public required TimeSpan DiscoveryWindow { get; init; }

    /// <summary>Coordinator announcement cadence.</summary>
    public required TimeSpan AnnounceInterval { get; init; }

    /// <summary>Lease crash-detection TTL (lost this long after renewals stop).</summary>
    public required TimeSpan LeaseTtl { get; init; }

    /// <summary>Idle lifetime of an untouched lease; null = never expire.</summary>
    public required TimeSpan? LeaseIdleExpiry { get; init; }

    /// <summary>This node's coordinator eligibility mode.</summary>
    public required CoordinatorMode CoordinatorMode { get; init; }

    /// <summary>The quorum rule applied to coordinator claims.</summary>
    public required QuorumPolicy QuorumPolicy { get; init; }

    /// <summary>Behavior during split brain / quorum loss.</summary>
    public required SplitBrainPolicy SplitBrainPolicy { get; init; }

    /// <summary>Default load-balancing strategy for types without an override.</summary>
    public required LoadBalanceType DefaultLoadBalance { get; init; }

    /// <summary>Maximum lease imbalance tolerated before grants steer away from a node.</summary>
    public required int LoadBalanceTolerance { get; init; }

    /// <summary>UDP discovery port.</summary>
    public required int DiscoveryPort { get; init; }

    /// <summary>IPv4 multicast group used for discovery.</summary>
    public required string MulticastAddress { get; init; }

    /// <summary>Whether multicast discovery is enabled.</summary>
    public required bool EnableMulticast { get; init; }

    /// <summary>Number of configured unicast static seeds.</summary>
    public required int StaticSeedCount { get; init; }

    /// <summary>Whether the UDP data plane is preferred for messaging.</summary>
    public required bool PreferUdp { get; init; }
}
