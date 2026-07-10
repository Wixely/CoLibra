using System.Reflection;

namespace CoLibra;

/// <summary>Configuration for a CoLibra node. <see cref="ServiceId"/> and <see cref="SharedSecret"/> are required.</summary>
public sealed class CoLibraOptions
{
    /// <summary>
    /// Identifies the kind of service this node belongs to. Discovery is scoped to it: nodes
    /// of a different service on the same network (or the same machine) are invisible to each other.
    /// </summary>
    public string ServiceId { get; set; } = string.Empty;

    /// <summary>
    /// Cluster secret. Discovery packets are HMAC-signed with it and mesh connections are
    /// mutually authenticated by it, so only processes holding the secret can join or issue commands.
    /// </summary>
    public string SharedSecret { get; set; } = string.Empty;

    /// <summary>The host application's version, carried in announces and checked per <see cref="VersionCompatibility"/>. Defaults to the entry assembly's version.</summary>
    public Version? ServiceVersion { get; set; }

    /// <summary>Rule for which peer service versions may share the cluster. Defaults to <see cref="VersionCompatibility.MajorMatch"/>.</summary>
    public VersionCompatibility VersionCompatibility { get; set; } = VersionCompatibility.MajorMatch;

    /// <summary>Fixed node identity; leave null for an auto-generated GUID v7 per process.</summary>
    public Guid? NodeId { get; set; }

    /// <summary>
    /// Optional application-defined name for this node — a machine id, a username, anything.
    /// Advertised to all members (visible on <see cref="ClusterMember.Name"/>) and addressable
    /// via <see cref="ICoLibraMessenger.SendByNameAsync"/>. Need not be unique: name-addressed
    /// messages are delivered to every node bearing the name.
    /// </summary>
    public string? NodeName { get; set; }

    /// <summary>UDP port for discovery. All CoLibra services on a machine can share it (bound with ReuseAddress). Default 41100.</summary>
    public int DiscoveryPort { get; set; } = 41100;

    /// <summary>TCP port for the node-to-node mesh. 0 (default) = OS-assigned; the actual port is advertised in announces.</summary>
    public int MeshPort { get; set; }

    /// <summary>IPv4 multicast group used for discovery. Default 239.255.41.10.</summary>
    public string MulticastAddress { get; set; } = "239.255.41.10";

    /// <summary>Enables multicast discovery. Default true.</summary>
    public bool EnableMulticast { get; set; } = true;

    /// <summary>Also announce via subnet broadcast, for networks where multicast is filtered. Default false.</summary>
    public bool EnableBroadcastFallback { get; set; }

    /// <summary>
    /// Static "host:port" (UDP discovery port) seeds probed by unicast, for WAN clusters or
    /// networks without multicast (Docker, Kubernetes, cloud VNets). Additive to multicast.
    /// DNS names are re-resolved on every probe.
    /// </summary>
    public List<string> StaticSeeds { get; } = [];

    /// <summary>How often the coordinator announces itself over UDP. Default 2 s.</summary>
    public TimeSpan AnnounceInterval { get; set; } = TimeSpan.FromSeconds(2);

    /// <summary>How often members heartbeat the coordinator over TCP (piggybacks lease renewals). Default 1 s.</summary>
    public TimeSpan HeartbeatInterval { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>Silence after which a node is declared dead. Default 5 s.</summary>
    public TimeSpan MemberTimeout { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>How long an election round waits for higher nodes to respond. Default 3 s.</summary>
    public TimeSpan ElectionTimeout { get; set; } = TimeSpan.FromSeconds(3);

    /// <summary>Window after a coordinator change during which members re-assert held leases before new grants resume. Default 2 s.</summary>
    public TimeSpan RebuildWindow { get; set; } = TimeSpan.FromSeconds(2);

    /// <summary>How long a starting node listens for an existing coordinator before claiming coordinatorship itself. Default 3 s.</summary>
    public TimeSpan DiscoveryWindow { get; set; } = TimeSpan.FromSeconds(3);

    /// <summary>Lease time-to-live; an owner that stops renewing loses the key after at most this long. Default 15 s.</summary>
    public TimeSpan LeaseTtl { get; set; } = TimeSpan.FromSeconds(15);

    /// <summary>
    /// Idle lifetime of an UNTOUCHED lease on a healthy node (distinct from <see cref="LeaseTtl"/>,
    /// which is crash detection). Ownership checks (<see cref="ICoLibraCluster.CanProcessAsync"/>,
    /// handle reads, routed deliveries) slide the expiry forward once it falls below 50%, so
    /// keys in active use live forever while ids that are never seen again age out — bounding
    /// memory and heartbeat size. The owner raises <see cref="LeaseLossReason.IdleExpired"/> and
    /// other nodes learn within one <see cref="LeaseTtl"/>. Null = never expire. Default 24 h.
    /// </summary>
    public TimeSpan? LeaseIdleExpiry { get; set; } = TimeSpan.FromHours(24);

    /// <summary>
    /// Per-lease-type overrides of <see cref="LeaseIdleExpiry"/>. A type mapped to null never
    /// expires — for permanent ids (e.g. a fixed set of source ids owned for the process's life).
    /// </summary>
    public Dictionary<string, TimeSpan?> PerTypeLeaseIdleExpiry { get; } = new(StringComparer.Ordinal);

    /// <summary>
    /// Local safety margin: if renewals go unacknowledged for LeaseTtl minus this margin,
    /// <see cref="ICoLibraCluster.CanProcessAsync"/> flips to false locally before the coordinator
    /// could re-grant the key elsewhere. Default 3 s.
    /// </summary>
    public TimeSpan LeaseRenewSafetyMargin { get; set; } = TimeSpan.FromSeconds(3);

    /// <summary>Behavior while split brain / quorum loss is detected. Default <see cref="CoLibra.SplitBrainPolicy.DenyNewLeases"/>.</summary>
    public SplitBrainPolicy SplitBrainPolicy { get; set; } = SplitBrainPolicy.DenyNewLeases;

    /// <summary>Quorum rule for coordinator claims. Default <see cref="CoLibra.QuorumPolicy.Majority"/>.</summary>
    public QuorumPolicy QuorumPolicy { get; set; } = QuorumPolicy.Majority;

    /// <summary>
    /// This node's coordinatorship stance: <see cref="CoordinatorMode.Eligible"/> (default),
    /// <see cref="CoordinatorMode.Forced"/> (this node IS the coordinator — asymmetric
    /// architectures like game servers), or <see cref="CoordinatorMode.Never"/> (member only).
    /// </summary>
    public CoordinatorMode CoordinatorMode { get; set; } = CoordinatorMode.Eligible;

    /// <summary>
    /// Whether this node accepts work leases (default true). When false the node never
    /// acquires or gets assigned work — it is excluded from load-balance math and from routed
    /// forced assignment, and its own lease requests are denied locally — while everything
    /// else (membership, messaging, routing payloads out, coordinatorship) works normally.
    /// Typical for authority nodes: a game server is <see cref="CoordinatorMode.Forced"/> and
    /// AcceptWork = false. Toggle at runtime with <see cref="ICoLibraCluster.SetAcceptingWorkAsync"/>;
    /// turning it off never revokes leases already held.
    /// </summary>
    public bool AcceptWork { get; set; } = true;

    /// <summary>Caches negative CanProcess answers locally (push-invalidated by the coordinator). Default true.</summary>
    public bool EnableDecisionCache { get; set; } = true;

    /// <summary>Backstop TTL on cached negative answers, in case an invalidation is lost. Default 30 s.</summary>
    public TimeSpan DecisionCacheTtl { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>Maximum cached negative answers before the oldest are evicted. Default 10000.</summary>
    public int DecisionCacheMaxEntries { get; set; } = 10_000;

    /// <summary>How long the coordinator holds a <see cref="ProcessingPreference.Other"/> request open for another node to claim the key. Default 5 s.</summary>
    public TimeSpan OtherPreferenceGraceWindow { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Where the auto-generated self-signed TLS certificate is persisted. Defaults to
    /// "colibra\{ServiceId}.pfx" under the application's base directory, so services on the
    /// same machine never share certificates.
    /// </summary>
    public string? CertificatePath { get; set; }

    /// <summary>Load-balance strategy for types without an entry in <see cref="PerTypeLoadBalance"/>. Default <see cref="LoadBalanceType.LeastLeases"/>.</summary>
    public LoadBalanceType DefaultLoadBalance { get; set; } = LoadBalanceType.LeastLeases;

    /// <summary>Per-lease-type load-balance strategy overrides.</summary>
    public Dictionary<string, LoadBalanceType> PerTypeLoadBalance { get; } = new(StringComparer.Ordinal);

    /// <summary>How many more leases of a type a node may hold than the least-loaded node before grants are steered away. Default 1.</summary>
    public int LoadBalanceTolerance { get; set; } = 1;

    /// <summary>This node's relative capacity for <see cref="LoadBalanceType.Weighted"/>. Default 1.0.</summary>
    public double Weight { get; set; } = 1.0;

    /// <summary>
    /// Opt-in replicated completion tracking: <see cref="ICoLibraCluster.MarkCompletedAsync"/>
    /// records that a key is finished forever, and the fact is replicated to every node so a
    /// single node's death does not cause its finished work to be recomputed. See
    /// <see cref="CompletionTrackingOptions"/> for semantics and bounds.
    /// </summary>
    public CompletionTrackingOptions CompletionTracking { get; } = new();

    /// <summary>
    /// Opt-in routed delivery for single-receiver topologies: any node can receive data and
    /// <see cref="ICoLibraCluster.Router"/> delivers it to the key's lease owner, force-assigning
    /// an owner when the key is free. See <see cref="RoutingOptions"/>.
    /// </summary>
    public RoutingOptions Routing { get; } = new();

    /// <summary>
    /// Opt-in direct node-to-node messaging (<see cref="ICoLibraCluster.Messenger"/>): send
    /// payloads to a specific node by id or by <see cref="NodeName"/>. See <see cref="MessagingOptions"/>.
    /// </summary>
    public MessagingOptions Messaging { get; } = new();

    internal Version ResolveServiceVersion() =>
        ServiceVersion
        ?? Assembly.GetEntryAssembly()?.GetName().Version
        ?? new Version(0, 0);

    internal string ResolveCertificatePath() =>
        CertificatePath ?? Path.Combine(AppContext.BaseDirectory, "colibra", $"{ServiceId}.pfx");
}
