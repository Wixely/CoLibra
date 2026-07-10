namespace CoLibra;

/// <summary>
/// The instance's suggestion to the coordinator when asking
/// <see cref="ICoLibraCluster.CanProcessAsync"/> about a key it does not yet own.
/// </summary>
public enum ProcessingPreference
{
    /// <summary>The coordinator decides per the load-balance policy configured for the key's type.</summary>
    Balanced = 0,

    /// <summary>
    /// This instance wants the work. The grant bypasses load-balance steering and is given
    /// unless another node already owns the key.
    /// </summary>
    This = 1,

    /// <summary>
    /// This instance would rather another node take the work. The coordinator grants it here
    /// only if no other node claims the key within <see cref="CoLibraOptions.OtherPreferenceGraceWindow"/>
    /// (or immediately when this is the only node), so work is never orphaned.
    /// </summary>
    Other = 2,
}

/// <summary>How new grants are steered across nodes for a given lease type.</summary>
public enum LoadBalanceType
{
    /// <summary>First come, first served. No steering.</summary>
    None = 0,

    /// <summary>
    /// Grants steer toward the node currently holding the fewest leases of the type
    /// (the "Equal" strategy). Existing leases are never revoked.
    /// </summary>
    LeastLeases = 1,

    /// <summary>Like <see cref="LeastLeases"/>, but counts are normalized by each node's configured weight.</summary>
    Weighted = 2,
}

/// <summary>What the node does with lease operations while split brain / quorum loss is detected.</summary>
public enum SplitBrainPolicy
{
    /// <summary>Keep operating normally; the condition is only logged and raised as an event.</summary>
    Continue = 0,

    /// <summary>Existing held leases keep renewing, but new acquisitions are denied until the condition clears.</summary>
    DenyNewLeases = 1,

    /// <summary>All lease operations throw <see cref="SplitBrainException"/> until the condition clears.</summary>
    ThrowOnAllOperations = 2,
}

/// <summary>
/// This node's relationship to coordinatorship, for asymmetric architectures (e.g. a game
/// server that must be the authority while its peers never lead).
/// </summary>
public enum CoordinatorMode
{
    /// <summary>Participates normally in elections (default).</summary>
    Eligible = 0,

    /// <summary>
    /// Forces this node to be the coordinator: it never joins a non-forced coordinator, claims
    /// coordinatorship with a superseding term at startup and after any loss, and escalates
    /// over (never yields to) non-forced rivals. Forced claims bypass the quorum gate — the
    /// node is the authority even alone. If several Forced nodes meet, they settle among
    /// themselves by the normal term/node-id rules and the losers join the winner.
    /// </summary>
    Forced = 1,

    /// <summary>
    /// Never claims coordinatorship. The node discovers, joins and works as a member only; if
    /// no eligible coordinator exists it waits, retrying, until one appears. A cluster of only
    /// Never nodes will never form — deploy at least one Eligible or Forced node.
    /// </summary>
    Never = 2,
}

/// <summary>Quorum rule used when claiming coordinatorship.</summary>
public enum QuorumPolicy
{
    /// <summary>
    /// A coordinator claimant must see a majority of the last-known cluster.
    /// Clusters of 1 or 2 nodes use quorum 1 (majority of 2 would deadlock on any partition).
    /// </summary>
    Majority = 0,

    /// <summary>No quorum check; any node may claim coordinatorship when none is reachable.</summary>
    Off = 1,
}

/// <summary>Lifecycle state of the local node within the cluster.</summary>
public enum ClusterState
{
    /// <summary>Not started yet.</summary>
    Starting = 0,

    /// <summary>Searching for an existing coordinator on the network.</summary>
    Discovering = 1,

    /// <summary>Connecting/handshaking with a discovered coordinator.</summary>
    Joining = 2,

    /// <summary>Joined; a remote coordinator is granting leases.</summary>
    Member = 3,

    /// <summary>This node is the coordinator.</summary>
    Coordinator = 4,

    /// <summary>The coordinator was lost; an election is in progress.</summary>
    Electing = 5,

    /// <summary>Fewer than a quorum of nodes are reachable; <see cref="SplitBrainPolicy"/> applies.</summary>
    QuorumLost = 6,

    /// <summary>Unrecoverable local fault (e.g. duplicate NodeId rejected by the cluster).</summary>
    Faulted = 7,

    /// <summary>The node has been stopped.</summary>
    Stopped = 8,
}

/// <summary>Why an acquisition was denied.</summary>
public enum LeaseDenialReason
{
    /// <summary>Not denied.</summary>
    None = 0,

    /// <summary>Another node currently owns the key.</summary>
    HeldByOther = 1,

    /// <summary>The load-balance policy steered the grant away from this node.</summary>
    Rebalance = 2,

    /// <summary>Denied by <see cref="SplitBrainPolicy.DenyNewLeases"/> during split brain / quorum loss.</summary>
    SplitBrain = 3,

    /// <summary>No coordinator is currently reachable and the request could not be queued.</summary>
    NoCoordinator = 4,

    /// <summary>The <see cref="ProcessingPreference.Other"/> grace window ended with another node taking the key.</summary>
    PreferredElsewhere = 5,

    /// <summary>
    /// The key was marked completed via <see cref="ICoLibraCluster.MarkCompletedAsync"/>;
    /// completed keys are never granted again (while the tombstone is retained).
    /// </summary>
    Completed = 6,

    /// <summary>
    /// This node is not accepting work (<see cref="CoLibraOptions.AcceptWork"/> /
    /// <see cref="ICoLibraCluster.SetAcceptingWorkAsync"/>).
    /// </summary>
    NotAcceptingWork = 7,
}
