namespace CoLibra;

/// <summary>Raised when the local node loses ownership of a lease (expiry after missed renewals, or a conflict lost after a partition heal).</summary>
public sealed class LeaseLostEventArgs : EventArgs
{
    /// <summary>The lost key.</summary>
    public required LeaseKey Key { get; init; }

    /// <summary>Why the lease was lost.</summary>
    public required LeaseLossReason Reason { get; init; }
}

/// <summary>Why a held lease was lost.</summary>
public enum LeaseLossReason
{
    /// <summary>Renewals stopped being acknowledged; the local safety margin expired the lease.</summary>
    RenewalTimedOut = 0,

    /// <summary>After a partition heal, a conflicting owner held a higher fencing token.</summary>
    ConflictLost = 1,

    /// <summary>The coordinator reported the lease as owned by another node.</summary>
    OwnedElsewhere = 2,

    /// <summary>Revoked by <see cref="ICoLibraCluster.ForceRebalanceAsync"/> to shed excess load.</summary>
    Rebalanced = 3,

    /// <summary>
    /// The lease went unchecked past <see cref="CoLibraOptions.LeaseIdleExpiry"/> and aged out
    /// (ownership checks slide the expiry; ids never seen again are reclaimed).
    /// </summary>
    IdleExpired = 4,
}

/// <summary>Outcome of <see cref="ICoLibraCluster.ForceRebalanceAsync"/>.</summary>
/// <param name="WasCoordinator">False when called on a non-coordinator (the call did nothing).</param>
/// <param name="LeasesRevoked">How many leases were revoked to restore balance (0 when already balanced).</param>
/// <param name="NodesShed">How many nodes gave up at least one lease.</param>
public readonly record struct RebalanceResult(bool WasCoordinator, int LeasesRevoked, int NodesShed);

/// <summary>Raised when a key this node was previously denied becomes available again.</summary>
public sealed class LeaseAvailableEventArgs : EventArgs
{
    /// <summary>The keys that were released or expired by their previous owner.</summary>
    public required IReadOnlyList<LeaseKey> Keys { get; init; }
}

/// <summary>Raised when nodes join or leave the cluster.</summary>
public sealed class MembershipChangedEventArgs : EventArgs
{
    /// <summary>The full member list after the change.</summary>
    public required IReadOnlyList<ClusterMember> Members { get; init; }

    /// <summary>Nodes added since the previous list.</summary>
    public required IReadOnlyList<NodeId> Joined { get; init; }

    /// <summary>Nodes removed since the previous list.</summary>
    public required IReadOnlyList<NodeId> Left { get; init; }
}

/// <summary>Raised when the local node's <see cref="ClusterState"/> changes.</summary>
public sealed class ClusterStateChangedEventArgs : EventArgs
{
    /// <summary>The previous state.</summary>
    public required ClusterState Previous { get; init; }

    /// <summary>The new state.</summary>
    public required ClusterState Current { get; init; }
}

/// <summary>Raised when split brain is detected (a second coordinator seen, or quorum lost).</summary>
public sealed class SplitBrainDetectedEventArgs : EventArgs
{
    /// <summary>What triggered the detection.</summary>
    public required SplitBrainKind Kind { get; init; }

    /// <summary>Human-readable detail (e.g. the rival coordinator's id and term).</summary>
    public required string Detail { get; init; }
}

/// <summary>The condition that triggered split-brain detection.</summary>
public enum SplitBrainKind
{
    /// <summary>Two coordinators discovered each other (partitions merging).</summary>
    RivalCoordinator = 0,

    /// <summary>Fewer than a quorum of the last-known cluster is reachable.</summary>
    QuorumLost = 1,
}

/// <summary>Thrown by cluster operations when <see cref="SplitBrainPolicy.ThrowOnAllOperations"/> is active and split brain / quorum loss is detected.</summary>
public sealed class SplitBrainException : InvalidOperationException
{
    /// <summary>Creates the exception.</summary>
    public SplitBrainException(string message) : base(message)
    {
    }
}
