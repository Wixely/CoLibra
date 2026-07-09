namespace CoLibra;

/// <summary>
/// The local node's view of a CoLibra cluster: decentralized discovery of peer services and
/// negotiation of exclusive, heartbeat-backed work leases.
/// </summary>
public interface ICoLibraCluster
{
    /// <summary>The local node's identity.</summary>
    NodeId LocalNodeId { get; }

    /// <summary>The local node's current lifecycle state.</summary>
    ClusterState State { get; }

    /// <summary>The currently known cluster members (including the local node).</summary>
    IReadOnlyList<ClusterMember> Members { get; }

    /// <summary>
    /// The single work-negotiation primitive. The first call for a key negotiates ownership with
    /// the coordinator (honoring <paramref name="preference"/>); the answer is cached locally, so
    /// subsequent calls are lock-free local reads that complete synchronously. Returns true only
    /// while this node exclusively owns the key and its renewals are being acknowledged — at no
    /// point (outside an accepted split brain) do two nodes get true for the same key.
    /// </summary>
    ValueTask<bool> CanProcessAsync(
        string type,
        string id,
        ProcessingPreference preference = ProcessingPreference.Balanced,
        CancellationToken cancellationToken = default);

    /// <summary>Explicitly gives up ownership of a key so another node can claim it.</summary>
    ValueTask ReleaseAsync(string type, string id);

    /// <summary>The keys this node currently owns.</summary>
    IReadOnlyCollection<LeaseKey> HeldLeases { get; }

    /// <summary>
    /// Advanced: acquires an explicit lease handle exposing the <see cref="FencingToken"/> and a
    /// <see cref="IExclusiveLease.Lost"/> cancellation token, for guarding writes to external systems.
    /// </summary>
    ValueTask<LeaseAcquisition> TryAcquireAsync(
        string type,
        string id,
        LeaseAcquireOptions? options = null,
        CancellationToken cancellationToken = default);

    /// <summary>Completes once the node has joined a cluster or become its coordinator.</summary>
    Task WaitForClusterAsync(CancellationToken cancellationToken = default);

    /// <summary>Raised when this node loses ownership of a lease.</summary>
    event EventHandler<LeaseLostEventArgs>? LeaseLost;

    /// <summary>Raised when a key this node was previously denied becomes available.</summary>
    event EventHandler<LeaseAvailableEventArgs>? LeaseAvailable;

    /// <summary>Raised when nodes join or leave.</summary>
    event EventHandler<MembershipChangedEventArgs>? MembershipChanged;

    /// <summary>Raised when the local node's state changes.</summary>
    event EventHandler<ClusterStateChangedEventArgs>? StateChanged;

    /// <summary>Raised when split brain is detected.</summary>
    event EventHandler<SplitBrainDetectedEventArgs>? SplitBrainDetected;
}
