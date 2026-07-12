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

    /// <summary>
    /// Records that this key is finished forever and releases its lease if held. The completion
    /// is replicated to every member (within roughly a heartbeat), so the finished work is not
    /// recomputed even if this node dies — <see cref="CanProcessAsync"/> returns false for
    /// completed keys on every node. Requires <see cref="CompletionTrackingOptions.Enabled"/>;
    /// throws <see cref="InvalidOperationException"/> otherwise. Completions recorded while
    /// disconnected are re-synced when the node rejoins.
    /// </summary>
    ValueTask MarkCompletedAsync(string type, string id, CancellationToken cancellationToken = default);

    /// <summary>
    /// Lock-free local read: whether the key is known to be completed. Always false when
    /// <see cref="CompletionTrackingOptions.Enabled"/> is off. The local registry converges
    /// within about a heartbeat of a completion anywhere in the cluster.
    /// </summary>
    bool IsCompleted(string type, string id);

    /// <summary>
    /// Routed delivery (requires <see cref="RoutingOptions.Enabled"/>; members throw
    /// <see cref="InvalidOperationException"/> otherwise). See <see cref="ICoLibraRouter"/>.
    /// </summary>
    ICoLibraRouter Router { get; }

    /// <summary>
    /// Direct node-to-node messaging (requires <see cref="MessagingOptions.Enabled"/>; members
    /// throw <see cref="InvalidOperationException"/> otherwise). See <see cref="ICoLibraMessenger"/>.
    /// </summary>
    ICoLibraMessenger Messenger { get; }

    /// <summary>Whether this node currently accepts work leases (see <see cref="CoLibraOptions.AcceptWork"/>).</summary>
    bool IsAcceptingWork { get; }

    /// <summary>
    /// Coordinator-only: re-evaluates every node's load and revokes the minimum set of leases
    /// needed to bring the cluster within tolerance (nodes at or below the mean are untouched;
    /// overloaded nodes shed only their excess, newest grants first; non-accepting nodes shed
    /// everything — the drain pattern). Freed keys redistribute through normal steering.
    /// Safe to call on ANY node: non-coordinators silently do nothing and return
    /// <c>WasCoordinator = false</c>. <paramref name="type"/> limits the pass to one lease type;
    /// null covers every type whose balance policy isn't <see cref="LoadBalanceType.None"/>.
    /// </summary>
    ValueTask<RebalanceResult> ForceRebalanceAsync(string? type = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Turns work acceptance on or off at runtime (advertised to the coordinator on the next
    /// heartbeat). Turning it off denies new acquisitions and excludes this node from balancing
    /// and forced assignment; leases already held are never revoked and keep renewing.
    /// </summary>
    ValueTask SetAcceptingWorkAsync(bool accept, CancellationToken cancellationToken = default);

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

    /// <summary>
    /// Captures a consistent, read-only <see cref="DiagnosticsSnapshot"/> of what this node
    /// currently knows: its identity and lifecycle state, the members it sees, who the coordinator
    /// is, work in flight (held leases, completions, pending acquires), coordinator-side lease
    /// distribution when this node is the coordinator, transport/link stats, and the non-secret
    /// configuration it is running with. The snapshot is read on the internal actor so all counts
    /// are mutually consistent; the shared secret and key material are never included. Intended for
    /// logging, health endpoints and dashboards — cheap enough to call periodically.
    /// </summary>
    ValueTask<DiagnosticsSnapshot> GetDiagnosticsAsync(CancellationToken cancellationToken = default);

    // NOTE: every event below is raised SYNCHRONOUSLY on CoLibra's internal single-threaded actor.
    // A handler that blocks, or that waits on any CoLibra call (e.g. `CanProcessAsync(...).Result`,
    // `ReleaseAsync(...).GetAwaiter().GetResult()`), DEADLOCKS the node — that call needs the same
    // actor the handler is occupying. A merely slow handler stalls heartbeats and can get the node
    // timed out of the cluster. Keep handlers fast and non-blocking; hand real work to your own
    // task/queue and return immediately. Exceptions thrown from a handler are caught and logged.

    /// <summary>Raised when this node loses ownership of a lease. Runs on the actor thread — see the note above.</summary>
    event EventHandler<LeaseLostEventArgs>? LeaseLost;

    /// <summary>Raised when a key this node was previously denied becomes available. Runs on the actor thread — see the note above.</summary>
    event EventHandler<LeaseAvailableEventArgs>? LeaseAvailable;

    /// <summary>Raised when nodes join or leave. Runs on the actor thread — see the note above.</summary>
    event EventHandler<MembershipChangedEventArgs>? MembershipChanged;

    /// <summary>Raised when the local node's state changes. Runs on the actor thread — see the note above.</summary>
    event EventHandler<ClusterStateChangedEventArgs>? StateChanged;

    /// <summary>Raised when split brain is detected. Runs on the actor thread — see the note above.</summary>
    event EventHandler<SplitBrainDetectedEventArgs>? SplitBrainDetected;
}
