namespace CoLibra;

/// <summary>
/// Settings for the replicated "done" registry. When enabled, completion tombstones recorded
/// via <see cref="ICoLibraCluster.MarkCompletedAsync"/> are replicated to every member (within
/// roughly a heartbeat), so any single node dying does not cause survivors to recompute its
/// finished work. Completions are monotonic facts merged by union, which is why lazy
/// replication is safe: at worst the last ~second of unsynced completions is redone.
/// A full-cluster restart still starts from a clean slate (state is in-memory only).
/// </summary>
public sealed class CompletionTrackingOptions
{
    /// <summary>Enables the feature. Default false — disabled it costs nothing.</summary>
    public bool Enabled { get; set; }

    /// <summary>
    /// Hard per-type memory bound; when exceeded, the oldest completions are evicted first
    /// (an evicted id could in principle be recomputed — the registry reduces recomputation,
    /// it is not a database). Default 100,000.
    /// </summary>
    public int MaxEntriesPerType { get; set; } = 100_000;

    /// <summary>
    /// Optional age limit after which completions are forgotten. Null (default) keeps entries
    /// until capacity eviction. Must exceed <see cref="CoLibraOptions.LeaseTtl"/> when set.
    /// </summary>
    public TimeSpan? Retention { get; set; }
}
