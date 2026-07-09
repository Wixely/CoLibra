namespace CoLibra.Leasing;

/// <summary>
/// The local copy of the cluster's replicated "done" set: per-type grow-only sets of completed
/// ids, bounded by FIFO capacity eviction and optional age retention. Every node (coordinator
/// and members alike) holds a full copy and merges peer snapshots by plain union — completions
/// are monotonic facts, so no conflict resolution is needed. Thread-safe: read from the
/// CanProcess fast path on any thread, written from the actor loop and sync handlers.
/// </summary>
internal sealed class CompletionRegistry(CompletionTrackingOptions options, TimeProvider time)
{
    private sealed class TypeSet
    {
        public readonly HashSet<string> Ids = new(StringComparer.Ordinal);
        public readonly Queue<(string Id, long AddedTs)> Order = new();
    }

    private readonly Dictionary<string, TypeSet> _byType = new(StringComparer.Ordinal);
    private readonly Lock _lock = new();
    private readonly long _retentionTicks = options.Retention is { } retention
        ? (long)(retention.TotalSeconds * time.TimestampFrequency)
        : 0;

    public bool Contains(LeaseKey key)
    {
        lock (_lock)
        {
            return _byType.TryGetValue(key.Type, out var set) && set.Ids.Contains(key.Id);
        }
    }

    /// <summary>Records a completion; returns false when it was already known.</summary>
    public bool Add(LeaseKey key, long nowTs)
    {
        lock (_lock)
        {
            return AddCore(key, nowTs);
        }
    }

    /// <summary>Union-merges a peer's entries; returns only the ones that were new locally.</summary>
    public List<LeaseKey> AddRange(IEnumerable<LeaseKey> keys, long nowTs)
    {
        var added = new List<LeaseKey>();
        lock (_lock)
        {
            foreach (var key in keys)
            {
                if (AddCore(key, nowTs))
                    added.Add(key);
            }
        }

        return added;
    }

    public List<LeaseKey> Snapshot()
    {
        lock (_lock)
        {
            var all = new List<LeaseKey>();
            foreach (var (type, set) in _byType)
                all.AddRange(set.Ids.Select(id => new LeaseKey(type, id)));
            return all;
        }
    }

    /// <summary>Drops entries older than the retention window (no-op when retention is off).</summary>
    public void TrimExpired(long nowTs)
    {
        if (_retentionTicks <= 0)
            return;

        lock (_lock)
        {
            foreach (var (type, set) in _byType.ToList())
            {
                while (set.Order.TryPeek(out var oldest) && nowTs - oldest.AddedTs >= _retentionTicks)
                {
                    set.Order.Dequeue();
                    set.Ids.Remove(oldest.Id);
                }

                if (set.Ids.Count == 0)
                    _byType.Remove(type);
            }
        }
    }

    private bool AddCore(LeaseKey key, long nowTs)
    {
        if (!_byType.TryGetValue(key.Type, out var set))
            _byType[key.Type] = set = new TypeSet();

        if (!set.Ids.Add(key.Id))
            return false;

        set.Order.Enqueue((key.Id, nowTs));
        while (set.Ids.Count > options.MaxEntriesPerType && set.Order.TryDequeue(out var oldest))
            set.Ids.Remove(oldest.Id);

        return true;
    }
}
