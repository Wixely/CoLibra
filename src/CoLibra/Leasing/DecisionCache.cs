using System.Collections.Concurrent;

namespace CoLibra.Leasing;

/// <summary>
/// Local cache of negative CanProcess answers (key is owned elsewhere / steered away).
/// Kept fresh primarily by coordinator push-invalidation; the TTL is a backstop for lost
/// notifications and the whole cache is flushed on coordinator change. Reads are lock-free.
/// A stale entry can only produce a stale "false" (a missed work item), never a stale "true".
/// </summary>
internal sealed class DecisionCache(bool enabled, TimeSpan ttl, int maxEntries, TimeProvider timeProvider)
{
    private readonly ConcurrentDictionary<LeaseKey, long> _deniedUntil = new();

    public bool IsDenied(LeaseKey key)
    {
        if (!enabled || !_deniedUntil.TryGetValue(key, out var expiresAt))
            return false;
        if (timeProvider.GetTimestamp() < expiresAt)
            return true;
        _deniedUntil.TryRemove(key, out _);
        return false;
    }

    public void Deny(LeaseKey key)
    {
        if (!enabled)
            return;
        var now = timeProvider.GetTimestamp();
        _deniedUntil[key] = now + (long)(ttl.TotalSeconds * timeProvider.TimestampFrequency);
        if (_deniedUntil.Count > maxEntries)
            Trim();
    }

    /// <summary>Evicts the given keys; returns the ones that were actually cached (worth raising LeaseAvailable for).</summary>
    public List<LeaseKey> Invalidate(IEnumerable<LeaseKey> keys)
    {
        var evicted = new List<LeaseKey>();
        foreach (var key in keys)
        {
            if (_deniedUntil.TryRemove(key, out _))
                evicted.Add(key);
        }

        return evicted;
    }

    public void Clear() => _deniedUntil.Clear();

    private void Trim()
    {
        // Rare path (cache overfull): drop the entries closest to expiry.
        foreach (var (key, _) in _deniedUntil.OrderBy(kv => kv.Value).Take(_deniedUntil.Count - maxEntries + maxEntries / 10))
            _deniedUntil.TryRemove(key, out _);
    }
}
