using CoLibra.Leasing;
using Microsoft.Extensions.Time.Testing;

namespace CoLibra.Tests;

public class DecisionCacheTests
{
    private static readonly LeaseKey Key = new("t", "1");

    [Fact]
    public void Caches_denials_until_invalidated()
    {
        var time = new FakeTimeProvider();
        var cache = new DecisionCache(enabled: true, TimeSpan.FromSeconds(30), 100, time);

        Assert.False(cache.IsDenied(Key));
        cache.Deny(Key);
        Assert.True(cache.IsDenied(Key));

        var evicted = cache.Invalidate([Key, new LeaseKey("t", "2")]);
        Assert.Equal([Key], evicted);
        Assert.False(cache.IsDenied(Key));
    }

    [Fact]
    public void Entries_expire_after_the_backstop_ttl()
    {
        var time = new FakeTimeProvider();
        var cache = new DecisionCache(enabled: true, TimeSpan.FromSeconds(30), 100, time);
        cache.Deny(Key);

        time.Advance(TimeSpan.FromSeconds(29));
        Assert.True(cache.IsDenied(Key));
        time.Advance(TimeSpan.FromSeconds(2));
        Assert.False(cache.IsDenied(Key));
    }

    [Fact]
    public void Clear_flushes_everything()
    {
        var cache = new DecisionCache(enabled: true, TimeSpan.FromSeconds(30), 100, new FakeTimeProvider());
        cache.Deny(Key);
        cache.Clear();
        Assert.False(cache.IsDenied(Key));
    }

    [Fact]
    public void Disabled_cache_never_reports_denials()
    {
        var cache = new DecisionCache(enabled: false, TimeSpan.FromSeconds(30), 100, new FakeTimeProvider());
        cache.Deny(Key);
        Assert.False(cache.IsDenied(Key));
    }

    [Fact]
    public void Trims_entries_closest_to_expiry_when_full()
    {
        var time = new FakeTimeProvider();
        var cache = new DecisionCache(enabled: true, TimeSpan.FromSeconds(30), maxEntries: 10, time);
        for (var i = 0; i < 10; i++)
        {
            cache.Deny(new LeaseKey("t", $"{i}"));
            time.Advance(TimeSpan.FromSeconds(1));
        }

        cache.Deny(new LeaseKey("t", "overflow"));
        Assert.True(cache.IsDenied(new LeaseKey("t", "overflow")));
        Assert.False(cache.IsDenied(new LeaseKey("t", "0"))); // oldest was trimmed
    }
}
