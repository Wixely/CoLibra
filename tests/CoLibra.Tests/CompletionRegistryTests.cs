using CoLibra.Leasing;
using Microsoft.Extensions.Time.Testing;

namespace CoLibra.Tests;

public class CompletionRegistryTests
{
    private static CompletionRegistry Create(FakeTimeProvider time, int maxPerType = 100_000, TimeSpan? retention = null) =>
        new(new CompletionTrackingOptions { Enabled = true, MaxEntriesPerType = maxPerType, Retention = retention }, time);

    [Fact]
    public void Add_records_and_dedupes()
    {
        var time = new FakeTimeProvider();
        var registry = Create(time);
        var key = new LeaseKey("t", "1");

        Assert.False(registry.Contains(key));
        Assert.True(registry.Add(key, time.GetTimestamp()));
        Assert.True(registry.Contains(key));
        Assert.False(registry.Add(key, time.GetTimestamp()));
    }

    [Fact]
    public void AddRange_returns_only_the_new_entries()
    {
        var time = new FakeTimeProvider();
        var registry = Create(time);
        registry.Add(new LeaseKey("t", "1"), time.GetTimestamp());

        var added = registry.AddRange(
            [new LeaseKey("t", "1"), new LeaseKey("t", "2"), new LeaseKey("u", "1")],
            time.GetTimestamp());

        Assert.Equal([new LeaseKey("t", "2"), new LeaseKey("u", "1")], added);
    }

    [Fact]
    public void Snapshot_round_trips_all_types()
    {
        var time = new FakeTimeProvider();
        var registry = Create(time);
        registry.Add(new LeaseKey("t", "1"), time.GetTimestamp());
        registry.Add(new LeaseKey("u", "2"), time.GetTimestamp());

        Assert.Equal(
            [new LeaseKey("t", "1"), new LeaseKey("u", "2")],
            registry.Snapshot().OrderBy(k => k.Type).ToList());
    }

    [Fact]
    public void Capacity_evicts_oldest_first_per_type()
    {
        var time = new FakeTimeProvider();
        var registry = Create(time, maxPerType: 3);
        for (var i = 1; i <= 4; i++)
        {
            registry.Add(new LeaseKey("t", $"{i}"), time.GetTimestamp());
            time.Advance(TimeSpan.FromSeconds(1));
        }

        Assert.False(registry.Contains(new LeaseKey("t", "1"))); // oldest evicted
        Assert.True(registry.Contains(new LeaseKey("t", "2")));
        Assert.True(registry.Contains(new LeaseKey("t", "4")));
    }

    [Fact]
    public void Capacity_is_per_type()
    {
        var time = new FakeTimeProvider();
        var registry = Create(time, maxPerType: 2);
        registry.Add(new LeaseKey("t", "1"), time.GetTimestamp());
        registry.Add(new LeaseKey("t", "2"), time.GetTimestamp());
        registry.Add(new LeaseKey("u", "1"), time.GetTimestamp());
        registry.Add(new LeaseKey("u", "2"), time.GetTimestamp());

        Assert.True(registry.Contains(new LeaseKey("t", "1")));
        Assert.True(registry.Contains(new LeaseKey("u", "1")));
    }

    [Fact]
    public void Evicted_entries_can_be_completed_again()
    {
        var time = new FakeTimeProvider();
        var registry = Create(time, maxPerType: 2);
        registry.Add(new LeaseKey("t", "1"), time.GetTimestamp());
        registry.Add(new LeaseKey("t", "2"), time.GetTimestamp());
        registry.Add(new LeaseKey("t", "3"), time.GetTimestamp()); // evicts "1"

        Assert.True(registry.Add(new LeaseKey("t", "1"), time.GetTimestamp()));
        Assert.True(registry.Contains(new LeaseKey("t", "1")));
    }

    [Fact]
    public void Retention_trims_expired_entries_only()
    {
        var time = new FakeTimeProvider();
        var registry = Create(time, retention: TimeSpan.FromMinutes(10));
        registry.Add(new LeaseKey("t", "old"), time.GetTimestamp());
        time.Advance(TimeSpan.FromMinutes(6));
        registry.Add(new LeaseKey("t", "new"), time.GetTimestamp());
        time.Advance(TimeSpan.FromMinutes(5)); // "old" is 11m, "new" is 5m

        registry.TrimExpired(time.GetTimestamp());

        Assert.False(registry.Contains(new LeaseKey("t", "old")));
        Assert.True(registry.Contains(new LeaseKey("t", "new")));
    }

    [Fact]
    public void No_retention_means_trim_is_a_noop()
    {
        var time = new FakeTimeProvider();
        var registry = Create(time);
        registry.Add(new LeaseKey("t", "1"), time.GetTimestamp());
        time.Advance(TimeSpan.FromDays(365));

        registry.TrimExpired(time.GetTimestamp());

        Assert.True(registry.Contains(new LeaseKey("t", "1")));
    }
}
