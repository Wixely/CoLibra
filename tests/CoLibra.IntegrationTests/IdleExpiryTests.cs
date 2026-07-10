using System.Collections.Concurrent;
using CoLibra.Runtime;

namespace CoLibra.IntegrationTests;

public class IdleExpiryTests(ITestOutputHelper output) : IAsyncLifetime
{
    private readonly TestCluster _cluster = new(output);

    private static readonly TimeSpan ShortExpiry = TimeSpan.FromSeconds(3 * TestCluster.Scale);

    private static Action<CoLibraOptions> WithIdleExpiry(TimeSpan? expiry) => o => o.LeaseIdleExpiry = expiry;

    public ValueTask InitializeAsync() => ValueTask.CompletedTask;

    public async ValueTask DisposeAsync() => await _cluster.DisposeAsync();

    [Fact]
    public async Task Untouched_leases_idle_expire_and_become_acquirable_elsewhere()
    {
        var a = await _cluster.StartNodeAsync(WithIdleExpiry(ShortExpiry)); // coordinator
        var b = await _cluster.StartNodeAsync(WithIdleExpiry(ShortExpiry));
        var lost = new ConcurrentBag<(LeaseKey Key, LeaseLossReason Reason)>();
        b.LeaseLost += (_, e) => lost.Add((e.Key, e.Reason));

        Assert.True(await b.CanProcessAsync("job", "ephemeral", ProcessingPreference.This));
        // ...and the id is never seen again.

        await TestCluster.WaitUntilAsync(
            () => b.HeldLeases.Count == 0,
            timeout: ShortExpiry * 4,
            because: "the untouched lease should idle-expire off the owner");
        Assert.Contains(lost, l => l.Key == new LeaseKey("job", "ephemeral") && l.Reason == LeaseLossReason.IdleExpired);

        // The coordinator reclaims it (crash TTL after renewals stop) and others can take it.
        await TestCluster.WaitUntilAsync(
            async () => await a.CanProcessAsync("job", "ephemeral", ProcessingPreference.This),
            timeout: ShortExpiry * 4,
            because: "the freed key should become acquirable by another node");
    }

    [Fact]
    public async Task Regularly_checked_leases_survive_many_expiry_periods()
    {
        var a = await _cluster.StartNodeAsync(WithIdleExpiry(ShortExpiry));
        Assert.True(await a.CanProcessAsync("job", "warm", ProcessingPreference.This));

        // Keep checking well past several idle periods; the 50% rule must keep sliding it.
        var deadline = DateTime.UtcNow + ShortExpiry * 3;
        while (DateTime.UtcNow < deadline)
        {
            Assert.True(await a.CanProcessAsync("job", "warm"), "a regularly-checked lease must never idle-expire");
            await Task.Delay(200);
        }

        Assert.Contains(new LeaseKey("job", "warm"), a.HeldLeases);
    }

    [Fact]
    public async Task Per_type_null_never_expires_while_defaults_do()
    {
        var a = await _cluster.StartNodeAsync(o =>
        {
            o.LeaseIdleExpiry = ShortExpiry;
            o.PerTypeLeaseIdleExpiry["permanent"] = null;
        });
        Assert.True(await a.CanProcessAsync("permanent", "id", ProcessingPreference.This));
        Assert.True(await a.CanProcessAsync("temp", "id", ProcessingPreference.This));

        await TestCluster.WaitUntilAsync(
            () => !a.HeldLeases.Contains(new LeaseKey("temp", "id")),
            timeout: ShortExpiry * 4,
            because: "the default-expiry type should age out");
        Assert.Contains(new LeaseKey("permanent", "id"), a.HeldLeases);
    }

    [Fact]
    public async Task Null_global_expiry_disables_the_feature()
    {
        var a = await _cluster.StartNodeAsync(WithIdleExpiry(null));
        Assert.True(await a.CanProcessAsync("job", "forever", ProcessingPreference.This));

        await Task.Delay(ShortExpiry * 2);

        Assert.Contains(new LeaseKey("job", "forever"), a.HeldLeases);
    }

    [Fact]
    public async Task Explicit_handles_observe_idle_expiry_via_Lost()
    {
        var a = await _cluster.StartNodeAsync(WithIdleExpiry(ShortExpiry));
        var acquisition = await a.TryAcquireAsync("job", "handled");
        Assert.True(acquisition.Granted);
        await using var lease = acquisition.Lease!;

        await TestCluster.WaitUntilAsync(
            () => lease.Lost.IsCancellationRequested,
            timeout: ShortExpiry * 4,
            because: "an unchecked handle should idle-expire and fire Lost");
    }
}
