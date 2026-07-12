using CoLibra.Runtime;

namespace CoLibra.IntegrationTests;

public class CompletionTrackingTests(ITestOutputHelper output) : IAsyncLifetime
{
    private readonly TestCluster _cluster = new(output);
    private static readonly Action<CoLibraOptions> WithCompletions = o => o.CompletionTracking.Enabled = true;

    public ValueTask InitializeAsync() => ValueTask.CompletedTask;

    public async ValueTask DisposeAsync() => await _cluster.DisposeAsync();

    [Fact]
    public async Task MarkCompleted_replicates_to_every_node()
    {
        var a = await _cluster.StartNodeAsync(WithCompletions);
        var b = await _cluster.StartNodeAsync(WithCompletions);
        var c = await _cluster.StartNodeAsync(WithCompletions);

        Assert.True(await a.CanProcessAsync("job", "1"));
        await a.MarkCompletedAsync("job", "1");

        await TestCluster.WaitUntilAsync(
            () => b.IsCompleted("job", "1") && c.IsCompleted("job", "1"),
            because: "the completion should replicate to every member");
        Assert.False(await b.CanProcessAsync("job", "1"));
        Assert.False(await c.CanProcessAsync("job", "1"));
        Assert.DoesNotContain(new LeaseKey("job", "1"), a.HeldLeases);
    }

    [Fact]
    public async Task Completion_by_a_non_owner_stops_the_owner_reprocessing()
    {
        var coordinator = await _cluster.StartNodeAsync(WithCompletions);
        var owner = await _cluster.StartNodeAsync(WithCompletions);
        var other = await _cluster.StartNodeAsync(WithCompletions);

        // owner (a member) exclusively holds K.
        Assert.True(await owner.CanProcessAsync("job", "K", ProcessingPreference.This));

        // A different node marks K completed even though it never owned it.
        await other.MarkCompletedAsync("job", "K");

        // The owner must stop reprocessing K and drop the now-finished lease, not keep answering true.
        await TestCluster.WaitUntilAsync(async () => !await owner.CanProcessAsync("job", "K"),
            because: "a key completed elsewhere must not be reprocessed by its owner");
        await TestCluster.WaitUntilAsync(() => !owner.HeldLeases.Contains(new LeaseKey("job", "K")),
            because: "the owner should drop a lease completed elsewhere");
    }

    [Fact]
    public async Task Completed_work_survives_its_owners_death()
    {
        var a = await _cluster.StartNodeAsync(WithCompletions);
        var b = await _cluster.StartNodeAsync(WithCompletions);
        var c = await _cluster.StartNodeAsync(WithCompletions);

        // b completes 10 keys and holds one in-flight key, then dies.
        for (var i = 0; i < 10; i++)
        {
            Assert.True(await b.CanProcessAsync("job", $"done_{i}", ProcessingPreference.This));
            await b.MarkCompletedAsync("job", $"done_{i}");
        }

        Assert.True(await b.CanProcessAsync("job", "inflight", ProcessingPreference.This));

        await TestCluster.WaitUntilAsync(() => a.IsCompleted("job", "done_9") && c.IsCompleted("job", "done_9"));
        await _cluster.StopNodeAsync(b);

        // The in-flight key becomes grantable again; the completed ones never do.
        await TestCluster.WaitUntilAsync(
            async () => await a.CanProcessAsync("job", "inflight", ProcessingPreference.This),
            because: "the dead node's in-flight key should be reclaimed");
        for (var i = 0; i < 10; i++)
        {
            Assert.False(await a.CanProcessAsync("job", $"done_{i}", ProcessingPreference.This));
            Assert.False(await c.CanProcessAsync("job", $"done_{i}", ProcessingPreference.This));
        }
    }

    [Fact]
    public async Task Registry_survives_coordinator_failover()
    {
        var coordinator = await _cluster.StartNodeAsync(WithCompletions);
        var m1 = await _cluster.StartNodeAsync(WithCompletions);
        var m2 = await _cluster.StartNodeAsync(WithCompletions);
        Assert.Equal(ClusterState.Coordinator, coordinator.State);

        Assert.True(await m1.CanProcessAsync("job", "x", ProcessingPreference.This));
        await m1.MarkCompletedAsync("job", "x");
        await TestCluster.WaitUntilAsync(() => m2.IsCompleted("job", "x"));

        await _cluster.StopNodeAsync(coordinator);
        await TestCluster.WaitUntilAsync(
            () => TestCluster.CoordinatorOf([m1, m2]) is not null &&
                  new[] { m1, m2 }.All(n => n.State is ClusterState.Coordinator or ClusterState.Member),
            because: "survivors should elect a new coordinator");

        Assert.False(await m1.CanProcessAsync("job", "x", ProcessingPreference.This));
        Assert.False(await m2.CanProcessAsync("job", "x", ProcessingPreference.This));
    }

    [Fact]
    public async Task Late_joiner_receives_the_full_registry()
    {
        var a = await _cluster.StartNodeAsync(WithCompletions);
        Assert.True(await a.CanProcessAsync("job", "early"));
        await a.MarkCompletedAsync("job", "early");

        var b = await _cluster.StartNodeAsync(WithCompletions);

        await TestCluster.WaitUntilAsync(
            () => b.IsCompleted("job", "early"),
            because: "the join-time snapshot should carry existing completions");
        Assert.False(await b.CanProcessAsync("job", "early"));
    }

    [Fact]
    public async Task Partition_heal_unions_both_sides()
    {
        var nodes = new CoLibraNode[5];
        for (var i = 0; i < 5; i++)
            nodes[i] = await _cluster.StartNodeAsync(WithCompletions);
        var minority = new[] { nodes[0], nodes[1] };
        var majority = new[] { nodes[2], nodes[3], nodes[4] };

        _cluster.Partition(minority, majority);
        await TestCluster.WaitUntilAsync(() => TestCluster.CoordinatorOf(majority) is not null);

        // Each side completes its own work while separated. The minority side is in
        // QuorumLost (DenyNewLeases by default denies acquisition, but completions are
        // monotonic facts and are always recorded).
        await minority[0].MarkCompletedAsync("job", "minority-side");
        Assert.True(await majority[0].CanProcessAsync("job", "majority-side", ProcessingPreference.This));
        await majority[0].MarkCompletedAsync("job", "majority-side");

        _cluster.Heal();
        await TestCluster.WaitUntilAsync(
            () => nodes.Count(n => n.State == ClusterState.Coordinator) == 1 &&
                  nodes.All(n => n.State is ClusterState.Coordinator or ClusterState.Member),
            timeout: TimeSpan.FromSeconds(25));

        await TestCluster.WaitUntilAsync(
            () => nodes.All(n => n.IsCompleted("job", "minority-side") && n.IsCompleted("job", "majority-side")),
            because: "healing should union both sides' completions everywhere");
    }

    [Fact]
    public async Task Stale_local_registry_is_backfilled_by_a_denied_acquire()
    {
        var a = await _cluster.StartNodeAsync(WithCompletions);
        var b = await _cluster.StartNodeAsync(WithCompletions);

        // b asks first so its denial-independent path is exercised: a completes, then b
        // (whose registry may not have synced yet) asks and must get denied Completed.
        Assert.True(await a.CanProcessAsync("job", "k", ProcessingPreference.This));
        await a.MarkCompletedAsync("job", "k");

        var acquisition = await b.TryAcquireAsync("job", "k");
        Assert.False(acquisition.Granted);
        Assert.Equal(LeaseDenialReason.Completed, acquisition.DenialReason);

        // The denial itself backfills b's local registry — no broadcast round-trip needed.
        Assert.True(b.IsCompleted("job", "k"));
        Assert.False(await b.CanProcessAsync("job", "k"));
    }

    [Fact]
    public async Task MarkCompleted_throws_when_the_feature_is_disabled()
    {
        var node = await _cluster.StartNodeAsync();
        await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await node.MarkCompletedAsync("job", "1"));
        Assert.False(node.IsCompleted("job", "1"));
    }

    [Fact]
    public async Task Completing_without_ever_holding_still_replicates()
    {
        var a = await _cluster.StartNodeAsync(WithCompletions);
        var b = await _cluster.StartNodeAsync(WithCompletions);

        await b.MarkCompletedAsync("job", "external"); // e.g. found already done in a durable store

        await TestCluster.WaitUntilAsync(() => a.IsCompleted("job", "external"));
        Assert.False(await a.CanProcessAsync("job", "external", ProcessingPreference.This));
    }
}
