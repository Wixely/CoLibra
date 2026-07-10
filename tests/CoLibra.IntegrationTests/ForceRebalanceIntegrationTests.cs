using System.Collections.Concurrent;
using CoLibra.Runtime;

namespace CoLibra.IntegrationTests;

public class ForceRebalanceIntegrationTests(ITestOutputHelper output) : IAsyncLifetime
{
    private readonly TestCluster _cluster = new(output);

    public ValueTask InitializeAsync() => ValueTask.CompletedTask;

    public async ValueTask DisposeAsync() => await _cluster.DisposeAsync();

    [Fact]
    public async Task Rebalances_an_imbalanced_cluster_with_minimal_movement()
    {
        var a = await _cluster.StartNodeAsync(); // coordinator; grabs everything before b exists
        for (var i = 0; i < 10; i++)
            Assert.True(await a.CanProcessAsync("job", $"j{i}", ProcessingPreference.This));
        var b = await _cluster.StartNodeAsync();

        var lost = new ConcurrentBag<LeaseKey>();
        a.LeaseLost += (_, e) =>
        {
            if (e.Reason == LeaseLossReason.Rebalanced)
                lost.Add(e.Key);
        };

        var result = await a.ForceRebalanceAsync();

        Assert.True(result.WasCoordinator);
        Assert.Equal(5, result.LeasesRevoked); // 10/2 nodes: shed exactly the excess, not everything
        Assert.Equal(1, result.NodesShed);
        await TestCluster.WaitUntilAsync(() => lost.Count == 5, because: "the owner should see Rebalanced losses");
        Assert.Equal(5, a.HeldLeases.Count); // the survivor half stays put — minimal movement

        // After the hold-down, b can pick the freed keys up (steering sends them its way).
        foreach (var key in lost)
        {
            await TestCluster.WaitUntilAsync(
                async () => await b.CanProcessAsync(key.Type, key.Id, ProcessingPreference.This),
                because: $"{key} should become acquirable by the underloaded node");
        }

        Assert.Equal(5, a.HeldLeases.Count);
        Assert.Equal(5, b.HeldLeases.Count);
    }

    [Fact]
    public async Task Non_coordinator_calls_do_nothing_silently()
    {
        var coordinator = await _cluster.StartNodeAsync();
        for (var i = 0; i < 6; i++)
            Assert.True(await coordinator.CanProcessAsync("job", $"j{i}", ProcessingPreference.This));
        var member = await _cluster.StartNodeAsync();

        var result = await member.ForceRebalanceAsync();

        Assert.False(result.WasCoordinator);
        Assert.Equal(0, result.LeasesRevoked);
        Assert.Equal(6, coordinator.HeldLeases.Count); // nothing changed anywhere
    }

    [Fact]
    public async Task Balanced_cluster_rebalance_is_a_noop()
    {
        var a = await _cluster.StartNodeAsync();
        var b = await _cluster.StartNodeAsync();
        for (var i = 0; i < 3; i++)
        {
            Assert.True(await a.CanProcessAsync("job", $"a{i}", ProcessingPreference.This));
            Assert.True(await b.CanProcessAsync("job", $"b{i}", ProcessingPreference.This));
        }

        var result = await a.ForceRebalanceAsync();

        Assert.True(result.WasCoordinator);
        Assert.Equal(0, result.LeasesRevoked);
        Assert.Equal(3, a.HeldLeases.Count);
        Assert.Equal(3, b.HeldLeases.Count);
    }

    [Fact]
    public async Task Drains_a_non_accepting_member_completely()
    {
        var coordinator = await _cluster.StartNodeAsync();
        var draining = await _cluster.StartNodeAsync();
        for (var i = 0; i < 4; i++)
            Assert.True(await draining.CanProcessAsync("job", $"d{i}", ProcessingPreference.This));

        await draining.SetAcceptingWorkAsync(false);
        await TestCluster.WaitUntilAsync(() =>
            coordinator.Members.Single(m => m.NodeId == draining.LocalNodeId).AcceptsWork == false);

        var result = await coordinator.ForceRebalanceAsync();

        Assert.Equal(4, result.LeasesRevoked);
        await TestCluster.WaitUntilAsync(() => draining.HeldLeases.Count == 0,
            because: "drain = AcceptWork off + ForceRebalance moves everything away");

        // And the coordinator can absorb the freed work after the hold-down.
        for (var i = 0; i < 4; i++)
        {
            await TestCluster.WaitUntilAsync(
                async () => await coordinator.CanProcessAsync("job", $"d{i}", ProcessingPreference.This));
        }
    }

    [Fact]
    public async Task Revoked_keys_never_grant_while_the_old_owner_still_holds_them()
    {
        var a = await _cluster.StartNodeAsync();
        for (var i = 0; i < 8; i++)
            Assert.True(await a.CanProcessAsync("job", $"j{i}", ProcessingPreference.This));
        var b = await _cluster.StartNodeAsync();

        var result = await a.ForceRebalanceAsync();
        Assert.True(result.LeasesRevoked > 0);

        // Exclusivity invariant across the move: at no point may BOTH nodes answer true.
        for (var i = 0; i < 8; i++)
        {
            var key = $"j{i}";
            var aHolds = await a.CanProcessAsync("job", key);
            var bHolds = await b.CanProcessAsync("job", key, ProcessingPreference.This);
            Assert.False(aHolds && bHolds, $"dual ownership of {key} during rebalance");
        }
    }
}
