using CoLibra.Leasing;
using Microsoft.Extensions.Time.Testing;

namespace CoLibra.Tests;

public class ForceRebalanceTests
{
    private readonly FakeTimeProvider _time = new();

    private CoordinatorLeaseTable Table(Action<CoLibraOptions>? mutate = null)
    {
        var options = new CoLibraOptions { ServiceId = "svc", SharedSecret = "s" };
        mutate?.Invoke(options);
        return new CoordinatorLeaseTable(1, options, _time);
    }

    private void Grant(CoordinatorLeaseTable table, NodeId node, string type, int count, int startAt = 0)
    {
        for (var i = 0; i < count; i++)
        {
            // Full Guid in the key: NodeId's short ToString collides across same-millisecond v7 ids.
            var outcome = table.Acquire(node, Guid.NewGuid(), new LeaseKey(type, $"{node.Value:N}:{startAt + i}"),
                ProcessingPreference.This, _time.GetTimestamp());
            Assert.True(outcome.Immediate!.Value.Granted);
        }
    }

    [Fact]
    public void Sheds_only_the_excess_from_the_overloaded_node()
    {
        var table = Table();
        var (a, b) = (NodeId.NewId(), NodeId.NewId());
        table.NodeUp(a, 1.0);
        table.NodeUp(b, 1.0);
        Grant(table, a, "job", 10);

        var revocations = table.ForceRebalance(null, _time.GetTimestamp());

        // mean = 10/2 = 5; a sheds down to ceil(5) = 5, all from a; b untouched.
        Assert.Equal(5, revocations.Count);
        Assert.All(revocations, r => Assert.Equal(a, r.Owner));
    }

    [Fact]
    public void Balanced_cluster_moves_nothing()
    {
        var table = Table();
        var (a, b) = (NodeId.NewId(), NodeId.NewId());
        table.NodeUp(a, 1.0);
        table.NodeUp(b, 1.0);
        Grant(table, a, "job", 5);
        Grant(table, b, "job", 5);

        Assert.Empty(table.ForceRebalance(null, _time.GetTimestamp()));
    }

    [Fact]
    public void Within_tolerance_moves_nothing()
    {
        var table = Table(); // LoadBalanceTolerance default 1
        var (a, b) = (NodeId.NewId(), NodeId.NewId());
        table.NodeUp(a, 1.0);
        table.NodeUp(b, 1.0);
        Grant(table, a, "job", 4);
        Grant(table, b, "job", 3);

        // mean 3.5, a's load 4 <= 3.5 + 1 → untouched.
        Assert.Empty(table.ForceRebalance(null, _time.GetTimestamp()));
    }

    [Fact]
    public void Newest_grants_are_shed_first()
    {
        var table = Table();
        var (a, b) = (NodeId.NewId(), NodeId.NewId());
        table.NodeUp(a, 1.0);
        Grant(table, a, "job", 4);          // ids a:0..a:3, oldest sequences
        table.NodeUp(b, 1.0);
        Grant(table, a, "job", 4, startAt: 4); // ids a:4..a:7, newest sequences

        var revocations = table.ForceRebalance(null, _time.GetTimestamp());

        // mean = 8/2 = 4 → shed 4, and they must be the newest four (a:4..a:7).
        Assert.Equal(4, revocations.Count);
        Assert.All(revocations, r => Assert.Contains(int.Parse(r.Key.Id.Split(':')[1]), new[] { 4, 5, 6, 7 }));
    }

    [Fact]
    public void None_balance_types_are_skipped()
    {
        var table = Table(o => o.PerTypeLoadBalance["fcfs"] = LoadBalanceType.None);
        var (a, b) = (NodeId.NewId(), NodeId.NewId());
        table.NodeUp(a, 1.0);
        table.NodeUp(b, 1.0);
        Grant(table, a, "fcfs", 10);

        Assert.Empty(table.ForceRebalance(null, _time.GetTimestamp()));
    }

    [Fact]
    public void Type_filter_limits_the_pass()
    {
        var table = Table();
        var (a, b) = (NodeId.NewId(), NodeId.NewId());
        table.NodeUp(a, 1.0);
        table.NodeUp(b, 1.0);
        Grant(table, a, "hot", 6);
        Grant(table, a, "cold", 6);

        var revocations = table.ForceRebalance("hot", _time.GetTimestamp());

        Assert.Equal(3, revocations.Count);
        Assert.All(revocations, r => Assert.Equal("hot", r.Key.Type));
    }

    [Fact]
    public void Weighted_types_balance_by_capacity()
    {
        var table = Table(o => o.DefaultLoadBalance = LoadBalanceType.Weighted);
        var (big, small) = (NodeId.NewId(), NodeId.NewId());
        table.NodeUp(big, 3.0);
        table.NodeUp(small, 1.0);
        Grant(table, small, "job", 8);

        var revocations = table.ForceRebalance(null, _time.GetTimestamp());

        // meanLoad = 8/(3+1) = 2 → small's fair share = ceil(2×1) = 2 → sheds 6.
        Assert.Equal(6, revocations.Count);
        Assert.All(revocations, r => Assert.Equal(small, r.Owner));
    }

    [Fact]
    public void Non_accepting_nodes_drain_completely()
    {
        var table = Table();
        var (draining, worker) = (NodeId.NewId(), NodeId.NewId());
        table.NodeUp(draining, 1.0);
        Grant(table, draining, "job", 4);
        table.NodeUp(worker, 1.0);
        table.SetAcceptsWork(draining, false);

        var revocations = table.ForceRebalance(null, _time.GetTimestamp());

        Assert.Equal(4, revocations.Count);
        Assert.All(revocations, r => Assert.Equal(draining, r.Owner));
    }

    [Fact]
    public void Revoked_keys_are_held_down_then_freed()
    {
        var table = Table();
        var (a, b) = (NodeId.NewId(), NodeId.NewId());
        table.NodeUp(a, 1.0);
        table.NodeUp(b, 1.0);
        Grant(table, a, "job", 6); // mean 3, tolerance 1: load 6 > 4 → sheds down to 3
        var revoked = table.ForceRebalance(null, _time.GetTimestamp());
        Assert.Equal(3, revoked.Count);
        var key = revoked[0].Key;

        // During hold-down: not grantable (reported as held by the old owner)...
        var denied = table.Acquire(b, Guid.NewGuid(), key, ProcessingPreference.This, _time.GetTimestamp());
        Assert.False(denied.Immediate!.Value.Granted);
        Assert.Equal(a, denied.Immediate.Value.CurrentOwner);

        // ...and the old owner's renewal must NOT re-adopt it.
        var lost = table.Renew(a, [(key, new FencingToken(1, 999))], _time.GetTimestamp());
        Assert.Equal([key], lost);

        // After the hold-down (the old owner's self-fence horizon, LeaseTtl - LeaseRenewSafetyMargin
        // = 12 s at defaults), the sweep frees it and b can take it.
        _time.Advance(TimeSpan.FromSeconds(13));
        var sweep = table.Sweep(_time.GetTimestamp());
        Assert.Contains(sweep.Freed, f => f.Key == key);
        var granted = table.Acquire(b, Guid.NewGuid(), key, ProcessingPreference.This, _time.GetTimestamp());
        Assert.True(granted.Immediate!.Value.Granted);
    }

    [Fact]
    public void Rejoining_owner_cannot_reassert_a_revoked_key()
    {
        var table = Table();
        var (a, b) = (NodeId.NewId(), NodeId.NewId());
        table.NodeUp(a, 1.0);
        table.NodeUp(b, 1.0);
        Grant(table, a, "job", 6);
        var key = table.ForceRebalance(null, _time.GetTimestamp())[0].Key;

        var rejected = table.AssertHeld(a, [(key, new FencingToken(1, 999))], _time.GetTimestamp());

        Assert.Equal([key], rejected);
    }
}
