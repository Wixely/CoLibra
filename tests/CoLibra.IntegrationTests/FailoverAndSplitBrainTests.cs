using CoLibra.Runtime;

namespace CoLibra.IntegrationTests;

public class FailoverAndSplitBrainTests(ITestOutputHelper output) : IAsyncLifetime
{
    private readonly TestCluster _cluster = new(output);

    public ValueTask InitializeAsync() => ValueTask.CompletedTask;

    public ValueTask DisposeAsync() => _cluster.DisposeAsync();

    [Fact]
    public async Task Killing_the_coordinator_triggers_election_and_failover()
    {
        var coordinator = await _cluster.StartNodeAsync();
        var m1 = await _cluster.StartNodeAsync();
        var m2 = await _cluster.StartNodeAsync();

        await TestCluster.WaitUntilAsync(() => coordinator.Members.Count == 3);
        await _cluster.StopNodeAsync(coordinator);

        await TestCluster.WaitUntilAsync(
            () => TestCluster.CoordinatorOf([m1, m2]) is not null,
            because: "a survivor should win the election");
        var successor = TestCluster.CoordinatorOf([m1, m2])!;
        var other = successor == m1 ? m2 : m1;
        await TestCluster.WaitUntilAsync(() => other.State == ClusterState.Member && other.Members.Count == 2,
            because: "the other survivor should rejoin the new coordinator");
    }

    [Fact]
    public async Task Held_leases_survive_coordinator_failover()
    {
        var coordinator = await _cluster.StartNodeAsync();
        var m1 = await _cluster.StartNodeAsync();
        var m2 = await _cluster.StartNodeAsync();

        Assert.True(await m1.CanProcessAsync("t", "1", ProcessingPreference.This));
        Assert.True(await m2.CanProcessAsync("t", "2", ProcessingPreference.This));

        var lostRaised = false;
        m1.LeaseLost += (_, _) => lostRaised = true;
        m2.LeaseLost += (_, _) => lostRaised = true;

        await _cluster.StopNodeAsync(coordinator);
        var survivors = new[] { m1, m2 };
        await TestCluster.WaitUntilAsync(() => TestCluster.CoordinatorOf(survivors) is not null &&
            survivors.All(n => n.State is ClusterState.Coordinator or ClusterState.Member));

        // Both keys remain exclusively owned by their original holders across the failover.
        Assert.True(await m1.CanProcessAsync("t", "1"));
        Assert.True(await m2.CanProcessAsync("t", "2"));
        Assert.False(await m1.CanProcessAsync("t", "2", ProcessingPreference.This));
        Assert.False(await m2.CanProcessAsync("t", "1", ProcessingPreference.This));
        Assert.False(lostRaised);
    }

    [Fact]
    public async Task Majority_partition_elects_a_new_coordinator_and_minority_loses_quorum()
    {
        var nodes = new List<CoLibraNode> { await _cluster.StartNodeAsync() };
        for (var i = 0; i < 4; i++)
            nodes.Add(await _cluster.StartNodeAsync());
        var coordinator = nodes[0];
        var minorityMember = nodes[1];
        var majority = nodes.Skip(2).ToArray();
        await TestCluster.WaitUntilAsync(() => coordinator.Members.Count == 5);

        var splitBrainSeen = false;
        coordinator.SplitBrainDetected += (_, _) => splitBrainSeen = true;

        _cluster.Partition([coordinator, minorityMember], majority);

        await TestCluster.WaitUntilAsync(
            () => TestCluster.CoordinatorOf(majority) is not null,
            because: "the 3-node majority should elect its own coordinator");
        await TestCluster.WaitUntilAsync(
            () => coordinator.State == ClusterState.QuorumLost,
            because: "the old coordinator sees 2 of 5 nodes and loses quorum");
        Assert.True(splitBrainSeen);

        // New leases are denied on the minority side under the default policy.
        Assert.False(await coordinator.CanProcessAsync("t", "new-key"));
    }

    [Fact]
    public async Task Minority_coordinator_stays_quorum_lost_past_the_departed_decay_window()
    {
        var nodes = new List<CoLibraNode> { await _cluster.StartNodeAsync() };
        for (var i = 0; i < 4; i++)
            nodes.Add(await _cluster.StartNodeAsync());
        var coordinator = nodes[0];
        var majority = nodes.Skip(2).ToArray();
        await TestCluster.WaitUntilAsync(() => coordinator.Members.Count == 5);

        _cluster.Partition([coordinator, nodes[1]], majority);
        await TestCluster.WaitUntilAsync(() => coordinator.State == ClusterState.QuorumLost,
            because: "2 of 5 is below quorum");

        // Past the RecentlyDeparted decay window (MemberTimeout*6): the minority coordinator must NOT
        // forget the majority and re-declare quorum — that would resume granting alongside the
        // majority's own coordinator (dual granting).
        await Task.Delay(TimeSpan.FromMilliseconds((600 * 6 + 2500) * TestCluster.Scale));
        Assert.Equal(ClusterState.QuorumLost, coordinator.State);
        Assert.False(await coordinator.CanProcessAsync("t", "x"));
    }

    [Fact]
    public async Task Healing_a_partition_merges_back_to_a_single_coordinator()
    {
        var nodes = new List<CoLibraNode> { await _cluster.StartNodeAsync() };
        for (var i = 0; i < 4; i++)
            nodes.Add(await _cluster.StartNodeAsync());
        var oldCoordinator = nodes[0];
        var majority = nodes.Skip(2).ToArray();

        _cluster.Partition([oldCoordinator, nodes[1]], majority);
        await TestCluster.WaitUntilAsync(() => TestCluster.CoordinatorOf(majority) is not null);

        _cluster.Heal();

        // The merge winner is whichever side holds the higher term when the partitions meet
        // (post-heal elections can bump terms); the invariant is a single coordinator and
        // full membership, with the pre-partition reign ended.
        await TestCluster.WaitUntilAsync(
            () => nodes.Count(n => n.State == ClusterState.Coordinator) == 1 &&
                  nodes.All(n => n.State is ClusterState.Coordinator or ClusterState.Member) &&
                  nodes.All(n => n.Members.Count == 5),
            timeout: TimeSpan.FromSeconds(25),
            because: "after healing, exactly one coordinator survives and everyone rejoins it");
    }

    [Fact]
    public async Task Conflicting_owners_after_heal_resolve_by_fencing_token()
    {
        // 2-node cluster: a partition deliberately lets both sides continue (documented caveat).
        var a = await _cluster.StartNodeAsync();
        var b = await _cluster.StartNodeAsync();

        Assert.True(await a.CanProcessAsync("t", "1", ProcessingPreference.This));
        _cluster.Partition([a], [b]);

        // b times out the lease it saw, elects itself, and claims the key with a newer term.
        await TestCluster.WaitUntilAsync(
            () => b.State == ClusterState.Coordinator, because: "b claims its own partition");
        await TestCluster.WaitUntilAsync(
            () => b.CanProcessAsync("t", "1", ProcessingPreference.This).AsTask().GetAwaiter().GetResult(),
            because: "b can claim the key once a's lease TTL lapses on b's side");

        _cluster.Heal();
        await TestCluster.WaitUntilAsync(
            () => new[] { a, b }.Count(n => n.State == ClusterState.Coordinator) == 1 &&
                  new[] { a, b }.All(n => n.State is ClusterState.Coordinator or ClusterState.Member),
            timeout: TimeSpan.FromSeconds(25));

        // Exactly one owner survives the merge; b's token was granted under the higher term.
        await TestCluster.WaitUntilAsync(async () =>
        {
            var aCan = await a.CanProcessAsync("t", "1");
            var bCan = await b.CanProcessAsync("t", "1");
            return !aCan && bCan;
        });
    }

    [Fact]
    public async Task Coordinator_does_not_regrant_a_partitioned_members_lease_before_it_self_fences()
    {
        // 3 nodes: a single member is partitioned into the minority. The coordinator + the other
        // member stay the majority. This is NOT the accepted 2-node split-brain case — exclusivity
        // must hold: the coordinator must not free and re-grant M's lease while M (still running,
        // not yet self-fenced) keeps answering CanProcess == true for it.
        var coordinator = await _cluster.StartNodeAsync();
        var m = await _cluster.StartNodeAsync();
        var n = await _cluster.StartNodeAsync();
        await TestCluster.WaitUntilAsync(() => coordinator.Members.Count == 3);

        Assert.True(await m.CanProcessAsync("t", "K", ProcessingPreference.This));

        // Isolate M; the coordinator and N remain a quorum and keep coordinating.
        _cluster.Partition([coordinator, n], [m]);

        // The coordinator times M out of the membership after MemberTimeout.
        await TestCluster.WaitUntilAsync(() => coordinator.Members.Count == 2,
            because: "the coordinator drops the partitioned member from membership");

        // M has not self-fenced yet (that happens only at LeaseTtl - safety margin, which is
        // configured strictly later than MemberTimeout). While M still owns K, N must be denied.
        var mStillHoldsK = await m.CanProcessAsync("t", "K");
        var nStoleK = await n.CanProcessAsync("t", "K", ProcessingPreference.This);
        Assert.True(mStillHoldsK, "precondition: M still believes it owns K within the safety window");
        Assert.False(nStoleK,
            "EXCLUSIVITY VIOLATION: coordinator re-granted a partitioned member's lease before the member self-fenced");
    }

    [Fact]
    public async Task Throw_policy_raises_on_operations_during_quorum_loss()
    {
        var nodes = new List<CoLibraNode>();
        for (var i = 0; i < 3; i++)
            nodes.Add(await _cluster.StartNodeAsync(o => o.SplitBrainPolicy = SplitBrainPolicy.ThrowOnAllOperations));
        var coordinator = TestCluster.CoordinatorOf(nodes)!;
        var members = nodes.Where(n => n != coordinator).ToArray();

        _cluster.Partition([coordinator], members);
        await TestCluster.WaitUntilAsync(() => coordinator.State == ClusterState.QuorumLost,
            because: "the coordinator alone is below quorum of 3");

        await Assert.ThrowsAsync<SplitBrainException>(async () =>
            await coordinator.CanProcessAsync("t", "1"));
    }

    [Fact]
    public async Task Continue_policy_keeps_granting_during_quorum_loss()
    {
        var nodes = new List<CoLibraNode>();
        for (var i = 0; i < 3; i++)
            nodes.Add(await _cluster.StartNodeAsync(o => o.SplitBrainPolicy = SplitBrainPolicy.Continue));
        var coordinator = TestCluster.CoordinatorOf(nodes)!;
        var members = nodes.Where(n => n != coordinator).ToArray();

        _cluster.Partition([coordinator], members);
        await TestCluster.WaitUntilAsync(() => coordinator.State == ClusterState.QuorumLost);

        Assert.True(await coordinator.CanProcessAsync("t", "1"));
    }
}
