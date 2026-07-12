using CoLibra.Runtime;

namespace CoLibra.IntegrationTests;

public class DiscoveryAndMembershipTests : IAsyncLifetime
{
    private readonly TestCluster _cluster = new();

    public ValueTask InitializeAsync() => ValueTask.CompletedTask;

    public ValueTask DisposeAsync() => _cluster.DisposeAsync();

    [Fact]
    public async Task First_node_becomes_coordinator_second_joins_as_member()
    {
        var first = await _cluster.StartNodeAsync();
        Assert.Equal(ClusterState.Coordinator, first.State);

        var second = await _cluster.StartNodeAsync();
        Assert.Equal(ClusterState.Member, second.State);

        await TestCluster.WaitUntilAsync(() => first.Members.Count == 2 && second.Members.Count == 2,
            because: "both nodes should see a 2-node member list");
        Assert.Contains(second.Members, m => m.NodeId == first.LocalNodeId && m.IsCoordinator);
    }

    [Fact]
    public async Task Simultaneous_startup_converges_to_a_single_coordinator()
    {
        var nodes = new List<CoLibraNode>();
        for (var i = 0; i < 3; i++)
            nodes.Add(await _cluster.StartNodeAsync(waitForCluster: false));

        await TestCluster.WaitUntilAsync(
            () => nodes.Count(n => n.State == ClusterState.Coordinator) == 1 &&
                  nodes.Count(n => n.State == ClusterState.Member) == 2 &&
                  nodes.All(n => n.Members.Count == 3),
            because: "three simultaneous starters should converge to one coordinator with full membership");
    }

    [Fact]
    public async Task Nodes_of_different_services_never_mix()
    {
        var orders = await _cluster.StartNodeAsync(o => o.ServiceId = "orders");
        var payments = await _cluster.StartNodeAsync(o => o.ServiceId = "payments");

        // Both stand alone as coordinators of their own single-node clusters.
        Assert.Equal(ClusterState.Coordinator, orders.State);
        Assert.Equal(ClusterState.Coordinator, payments.State);
        await Task.Delay(500);
        Assert.Single(orders.Members);
        Assert.Single(payments.Members);
    }

    [Fact]
    public async Task Nodes_with_different_secrets_never_mix()
    {
        var real = await _cluster.StartNodeAsync();
        var impostor = await _cluster.StartNodeAsync(o => o.SharedSecret = "wrong");

        Assert.Equal(ClusterState.Coordinator, real.State);
        Assert.Equal(ClusterState.Coordinator, impostor.State);
        await Task.Delay(500);
        Assert.Single(real.Members);
    }

    [Fact]
    public async Task Incompatible_service_versions_form_separate_clusters()
    {
        var v1 = await _cluster.StartNodeAsync(o => o.ServiceVersion = new Version(1, 0));
        var v2 = await _cluster.StartNodeAsync(o => o.ServiceVersion = new Version(2, 0)); // MajorMatch default

        Assert.Equal(ClusterState.Coordinator, v1.State);
        Assert.Equal(ClusterState.Coordinator, v2.State);
        await Task.Delay(500);
        Assert.Single(v1.Members);
        Assert.Single(v2.Members);
    }

    [Fact]
    public async Task Compatible_minor_versions_join_the_same_cluster()
    {
        var v10 = await _cluster.StartNodeAsync(o => o.ServiceVersion = new Version(1, 0));
        var v15 = await _cluster.StartNodeAsync(o => o.ServiceVersion = new Version(1, 5));

        Assert.Equal(ClusterState.Coordinator, v10.State);
        Assert.Equal(ClusterState.Member, v15.State);
    }

    [Fact]
    public async Task Live_duplicate_node_id_is_rejected_and_faults()
    {
        var fixedId = Guid.NewGuid();
        var coordinator = await _cluster.StartNodeAsync();
        var original = await _cluster.StartNodeAsync(o => o.NodeId = fixedId);
        Assert.Equal(ClusterState.Member, original.State);

        var duplicate = await _cluster.StartNodeAsync(o => o.NodeId = fixedId, waitForCluster: false);
        await TestCluster.WaitUntilAsync(() => duplicate.State == ClusterState.Faulted,
            because: "a second live node with the same NodeId must fault (the original, a different "
                + "incarnation, keeps its place)");
        Assert.Equal(ClusterState.Member, original.State);
        Assert.Equal(ClusterState.Coordinator, coordinator.State);

        // The faulted node fails WaitForClusterAsync fast rather than hanging forever.
        await Assert.ThrowsAsync<InvalidOperationException>(() => duplicate.WaitForClusterAsync());
    }

    [Fact]
    public async Task Restarted_node_with_fixed_id_rejoins_after_its_ghost_disconnects()
    {
        var coordinator = await _cluster.StartNodeAsync();
        var fixedId = Guid.NewGuid();

        var incarnation1 = await _cluster.StartNodeAsync(o => o.NodeId = fixedId);
        await _cluster.StopNodeAsync(incarnation1); // clean close removes the session

        var incarnation2 = await _cluster.StartNodeAsync(o => o.NodeId = fixedId);
        Assert.Equal(ClusterState.Member, incarnation2.State);
        await TestCluster.WaitUntilAsync(() => coordinator.Members.Count == 2);
    }

    [Fact]
    public async Task Member_departure_is_detected_and_membership_shrinks()
    {
        var coordinator = await _cluster.StartNodeAsync();
        var member = await _cluster.StartNodeAsync();
        await TestCluster.WaitUntilAsync(() => coordinator.Members.Count == 2);

        await _cluster.StopNodeAsync(member);
        await TestCluster.WaitUntilAsync(() => coordinator.Members.Count == 1,
            because: "the coordinator should notice the member leaving");
    }
}
