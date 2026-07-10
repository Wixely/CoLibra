using CoLibra.Runtime;

namespace CoLibra.IntegrationTests;

/// <summary>
/// A common deployment ships ONE config to every node, so the static seed list ends up
/// containing every server — including the node running it. These tests prove a node probing
/// its own address is harmless: discovery messages from self are dropped by the NodeId guard,
/// so the node still forms/joins a cluster and leases stay exclusive.
/// </summary>
public class SelfSeedTests(ITestOutputHelper output) : IAsyncLifetime
{
    private readonly TestCluster _cluster = new(output);

    // The in-memory hub assigns 127.0.0.2, .3, ... in order, so the first two nodes are known.
    private const string NodeA = "127.0.0.2:41101";
    private const string NodeB = "127.0.0.3:41101";

    public ValueTask InitializeAsync() => ValueTask.CompletedTask;

    public async ValueTask DisposeAsync() => await _cluster.DisposeAsync();

    [Fact]
    public async Task A_lone_node_seeded_with_its_own_address_still_coordinates_and_works()
    {
        var a = await _cluster.StartNodeAsync(o => o.StaticSeeds.Add(NodeA)); // seeds == just itself

        Assert.Equal(ClusterState.Coordinator, a.State);
        Assert.True(await a.CanProcessAsync("job", "1", ProcessingPreference.This));
    }

    [Fact]
    public async Task Every_node_carrying_the_full_self_inclusive_seed_list_clusters_normally()
    {
        // The realistic "one shared config" shape: every node's seed list is the whole fleet,
        // so each list includes that node's own address plus the others.
        string[] fullFleet = [NodeA, NodeB];
        var a = await _cluster.StartNodeAsync(o => o.StaticSeeds.AddRange(fullFleet));
        var b = await _cluster.StartNodeAsync(o => o.StaticSeeds.AddRange(fullFleet));

        await TestCluster.WaitUntilAsync(
            () => a.Members.Count == 2 && b.Members.Count == 2,
            because: "self-referential seeds must not prevent the two nodes from finding each other");

        // Exclusivity is intact — self-probing didn't corrupt ownership.
        Assert.True(await a.CanProcessAsync("job", "shared", ProcessingPreference.This));
        Assert.False(await b.CanProcessAsync("job", "shared", ProcessingPreference.This));

        // And the survivor keeps working after the other leaves.
        await _cluster.StopNodeAsync(a);
        await TestCluster.WaitUntilAsync(
            () => b.State == ClusterState.Coordinator,
            because: "the remaining self-seeded node should take over cleanly");
    }
}
