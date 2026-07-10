using CoLibra.Runtime;

namespace CoLibra.IntegrationTests;

public class CoordinatorModeTests(ITestOutputHelper output) : IAsyncLifetime
{
    private readonly TestCluster _cluster = new(output);

    public ValueTask InitializeAsync() => ValueTask.CompletedTask;

    public async ValueTask DisposeAsync() => await _cluster.DisposeAsync();

    [Fact]
    public async Task Forced_node_becomes_coordinator_alone()
    {
        var server = await _cluster.StartNodeAsync(o => o.CoordinatorMode = CoordinatorMode.Forced);
        Assert.Equal(ClusterState.Coordinator, server.State);
    }

    [Fact]
    public async Task Forced_node_takes_over_an_existing_cluster()
    {
        var a = await _cluster.StartNodeAsync();
        var b = await _cluster.StartNodeAsync();
        Assert.Equal(ClusterState.Coordinator, a.State);

        var server = await _cluster.StartNodeAsync(o => o.CoordinatorMode = CoordinatorMode.Forced);

        await TestCluster.WaitUntilAsync(
            () => server.State == ClusterState.Coordinator &&
                  a.State == ClusterState.Member && b.State == ClusterState.Member &&
                  server.Members.Count == 3,
            because: "the forced node must supersede the incumbent and absorb its members");
    }

    [Fact]
    public async Task Forced_node_keeps_leadership_after_takeover_and_leases_survive()
    {
        var a = await _cluster.StartNodeAsync();
        Assert.True(await a.CanProcessAsync("job", "held-before", ProcessingPreference.This));

        var server = await _cluster.StartNodeAsync(o => o.CoordinatorMode = CoordinatorMode.Forced);
        await TestCluster.WaitUntilAsync(
            () => server.State == ClusterState.Coordinator && a.State == ClusterState.Member);

        // a's lease was re-asserted during its rejoin; nobody else can take the key.
        Assert.True(await a.CanProcessAsync("job", "held-before"));
        Assert.False(await server.CanProcessAsync("job", "held-before", ProcessingPreference.This));

        // Stability: the incumbent must not reclaim leadership afterwards.
        await Task.Delay(TimeSpan.FromSeconds(1));
        Assert.Equal(ClusterState.Coordinator, server.State);
        Assert.Equal(ClusterState.Member, a.State);
    }

    [Fact]
    public async Task Forced_node_wins_the_election_after_coordinator_death()
    {
        var a = await _cluster.StartNodeAsync(); // first up: coordinator
        var server = await _cluster.StartNodeAsync(o => o.CoordinatorMode = CoordinatorMode.Forced);
        await TestCluster.WaitUntilAsync(() => server.State == ClusterState.Coordinator);
        var c = await _cluster.StartNodeAsync();

        await _cluster.StopNodeAsync(server); // kill the forced coordinator
        await TestCluster.WaitUntilAsync(
            () => TestCluster.CoordinatorOf([a, c]) is not null,
            because: "eligible nodes still elect among themselves when the forced node dies");

        var server2 = await _cluster.StartNodeAsync(o => o.CoordinatorMode = CoordinatorMode.Forced);
        await TestCluster.WaitUntilAsync(
            () => server2.State == ClusterState.Coordinator &&
                  a.State == ClusterState.Member && c.State == ClusterState.Member,
            because: "a returning forced node reclaims leadership");
    }

    [Fact]
    public async Task Two_forced_nodes_converge_to_one_coordinator()
    {
        var f1 = await _cluster.StartNodeAsync(o => o.CoordinatorMode = CoordinatorMode.Forced);
        var f2 = await _cluster.StartNodeAsync(o => o.CoordinatorMode = CoordinatorMode.Forced, waitForCluster: false);

        await TestCluster.WaitUntilAsync(
            () => new[] { f1, f2 }.Count(n => n.State == ClusterState.Coordinator) == 1 &&
                  new[] { f1, f2 }.All(n => n.State is ClusterState.Coordinator or ClusterState.Member),
            because: "forced nodes settle among themselves");

        // Stability check: no leadership flapping.
        var coordinator = TestCluster.CoordinatorOf([f1, f2])!;
        await Task.Delay(TimeSpan.FromSeconds(1));
        Assert.Equal(ClusterState.Coordinator, coordinator.State);
    }

    [Fact]
    public async Task Never_node_joins_but_never_leads()
    {
        var a = await _cluster.StartNodeAsync();
        var client = await _cluster.StartNodeAsync(o => o.CoordinatorMode = CoordinatorMode.Never);
        Assert.Equal(ClusterState.Member, client.State);

        await _cluster.StopNodeAsync(a);

        // With no eligible node left, the Never node must not claim; it keeps searching.
        await Task.Delay(TimeSpan.FromSeconds(1));
        Assert.NotEqual(ClusterState.Coordinator, client.State);

        // An eligible node arriving resolves the wait.
        var rescue = await _cluster.StartNodeAsync();
        await TestCluster.WaitUntilAsync(
            () => rescue.State == ClusterState.Coordinator && client.State == ClusterState.Member,
            because: "the Never node should join the newly available coordinator");
    }

    [Fact]
    public async Task Game_server_topology_works_end_to_end()
    {
        // Asymmetric architecture: one forced "game server" + Never "clients", with messaging.
        var server = await _cluster.StartNodeAsync(o =>
        {
            o.CoordinatorMode = CoordinatorMode.Forced;
            o.NodeName = "game-server";
            o.Messaging.Enabled = true;
        });
        var client1 = await _cluster.StartNodeAsync(o =>
        {
            o.CoordinatorMode = CoordinatorMode.Never;
            o.NodeName = "player-1";
            o.Messaging.Enabled = true;
        });

        Assert.Equal(ClusterState.Coordinator, server.State);
        Assert.Equal(ClusterState.Member, client1.State);

        var received = new TaskCompletionSource<string?>(TaskCreationOptions.RunContinuationsAsynchronously);
        _ = server.Messenger.RegisterHandler("game", (message, _) =>
        {
            received.TrySetResult(message.OriginName);
            return ValueTask.CompletedTask;
        });

        var results = await client1.Messenger.SendByNameAsync("game-server", "game", new byte[] { 1 });
        Assert.Single(results);
        Assert.True(results[0].Delivered);
        Assert.Equal("player-1", await received.Task.WaitAsync(TestCluster.Eventually));
    }
}
