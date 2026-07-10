using CoLibra.Runtime;

namespace CoLibra.IntegrationTests;

public class AcceptWorkTests(ITestOutputHelper output) : IAsyncLifetime
{
    private readonly TestCluster _cluster = new(output);

    public ValueTask InitializeAsync() => ValueTask.CompletedTask;

    public async ValueTask DisposeAsync() => await _cluster.DisposeAsync();

    [Fact]
    public async Task Non_accepting_node_is_denied_locally_and_at_the_coordinator()
    {
        var coordinator = await _cluster.StartNodeAsync();
        var idle = await _cluster.StartNodeAsync(o => o.AcceptWork = false);

        Assert.False(idle.IsAcceptingWork);
        Assert.False(await idle.CanProcessAsync("job", "x", ProcessingPreference.This));
        var acquisition = await idle.TryAcquireAsync("job", "x");
        Assert.Equal(LeaseDenialReason.NotAcceptingWork, acquisition.DenialReason);

        // Other nodes are unaffected.
        Assert.True(await coordinator.CanProcessAsync("job", "x", ProcessingPreference.This));
    }

    [Fact]
    public async Task Authority_node_does_not_starve_workers_under_balanced_grants()
    {
        // The scenario AcceptWork exists for: an idle authority (game-server style) used to pin
        // the load-balance minimum at zero, denying every Balanced grant to busy workers.
        var authority = await _cluster.StartNodeAsync(o => o.AcceptWork = false);
        var worker = await _cluster.StartNodeAsync();
        Assert.Equal(ClusterState.Coordinator, authority.State);

        for (var i = 0; i < 5; i++)
        {
            Assert.True(await worker.CanProcessAsync("job", $"j{i}", ProcessingPreference.Balanced),
                $"worker should receive balanced grant {i} despite the idle authority node");
        }
    }

    [Fact]
    public async Task Acceptance_toggles_at_runtime_and_propagates()
    {
        var coordinator = await _cluster.StartNodeAsync();
        var worker = await _cluster.StartNodeAsync();

        Assert.True(await worker.CanProcessAsync("job", "before", ProcessingPreference.This));

        await worker.SetAcceptingWorkAsync(false);
        Assert.False(await worker.CanProcessAsync("job", "during", ProcessingPreference.This));

        // The flip is advertised via heartbeat and lands in everyone's member list.
        await TestCluster.WaitUntilAsync(() =>
            coordinator.Members.Single(m => m.NodeId == worker.LocalNodeId).AcceptsWork == false,
            because: "the acceptance flip should propagate to the coordinator's membership view");

        // Held leases are never revoked by the toggle.
        Assert.True(await worker.CanProcessAsync("job", "before"));

        await worker.SetAcceptingWorkAsync(true);
        await TestCluster.WaitUntilAsync(() =>
            coordinator.Members.Single(m => m.NodeId == worker.LocalNodeId).AcceptsWork,
            because: "the re-enable should propagate before grants resume");
        Assert.True(await worker.CanProcessAsync("job", "after", ProcessingPreference.This));
    }

    [Fact]
    public async Task Forced_assignment_skips_non_accepting_handlers()
    {
        var origin = await _cluster.StartNodeAsync(o => o.Routing.Enabled = true);
        var idleHandler = await _cluster.StartNodeAsync(o =>
        {
            o.Routing.Enabled = true;
            o.AcceptWork = false;
        });
        var worker = await _cluster.StartNodeAsync(o => o.Routing.Enabled = true);

        var hits = new List<NodeId>();
        _ = idleHandler.Router.RegisterHandler("evt", (d, _) => { lock (hits) { hits.Add(idleHandler.LocalNodeId); } return ValueTask.CompletedTask; });
        _ = worker.Router.RegisterHandler("evt", (d, _) => { lock (hits) { hits.Add(worker.LocalNodeId); } return ValueTask.CompletedTask; });
        await TestCluster.WaitUntilAsync(async () => await origin.CountRoutedAdvertisersAsync("evt") >= 1);

        for (var i = 0; i < 3; i++)
        {
            var result = await origin.Router.RouteAsync("evt", $"k{i}", new byte[] { 1 });
            Assert.Equal(RouteStatus.Delivered, result.Status);
            Assert.Equal(worker.LocalNodeId, result.Owner);
        }

        lock (hits)
        {
            Assert.DoesNotContain(idleHandler.LocalNodeId, hits);
        }
    }

    [Fact]
    public async Task Nobody_accepting_yields_NoHandler_for_routing()
    {
        var origin = await _cluster.StartNodeAsync(o => o.Routing.Enabled = true);
        var idle = await _cluster.StartNodeAsync(o =>
        {
            o.Routing.Enabled = true;
            o.AcceptWork = false;
        });
        _ = idle.Router.RegisterHandler("evt", (d, _) => ValueTask.CompletedTask);

        var result = await origin.Router.RouteAsync("evt", "x", new byte[] { 1 });

        Assert.Equal(RouteStatus.NoHandler, result.Status);
    }
}
