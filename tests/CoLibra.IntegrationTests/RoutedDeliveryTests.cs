using System.Collections.Concurrent;
using CoLibra.Runtime;

namespace CoLibra.IntegrationTests;

public class RoutedDeliveryTests(ITestOutputHelper output) : IAsyncLifetime
{
    private readonly TestCluster _cluster = new(output);

    private static readonly Action<CoLibraOptions> WithRouting = o =>
    {
        o.Routing.Enabled = true;
        o.Routing.DeliveryTimeout = TimeSpan.FromSeconds(2);
        o.Routing.AssignmentAckTimeout = TimeSpan.FromMilliseconds(400);
        o.Routing.OwnerCacheTtl = TimeSpan.FromSeconds(2);
    };

    public ValueTask InitializeAsync() => ValueTask.CompletedTask;

    public async ValueTask DisposeAsync() => await _cluster.DisposeAsync();

    private static Task WaitForAdvertisersAsync(CoLibraNode coordinator, string type, int count) =>
        TestCluster.WaitUntilAsync(async () => await coordinator.CountRoutedAdvertisersAsync(type) >= count,
            because: $"the coordinator should see {count} advertiser(s) for '{type}'");

    private static ConcurrentBag<(NodeId Node, string Id, byte[] Payload)> NewSink() => [];

    private static IAsyncDisposable Handle(CoLibraNode node, string type, ConcurrentBag<(NodeId, string, byte[])> sink) =>
        node.Router.RegisterHandler(type, (delivery, _) =>
        {
            sink.Add((node.LocalNodeId, delivery.Key.Id, delivery.Payload.ToArray()));
            return ValueTask.CompletedTask;
        });

    [Fact]
    public async Task Routes_from_a_non_owner_to_the_handler_exactly_once()
    {
        var a = await _cluster.StartNodeAsync(WithRouting);
        var b = await _cluster.StartNodeAsync(WithRouting);
        var sink = NewSink();
        _ = Handle(b, "evt", sink);
        await WaitForAdvertisersAsync(a, "evt", 1);

        var payload = new byte[] { 1, 2, 3, 4 };
        var result = await a.Router.RouteAsync("evt", "e1", payload);

        Assert.Equal(RouteStatus.Delivered, result.Status);
        Assert.Equal(b.LocalNodeId, result.Owner);
        await TestCluster.WaitUntilAsync(() => sink.Count == 1);
        var (node, id, received) = sink.Single();
        Assert.Equal(b.LocalNodeId, node);
        Assert.Equal("e1", id);
        Assert.Equal(payload, received);

        // The forced assignment installed a real lease on b.
        Assert.Contains(new LeaseKey("evt", "e1"), b.HeldLeases);
        Assert.False(await a.CanProcessAsync("evt", "e1", ProcessingPreference.This));
    }

    [Fact]
    public async Task Routes_locally_when_the_origin_is_the_only_handler()
    {
        var a = await _cluster.StartNodeAsync(WithRouting);
        await _cluster.StartNodeAsync(WithRouting); // a peer with no handler
        var sink = NewSink();
        _ = Handle(a, "evt", sink);

        var result = await a.Router.RouteAsync("evt", "self", new byte[] { 9 });

        Assert.Equal(RouteStatus.DeliveredLocal, result.Status);
        await TestCluster.WaitUntilAsync(() => sink.Count == 1);
        Assert.Contains(new LeaseKey("evt", "self"), a.HeldLeases);
    }

    [Fact]
    public async Task Subsequent_routes_for_the_same_key_reach_the_same_owner()
    {
        var a = await _cluster.StartNodeAsync(WithRouting);
        var b = await _cluster.StartNodeAsync(WithRouting);
        var c = await _cluster.StartNodeAsync(WithRouting);
        var sink = NewSink();
        _ = Handle(b, "evt", sink);
        _ = Handle(c, "evt", sink);
        await WaitForAdvertisersAsync(a, "evt", 2);

        for (var i = 0; i < 5; i++)
        {
            var result = await a.Router.RouteAsync("evt", "sticky", new[] { (byte)i });
            Assert.True(result.Delivered, $"route {i}: {result.Status}");
        }

        await TestCluster.WaitUntilAsync(() => sink.Count == 5);
        Assert.Single(sink.Select(e => e.Item1).Distinct()); // one owner processed every payload
    }

    [Fact]
    public async Task Assignments_spread_across_handlers_by_load()
    {
        var a = await _cluster.StartNodeAsync(WithRouting);
        var b = await _cluster.StartNodeAsync(WithRouting);
        var c = await _cluster.StartNodeAsync(WithRouting);
        var sink = NewSink();
        _ = Handle(b, "evt", sink);
        _ = Handle(c, "evt", sink);
        await WaitForAdvertisersAsync(a, "evt", 2);

        for (var i = 0; i < 6; i++)
            Assert.True((await a.Router.RouteAsync("evt", $"k{i}", new byte[] { 1 })).Delivered);

        await TestCluster.WaitUntilAsync(() => sink.Count == 6);
        var perNode = sink.GroupBy(e => e.Item1).ToDictionary(g => g.Key, g => g.Count());
        Assert.Equal(2, perNode.Count);
        Assert.Equal(3, perNode[b.LocalNodeId]);
        Assert.Equal(3, perNode[c.LocalNodeId]);
    }

    [Fact]
    public async Task NoHandler_when_nobody_registered_the_type()
    {
        var a = await _cluster.StartNodeAsync(WithRouting);
        await _cluster.StartNodeAsync(WithRouting);

        var result = await a.Router.RouteAsync("nobody-home", "x", new byte[] { 1 });

        Assert.Equal(RouteStatus.NoHandler, result.Status);
    }

    [Fact]
    public async Task Owner_death_reassigns_to_the_surviving_handler()
    {
        var a = await _cluster.StartNodeAsync(WithRouting);
        var b = await _cluster.StartNodeAsync(WithRouting);
        var c = await _cluster.StartNodeAsync(WithRouting);
        var sink = NewSink();
        _ = Handle(b, "evt", sink);
        _ = Handle(c, "evt", sink);
        await WaitForAdvertisersAsync(a, "evt", 2);

        // Pin the key to one owner, find which, then kill it.
        Assert.True((await a.Router.RouteAsync("evt", "ha", new byte[] { 1 })).Delivered);
        await TestCluster.WaitUntilAsync(() => sink.Count == 1);
        var owner = sink.Single().Item1 == b.LocalNodeId ? b : c;
        var survivor = ReferenceEquals(owner, b) ? c : b;
        await _cluster.StopNodeAsync(owner);
        await TestCluster.WaitUntilAsync(() => a.Members.Count == 2, because: "the dead owner should be removed");

        var result = await a.Router.RouteAsync("evt", "ha", new byte[] { 2 });

        Assert.Equal(RouteStatus.Delivered, result.Status);
        Assert.Equal(survivor.LocalNodeId, result.Owner);
        await TestCluster.WaitUntilAsync(() => sink.Count(e => e.Item1 == survivor.LocalNodeId) == 1);
    }

    [Fact]
    public async Task Relay_path_works_without_direct_channels()
    {
        var a = await _cluster.StartNodeAsync(o => { WithRouting(o); o.Routing.UseDirectChannels = false; });
        var b = await _cluster.StartNodeAsync(o => { WithRouting(o); o.Routing.UseDirectChannels = false; });
        var c = await _cluster.StartNodeAsync(o => { WithRouting(o); o.Routing.UseDirectChannels = false; });
        var sink = NewSink();
        _ = Handle(c, "evt", sink);
        await WaitForAdvertisersAsync(a, "evt", 1);

        // b (member) routes to c (member): the payload must hop through the coordinator a.
        Assert.Equal(ClusterState.Coordinator, a.State);
        var result = await b.Router.RouteAsync("evt", "relayed", new byte[] { 7, 7 });

        Assert.Equal(RouteStatus.Delivered, result.Status);
        Assert.Equal(c.LocalNodeId, result.Owner);
        await TestCluster.WaitUntilAsync(() => sink.Count == 1);
        Assert.Equal(new byte[] { 7, 7 }, sink.Single().Item3);
    }

    [Fact]
    public async Task Minority_partition_refuses_to_route()
    {
        var nodes = new CoLibraNode[5];
        for (var i = 0; i < 5; i++)
            nodes[i] = await _cluster.StartNodeAsync(WithRouting);
        var coordinator = TestCluster.CoordinatorOf(nodes)!;
        var minorityMember = nodes.First(n => !ReferenceEquals(n, coordinator));
        var majority = nodes.Where(n => !ReferenceEquals(n, coordinator) && !ReferenceEquals(n, minorityMember)).ToArray();
        _ = Handle(minorityMember, "evt", NewSink());

        _cluster.Partition([coordinator, minorityMember], majority);
        await TestCluster.WaitUntilAsync(
            () => coordinator.State == ClusterState.QuorumLost,
            because: "the minority coordinator should detect quorum loss");

        var result = await minorityMember.Router.RouteAsync("evt", "blocked", new byte[] { 1 });

        Assert.Equal(RouteStatus.QuorumUnavailable, result.Status);
    }

    [Fact]
    public async Task Completed_keys_are_not_routable()
    {
        var a = await _cluster.StartNodeAsync(o => { WithRouting(o); o.CompletionTracking.Enabled = true; });
        var b = await _cluster.StartNodeAsync(o => { WithRouting(o); o.CompletionTracking.Enabled = true; });
        var sink = NewSink();
        _ = Handle(b, "evt", sink);
        await WaitForAdvertisersAsync(a, "evt", 1);

        Assert.True((await a.Router.RouteAsync("evt", "once", new byte[] { 1 })).Delivered);
        await b.MarkCompletedAsync("evt", "once");

        var result = await a.Router.RouteAsync("evt", "once", new byte[] { 2 });

        Assert.Equal(RouteStatus.KeyCompleted, result.Status);
        await TestCluster.WaitUntilAsync(() => sink.Count == 1); // never delivered twice
    }

    [Fact]
    public async Task Oversized_payloads_are_rejected_client_side()
    {
        var a = await _cluster.StartNodeAsync(WithRouting);
        var result = await a.Router.RouteAsync("evt", "big", new byte[2 * 1024 * 1024]);
        Assert.Equal(RouteStatus.PayloadTooLarge, result.Status);
    }

    [Fact]
    public async Task Router_throws_when_the_feature_is_disabled()
    {
        var a = await _cluster.StartNodeAsync();
        Assert.Throws<InvalidOperationException>(() => a.Router);
    }

    [Fact]
    public async Task Duplicate_handler_registration_throws_and_disposal_unregisters()
    {
        var a = await _cluster.StartNodeAsync(WithRouting);
        var sink = NewSink();
        var registration = Handle(a, "evt", sink);
        Assert.Throws<InvalidOperationException>(() => Handle(a, "evt", sink));

        await registration.DisposeAsync();
        var result = await a.Router.RouteAsync("evt", "x", new byte[] { 1 });
        Assert.Equal(RouteStatus.NoHandler, result.Status);
    }
}
