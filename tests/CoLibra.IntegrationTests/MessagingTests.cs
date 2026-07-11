using System.Collections.Concurrent;
using CoLibra.Runtime;

namespace CoLibra.IntegrationTests;

public class MessagingTests(ITestOutputHelper output) : IAsyncLifetime
{
    private readonly TestCluster _cluster = new(output);

    private static Action<CoLibraOptions> WithMessaging(string? name = null) => o =>
    {
        o.Messaging.Enabled = true;
        o.Messaging.DeliveryTimeout = TimeSpan.FromSeconds(2 * TestCluster.Scale);
        o.NodeName = name;
    };

    public ValueTask InitializeAsync() => ValueTask.CompletedTask;

    public async ValueTask DisposeAsync() => await _cluster.DisposeAsync();

    [Fact]
    public async Task Sends_bytes_to_a_node_by_id()
    {
        var a = await _cluster.StartNodeAsync(WithMessaging("alice"));
        var b = await _cluster.StartNodeAsync(WithMessaging("bob"));
        var received = new TaskCompletionSource<ReceivedMessage>(TaskCreationOptions.RunContinuationsAsynchronously);
        _ = b.Messenger.RegisterHandler("chat", (message, _) =>
        {
            received.TrySetResult(message);
            return ValueTask.CompletedTask;
        });

        var result = await a.Messenger.SendAsync(b.LocalNodeId, "chat", new byte[] { 1, 2, 3 });

        Assert.Equal(SendStatus.Delivered, result.Status);
        var message = await received.Task.WaitAsync(TestCluster.Eventually);
        Assert.Equal(new byte[] { 1, 2, 3 }, message.Payload.ToArray());
        Assert.Equal(a.LocalNodeId, message.Origin);
        Assert.Equal("alice", message.OriginName);
        Assert.Equal("chat", message.Channel);
    }

    [Fact]
    public async Task Member_list_carries_names_for_addressing()
    {
        var a = await _cluster.StartNodeAsync(WithMessaging("alice"));
        var b = await _cluster.StartNodeAsync(WithMessaging("bob"));

        await TestCluster.WaitUntilAsync(() =>
            a.Members.Any(m => m.Name == "bob") && b.Members.Any(m => m.Name == "alice"),
            because: "names should replicate through membership");
        Assert.Equal(b.LocalNodeId, a.Members.Single(m => m.Name == "bob").NodeId);
    }

    [Fact]
    public async Task Sends_typed_payloads_by_name_to_all_matches()
    {
        var a = await _cluster.StartNodeAsync(WithMessaging("sender"));
        var b = await _cluster.StartNodeAsync(WithMessaging("worker"));
        var c = await _cluster.StartNodeAsync(WithMessaging("worker"));
        var hits = new ConcurrentBag<NodeId>();
        foreach (var node in new[] { b, c })
        {
            _ = node.Messenger.RegisterHandler<string>("notify", (message, _) =>
            {
                Assert.Equal("hello workers", message.Value);
                hits.Add(node.LocalNodeId);
                return ValueTask.CompletedTask;
            });
        }

        await TestCluster.WaitUntilAsync(() => a.Members.Count(m => m.Name == "worker") == 2);
        var results = await a.Messenger.SendByNameAsync("worker", "notify", "hello workers");

        Assert.Equal(2, results.Count);
        Assert.All(results, r => Assert.Equal(SendStatus.Delivered, r.Status));
        await TestCluster.WaitUntilAsync(() => hits.Count == 2);
        Assert.Contains(b.LocalNodeId, hits); // set comparison: Guid ordering is unrelated to start order
        Assert.Contains(c.LocalNodeId, hits);
    }

    [Fact]
    public async Task SendByName_returns_empty_for_unknown_names()
    {
        var a = await _cluster.StartNodeAsync(WithMessaging("alice"));
        var results = await a.Messenger.SendByNameAsync("nobody", "chat", new byte[] { 1 });
        Assert.Empty(results);
    }

    [Fact]
    public async Task Send_to_unknown_id_reports_UnknownTarget()
    {
        var a = await _cluster.StartNodeAsync(WithMessaging());
        var result = await a.Messenger.SendAsync(NodeId.NewId(), "chat", new byte[] { 1 });
        Assert.Equal(SendStatus.UnknownTarget, result.Status);
    }

    [Fact]
    public async Task Send_without_a_handler_reports_NoHandler()
    {
        var a = await _cluster.StartNodeAsync(WithMessaging());
        var b = await _cluster.StartNodeAsync(WithMessaging());

        var result = await a.Messenger.SendAsync(b.LocalNodeId, "nobody-listens", new byte[] { 1 });

        Assert.Equal(SendStatus.NoHandler, result.Status);
    }

    [Fact]
    public async Task Self_send_delivers_in_process()
    {
        var a = await _cluster.StartNodeAsync(WithMessaging("solo"));
        var received = new TaskCompletionSource<ReceivedMessage>(TaskCreationOptions.RunContinuationsAsynchronously);
        _ = a.Messenger.RegisterHandler("loop", (message, _) =>
        {
            received.TrySetResult(message);
            return ValueTask.CompletedTask;
        });

        var result = await a.Messenger.SendAsync(a.LocalNodeId, "loop", new byte[] { 42 });

        Assert.Equal(SendStatus.Delivered, result.Status);
        var message = await received.Task.WaitAsync(TestCluster.Eventually);
        Assert.Equal("solo", message.OriginName);
    }

    [Fact]
    public async Task Member_to_member_works_via_coordinator_relay()
    {
        var a = await _cluster.StartNodeAsync(o => { WithMessaging("coord")(o); o.Messaging.UseDirectChannels = false; });
        var b = await _cluster.StartNodeAsync(o => { WithMessaging("b")(o); o.Messaging.UseDirectChannels = false; });
        var c = await _cluster.StartNodeAsync(o => { WithMessaging("c")(o); o.Messaging.UseDirectChannels = false; });
        Assert.Equal(ClusterState.Coordinator, a.State);
        var received = new TaskCompletionSource<ReceivedMessage>(TaskCreationOptions.RunContinuationsAsynchronously);
        _ = c.Messenger.RegisterHandler("chat", (message, _) =>
        {
            received.TrySetResult(message);
            return ValueTask.CompletedTask;
        });

        // b (a member) learns of c via a pushed membership update; wait for it before sending.
        await TestCluster.WaitUntilAsync(() => b.Members.Any(m => m.NodeId == c.LocalNodeId));
        var result = await b.Messenger.SendAsync(c.LocalNodeId, "chat", new byte[] { 9, 9 });

        Assert.Equal(SendStatus.Delivered, result.Status);
        var message = await received.Task.WaitAsync(TestCluster.Eventually);
        Assert.Equal(new byte[] { 9, 9 }, message.Payload.ToArray());
        Assert.Equal(b.LocalNodeId, message.Origin);
    }

    [Fact]
    public async Task Broadcast_reaches_every_node_except_the_sender()
    {
        var a = await _cluster.StartNodeAsync(WithMessaging("a"));
        var b = await _cluster.StartNodeAsync(WithMessaging("b"));
        var c = await _cluster.StartNodeAsync(WithMessaging("c"));
        var hits = new ConcurrentBag<NodeId>();
        var selfHit = false;
        foreach (var node in new[] { b, c })
        {
            _ = node.Messenger.RegisterHandler<string>("news", (m, _) =>
            {
                Assert.Equal("hello all", m.Value);
                hits.Add(node.LocalNodeId);
                return ValueTask.CompletedTask;
            });
        }

        _ = a.Messenger.RegisterHandler<string>("news", (_, _) => { selfHit = true; return ValueTask.CompletedTask; });
        await TestCluster.WaitUntilAsync(() => a.Members.Count == 3);
        var results = await a.Messenger.BroadcastAsync("news", "hello all");

        Assert.Equal(2, results.Count); // b and c, not a
        Assert.All(results, r => Assert.Equal(SendStatus.Delivered, r.Status));
        await TestCluster.WaitUntilAsync(() => hits.Count == 2);
        Assert.Contains(b.LocalNodeId, hits); // set comparison: Guid ordering is unrelated to start order
        Assert.Contains(c.LocalNodeId, hits);
        Assert.False(selfHit, "broadcast must not deliver to the sender");
    }

    [Fact]
    public async Task Broadcast_from_a_lone_node_returns_empty()
    {
        var a = await _cluster.StartNodeAsync(WithMessaging("solo"));
        var results = await a.Messenger.BroadcastAsync("news", new byte[] { 1 });
        Assert.Empty(results);
    }

    [Fact]
    public async Task Messenger_throws_when_the_feature_is_disabled()
    {
        var a = await _cluster.StartNodeAsync();
        Assert.Throws<InvalidOperationException>(() => a.Messenger);
    }

    [Fact]
    public async Task Oversized_messages_are_rejected_client_side()
    {
        var a = await _cluster.StartNodeAsync(WithMessaging());
        var result = await a.Messenger.SendAsync(a.LocalNodeId, "chat", new byte[2 * 1024 * 1024]);
        Assert.Equal(SendStatus.PayloadTooLarge, result.Status);
    }

    [Fact]
    public async Task Messaging_works_with_routing_disabled()
    {
        // Messaging must not depend on the routing feature being enabled.
        var a = await _cluster.StartNodeAsync(WithMessaging("a"));
        var b = await _cluster.StartNodeAsync(WithMessaging("b"));
        var received = new TaskCompletionSource<string?>(TaskCreationOptions.RunContinuationsAsynchronously);
        _ = b.Messenger.RegisterHandler("ping", (message, _) =>
        {
            received.TrySetResult(message.OriginName);
            return ValueTask.CompletedTask;
        });

        var result = await a.Messenger.SendAsync(b.LocalNodeId, "ping", new byte[] { 1 });

        Assert.Equal(SendStatus.Delivered, result.Status);
        Assert.Equal("a", await received.Task.WaitAsync(TestCluster.Eventually));
        Assert.Throws<InvalidOperationException>(() => a.Router); // routing genuinely off
    }
}
