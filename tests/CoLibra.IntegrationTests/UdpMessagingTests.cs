using System.Collections.Concurrent;
using CoLibra.Messaging.LiteNetLib;
using CoLibra.Runtime;

namespace CoLibra.IntegrationTests;

/// <summary>
/// The control plane runs on the in-memory mesh; the UDP data plane uses REAL loopback sockets
/// via the LiteNetLib engine (in-memory member addresses are 127.0.0.x, which all reach the
/// real listener bound on 0.0.0.0).
/// </summary>
public class UdpMessagingTests(ITestOutputHelper output) : IAsyncLifetime
{
    private readonly TestCluster _cluster = new(output);

    private static Action<CoLibraOptions> WithUdp(string name) => o =>
    {
        o.Messaging.Enabled = true;
        o.Messaging.PreferUdp = true;
        o.Messaging.DeliveryTimeout = TimeSpan.FromSeconds(2 * TestCluster.Scale);
        o.Messaging.LinkHandshakeTimeout = TimeSpan.FromSeconds(2 * TestCluster.Scale);
        o.NodeName = name;
    };

    private Task<CoLibraNode> StartUdpNodeAsync(string name, LiteNetLibEngine? engine = null) =>
        _cluster.StartNodeAsync(WithUdp(name), waitForCluster: true, engine ?? new LiteNetLibEngine());

    public ValueTask InitializeAsync() => ValueTask.CompletedTask;

    public async ValueTask DisposeAsync() => await _cluster.DisposeAsync();

    [Fact]
    public async Task Reliable_messages_travel_the_udp_link_with_acks()
    {
        var a = await StartUdpNodeAsync("a");
        var b = await StartUdpNodeAsync("b");
        var received = new TaskCompletionSource<ReceivedMessage>(TaskCreationOptions.RunContinuationsAsynchronously);
        _ = b.Messenger.RegisterHandler("game", (m, _) =>
        {
            received.TrySetResult(m);
            return ValueTask.CompletedTask;
        });

        var result = await a.Messenger.SendAsync(b.LocalNodeId, "game", new byte[] { 1, 2, 3, 4 });

        Assert.Equal(SendStatus.Delivered, result.Status);
        var message = await received.Task.WaitAsync(TestCluster.Eventually);
        Assert.Equal(new byte[] { 1, 2, 3, 4 }, message.Payload.ToArray());
        Assert.Equal(a.LocalNodeId, message.Origin);
        Assert.Equal("a", message.OriginName);
        Assert.True(await a.HasActiveUdpLinkAsync(b.LocalNodeId), "the send should have established a UDP link");
    }

    [Fact]
    public async Task Sequenced_messages_are_fire_and_forget_and_arrive()
    {
        var a = await StartUdpNodeAsync("a");
        var b = await StartUdpNodeAsync("b");
        var received = new ConcurrentBag<int>();
        _ = b.Messenger.RegisterHandler("positions", (m, _) =>
        {
            received.Add(m.Payload.Span[0]);
            return ValueTask.CompletedTask;
        });

        // Prime the link (first send establishes it; Sequenced may race the handshake).
        Assert.Equal(SendStatus.Delivered,
            (await a.Messenger.SendAsync(b.LocalNodeId, "positions", new byte[] { 0 })).Status);

        for (var i = 1; i <= 20; i++)
        {
            var result = await a.Messenger.SendAsync(b.LocalNodeId, "positions", new[] { (byte)i }, MessageDelivery.Sequenced);
            Assert.Equal(SendStatus.Sent, result.Status);
        }

        // Loopback: no loss expected, but Sequenced only promises the tail arrives.
        await TestCluster.WaitUntilAsync(() => received.Contains(20), because: "the newest sequenced packet should arrive");
    }

    [Fact]
    public async Task Falls_back_to_tcp_when_the_peer_has_no_udp_engine()
    {
        var udpNode = await StartUdpNodeAsync("udp");
        var tcpNode = await _cluster.StartNodeAsync(o =>
        {
            o.Messaging.Enabled = true;
            o.NodeName = "tcp-only";
        });
        var received = new TaskCompletionSource<string?>(TaskCreationOptions.RunContinuationsAsynchronously);
        _ = tcpNode.Messenger.RegisterHandler("game", (m, _) =>
        {
            received.TrySetResult(m.OriginName);
            return ValueTask.CompletedTask;
        });

        var result = await udpNode.Messenger.SendAsync(tcpNode.LocalNodeId, "game", new byte[] { 7 });

        Assert.Equal(SendStatus.Delivered, result.Status);
        Assert.Equal("udp", await received.Task.WaitAsync(TestCluster.Eventually));
        Assert.False(await udpNode.HasActiveUdpLinkAsync(tcpNode.LocalNodeId));
    }

    [Fact]
    public async Task Oversized_payloads_take_the_tcp_path()
    {
        var a = await StartUdpNodeAsync("a");
        var b = await StartUdpNodeAsync("b");
        var received = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
        _ = b.Messenger.RegisterHandler("blob", (m, _) =>
        {
            received.TrySetResult(m.Payload.Length);
            return ValueTask.CompletedTask;
        });

        var big = new byte[64 * 1024]; // over MaxUdpPayloadBytes (8 KiB), under MaxPayloadBytes (1 MiB)
        var result = await a.Messenger.SendAsync(b.LocalNodeId, "blob", big);

        Assert.Equal(SendStatus.Delivered, result.Status);
        Assert.Equal(big.Length, await received.Task.WaitAsync(TestCluster.Eventually));
    }

    [Fact]
    public async Task Reliable_delivery_survives_heavy_packet_loss()
    {
        var lossyEngine = new LiteNetLibEngine { SimulatePacketLossChance = 25 };
        var a = await StartUdpNodeAsync("a", lossyEngine);
        var b = await StartUdpNodeAsync("b");
        var received = new ConcurrentBag<int>();
        _ = b.Messenger.RegisterHandler("fire", (m, _) =>
        {
            received.Add(BitConverter.ToInt32(m.Payload.Span));
            return ValueTask.CompletedTask;
        });

        const int Count = 100;
        for (var i = 0; i < Count; i++)
        {
            var result = await a.Messenger.SendAsync(b.LocalNodeId, "fire", BitConverter.GetBytes(i), MessageDelivery.Reliable);
            Assert.Equal(SendStatus.Delivered, result.Status);
        }

        await TestCluster.WaitUntilAsync(() => received.Count == Count,
            because: "reliable delivery must retransmit through 25% simulated loss");
        Assert.Equal(Enumerable.Range(0, Count), received.Order());
    }

    [Fact]
    public async Task Links_reestablish_after_coordinator_failover()
    {
        var a = await StartUdpNodeAsync("a"); // first up: coordinator
        var b = await StartUdpNodeAsync("b");
        var c = await StartUdpNodeAsync("c");
        var received = new ConcurrentBag<string>();
        _ = c.Messenger.RegisterHandler("game", (m, _) =>
        {
            received.Add($"{m.OriginName}");
            return ValueTask.CompletedTask;
        });

        Assert.Equal(SendStatus.Delivered, (await b.Messenger.SendAsync(c.LocalNodeId, "game", new byte[] { 1 })).Status);
        Assert.True(await b.HasActiveUdpLinkAsync(c.LocalNodeId));

        await _cluster.StopNodeAsync(a); // kill the coordinator: term changes, wire ids re-scope
        await TestCluster.WaitUntilAsync(
            () => TestCluster.CoordinatorOf([b, c]) is not null &&
                  new[] { b, c }.All(n => n.State is ClusterState.Coordinator or ClusterState.Member),
            because: "the survivors should re-form");

        var result = await b.Messenger.SendAsync(c.LocalNodeId, "game", new byte[] { 2 });

        Assert.Equal(SendStatus.Delivered, result.Status);
        await TestCluster.WaitUntilAsync(() => received.Count == 2);
    }
}
