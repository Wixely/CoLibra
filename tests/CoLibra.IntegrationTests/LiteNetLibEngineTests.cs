using System.Net;
using CoLibra.Messaging.LiteNetLib;

namespace CoLibra.IntegrationTests;

/// <summary>Engine-level smoke tests over real loopback UDP, independent of the CoLibra handshake.</summary>
public class LiteNetLibEngineTests : IAsyncLifetime
{
    private readonly LiteNetLibEngine _server = new();
    private readonly LiteNetLibEngine _client = new();

    public ValueTask InitializeAsync() => ValueTask.CompletedTask;

    public async ValueTask DisposeAsync()
    {
        await _server.DisposeAsync();
        await _client.DisposeAsync();
    }

    [Fact]
    public async Task Connects_with_key_validation_and_moves_datagrams_both_ways()
    {
        var serverReceived = new TaskCompletionSource<byte[]>(TaskCreationOptions.RunContinuationsAsynchronously);
        var serverPeerSeen = new TaskCompletionSource<IUdpPeer>(TaskCreationOptions.RunContinuationsAsynchronously);
        var serverPort = await _server.StartAsync(new UdpEngineCallbacks
        {
            AcceptConnection = key => key == "good-key" ? "tag" : null,
            DatagramReceived = (peer, data) =>
            {
                serverPeerSeen.TrySetResult(peer);
                serverReceived.TrySetResult(data.ToArray());
            },
            PeerDisconnected = _ => { },
        }, 0, TestContext.Current.CancellationToken);

        var clientReceived = new TaskCompletionSource<byte[]>(TaskCreationOptions.RunContinuationsAsynchronously);
        await _client.StartAsync(new UdpEngineCallbacks
        {
            AcceptConnection = _ => null,
            DatagramReceived = (_, data) => clientReceived.TrySetResult(data.ToArray()),
            PeerDisconnected = _ => { },
        }, 0, TestContext.Current.CancellationToken);

        var peer = await _client.ConnectAsync(
            new IPEndPoint(IPAddress.Loopback, serverPort), "good-key", TimeSpan.FromSeconds(5),
            TestContext.Current.CancellationToken);

        Assert.NotNull(peer);
        await peer.SendAsync(MessageDelivery.ReliableOrdered, new byte[] { 1, 2, 3 });
        Assert.Equal([1, 2, 3], await serverReceived.Task.WaitAsync(TimeSpan.FromSeconds(5)));

        // The server replies on the peer it saw; the tag survives.
        var serverPeer = await serverPeerSeen.Task;
        Assert.Equal("tag", serverPeer.Tag);
        await serverPeer.SendAsync(MessageDelivery.ReliableOrdered, new byte[] { 9 });
        Assert.Equal([9], await clientReceived.Task.WaitAsync(TimeSpan.FromSeconds(5)));
    }

    [Fact]
    public async Task Nat_punch_trio_introduces_and_connects()
    {
        // master pairs introduce requests by token; host and client punch, then client connects.
        await using var master = new LiteNetLibEngine();
        await using var host = new LiteNetLibEngine();

        var pending = new System.Collections.Concurrent.ConcurrentDictionary<string, (IPEndPoint Local, IPEndPoint Remote)>();
        master.EnableNatPunch(new NatPunchCallbacks
        {
            IntroductionRequested = (local, remote, token) =>
            {
                if (pending.TryRemove(token, out var first) && !first.Remote.Equals(remote))
                    master.Introduce(first.Local, first.Remote, local, remote, token);
                else
                    pending[token] = (local, remote);
            },
            IntroductionSucceeded = (_, _, _) => { },
        });
        var masterPort = await master.StartAsync(NoopCallbacks(), 0, TestContext.Current.CancellationToken);

        var hostReceived = new TaskCompletionSource<byte[]>(TaskCreationOptions.RunContinuationsAsynchronously);
        host.EnableNatPunch(new NatPunchCallbacks
        {
            IntroductionRequested = (_, _, _) => { },
            IntroductionSucceeded = (_, _, _) => { }, // host side: mapping opened; waits for inbound
        });
        await host.StartAsync(new UdpEngineCallbacks
        {
            AcceptConnection = key => key == "punched-key" ? "tag" : null,
            DatagramReceived = (_, data) => hostReceived.TrySetResult(data.ToArray()),
            PeerDisconnected = _ => { },
        }, 0, TestContext.Current.CancellationToken);

        var clientTarget = new TaskCompletionSource<IPEndPoint>(TaskCreationOptions.RunContinuationsAsynchronously);
        _client.EnableNatPunch(new NatPunchCallbacks
        {
            IntroductionRequested = (_, _, _) => { },
            IntroductionSucceeded = (target, _, _) => clientTarget.TrySetResult(target),
        });
        await _client.StartAsync(NoopCallbacks(), 0, TestContext.Current.CancellationToken);

        var masterEndpoint = new IPEndPoint(IPAddress.Loopback, masterPort);
        host.SendIntroduceRequest(masterEndpoint, "tok1");
        _client.SendIntroduceRequest(masterEndpoint, "tok1");

        var target = await clientTarget.Task.WaitAsync(TimeSpan.FromSeconds(5));
        var peer = await _client.ConnectAsync(target, "punched-key", TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

        Assert.NotNull(peer);
        await peer.SendAsync(MessageDelivery.ReliableOrdered, new byte[] { 42 });
        Assert.Equal([42], await hostReceived.Task.WaitAsync(TimeSpan.FromSeconds(5)));
    }

    private static UdpEngineCallbacks NoopCallbacks() => new()
    {
        AcceptConnection = _ => null,
        DatagramReceived = (_, _) => { },
        PeerDisconnected = _ => { },
    };

    [Fact]
    public async Task Rejects_wrong_connection_keys()
    {
        var serverPort = await _server.StartAsync(new UdpEngineCallbacks
        {
            AcceptConnection = key => key == "good-key" ? "tag" : null,
            DatagramReceived = (_, _) => { },
            PeerDisconnected = _ => { },
        }, 0, TestContext.Current.CancellationToken);
        await _client.StartAsync(new UdpEngineCallbacks
        {
            AcceptConnection = _ => null,
            DatagramReceived = (_, _) => { },
            PeerDisconnected = _ => { },
        }, 0, TestContext.Current.CancellationToken);

        var peer = await _client.ConnectAsync(
            new IPEndPoint(IPAddress.Loopback, serverPort), "wrong-key", TimeSpan.FromSeconds(3),
            TestContext.Current.CancellationToken);

        Assert.Null(peer);
    }
}
