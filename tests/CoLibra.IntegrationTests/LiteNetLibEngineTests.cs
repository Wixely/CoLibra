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
