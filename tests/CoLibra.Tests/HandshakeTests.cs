using System.Net;
using CoLibra.Security;
using CoLibra.Transport;

namespace CoLibra.Tests;

public class HandshakeTests
{
    private static (InMemoryChannel Client, InMemoryChannel Server) ChannelPair()
    {
        var hub = new InMemoryHub();
        var pair = new InMemoryChannelPair(
            new IPEndPoint(IPAddress.Loopback, 1), new IPEndPoint(IPAddress.Loopback, 2), hub);
        return (pair.Initiated, pair.Accepted);
    }

    [Fact]
    public async Task Mutually_authenticates_with_shared_secret()
    {
        var keys = new ClusterKeys("svc", "secret");
        var clientId = NodeId.NewId();
        var serverId = NodeId.NewId();
        var (client, server) = ChannelPair();
        var ct = TestContext.Current.CancellationToken;

        var serverTask = Handshake.AsServerAsync(server, keys, serverId, 20, ct);
        var clientTask = Handshake.AsClientAsync(client, keys, clientId, 10, ct);

        var seenByServer = await serverTask;
        var seenByClient = await clientTask;
        Assert.Equal(clientId, seenByServer.NodeId);
        Assert.Equal(10, seenByServer.Incarnation);
        Assert.Equal(serverId, seenByClient.NodeId);
        Assert.Equal(20, seenByClient.Incarnation);
    }

    [Fact]
    public async Task Rejects_client_with_wrong_secret()
    {
        var (client, server) = ChannelPair();
        var ct = TestContext.Current.CancellationToken;

        var serverTask = Handshake.AsServerAsync(server, new ClusterKeys("svc", "right"), NodeId.NewId(), 1, ct);
        var clientTask = Handshake.AsClientAsync(client, new ClusterKeys("svc", "wrong"), NodeId.NewId(), 1, ct);

        await Assert.ThrowsAsync<UnauthorizedAccessException>(async () => await serverTask);
        await server.DisposeAsync(); // closes the pair so the waiting client fails fast
        await Assert.ThrowsAnyAsync<Exception>(async () => await clientTask);
    }

    [Fact]
    public async Task Rejects_server_with_wrong_secret()
    {
        var (client, server) = ChannelPair();
        var ct = TestContext.Current.CancellationToken;

        // The server's proof is keyed by a different secret; the client must reject it.
        var serverTask = Handshake.AsServerAsync(server, new ClusterKeys("svc", "impostor"), NodeId.NewId(), 1, ct);
        var clientTask = Handshake.AsClientAsync(client, new ClusterKeys("svc", "real"), NodeId.NewId(), 1, ct);

        await Assert.ThrowsAnyAsync<Exception>(async () => await serverTask);
        await server.DisposeAsync();
        await Assert.ThrowsAnyAsync<Exception>(async () => await clientTask);
    }
}
