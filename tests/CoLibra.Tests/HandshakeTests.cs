using System.Net;
using CoLibra.Protocol;
using CoLibra.Security;
using CoLibra.Transport;

namespace CoLibra.Tests;

public class HandshakeTests
{
    // Wraps a channel to inject arbitrary TLS cert hashes (the real ones come from SslStream).
    private sealed class BoundChannel(IMessageChannel inner, byte[] local, byte[] remote) : IMessageChannel
    {
        public EndPoint RemoteEndPoint => inner.RemoteEndPoint;
        public byte[] LocalCertificateHash => local;
        public byte[] RemoteCertificateHash => remote;
        public ValueTask SendAsync(Message message, CancellationToken ct) => inner.SendAsync(message, ct);
        public ValueTask<Message?> ReceiveAsync(CancellationToken ct) => inner.ReceiveAsync(ct);
        public ValueTask DisposeAsync() => inner.DisposeAsync();
    }

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
    public async Task Rejects_a_mitm_relaying_across_different_tls_certs()
    {
        // Both ends hold the shared secret (the attacker just relays the proofs), but the client saw
        // the ATTACKER's cert while the server presented its OWN. The channel binding must reject it.
        var keys = new ClusterKeys("svc", "secret");
        var (client, server) = ChannelPair();
        var ct = TestContext.Current.CancellationToken;

        var boundServer = new BoundChannel(server, local: [1, 1, 1], remote: []);   // server presented cert 1,1,1
        var boundClient = new BoundChannel(client, local: [], remote: [9, 9, 9]);   // client saw the MITM's cert

        var serverTask = Handshake.AsServerAsync(boundServer, keys, NodeId.NewId(), 1, ct);
        var clientTask = Handshake.AsClientAsync(boundClient, keys, NodeId.NewId(), 1, ct);

        await Assert.ThrowsAsync<UnauthorizedAccessException>(async () => await serverTask);
        await boundServer.DisposeAsync();
        await Assert.ThrowsAnyAsync<Exception>(async () => await clientTask);
    }

    [Fact]
    public async Task Accepts_a_matching_channel_binding()
    {
        var keys = new ClusterKeys("svc", "secret");
        var (client, server) = ChannelPair();
        var ct = TestContext.Current.CancellationToken;

        // No MITM: the client saw exactly the cert the server presented.
        var boundServer = new BoundChannel(server, local: [7, 7, 7], remote: []);
        var boundClient = new BoundChannel(client, local: [], remote: [7, 7, 7]);

        var serverTask = Handshake.AsServerAsync(boundServer, keys, NodeId.NewId(), 1, ct);
        var clientTask = Handshake.AsClientAsync(boundClient, keys, NodeId.NewId(), 1, ct);
        await serverTask;
        await clientTask;
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
