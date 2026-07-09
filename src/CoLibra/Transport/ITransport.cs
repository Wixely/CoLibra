using System.Net;
using System.Threading.Channels;
using CoLibra.Protocol;

namespace CoLibra.Transport;

internal readonly record struct ReceivedDatagram(byte[] Payload, IPEndPoint Source);

/// <summary>
/// The node's view of the network: an unreliable signed-datagram side for discovery and a
/// reliable ordered message-channel side for the mesh. Implemented over real sockets
/// (UDP + TLS TCP) in production and in-memory (with scripted partitions) in tests.
/// </summary>
internal interface ITransport : IAsyncDisposable
{
    /// <summary>The advertised mesh endpoint (actual port when OS-assigned).</summary>
    IPEndPoint MeshEndpoint { get; }

    /// <summary>Received discovery datagrams (still enveloped/signed).</summary>
    ChannelReader<ReceivedDatagram> Datagrams { get; }

    /// <summary>Accepted (but not yet authenticated) inbound mesh connections.</summary>
    ChannelReader<IMessageChannel> Inbound { get; }

    ValueTask StartAsync(CancellationToken cancellationToken);

    /// <summary>Sends a discovery datagram: to <paramref name="unicastTarget"/> when given, otherwise multicast (+ broadcast fallback).</summary>
    ValueTask SendDatagramAsync(byte[] datagram, IPEndPoint? unicastTarget, CancellationToken cancellationToken);

    /// <summary>Opens an outbound mesh connection (encrypted, not yet authenticated).</summary>
    ValueTask<IMessageChannel> ConnectAsync(IPEndPoint remote, CancellationToken cancellationToken);
}

/// <summary>A reliable, ordered, full-duplex channel of protocol messages.</summary>
internal interface IMessageChannel : IAsyncDisposable
{
    EndPoint RemoteEndPoint { get; }

    ValueTask SendAsync(Message message, CancellationToken cancellationToken);

    /// <summary>Receives the next message; null when the channel is closed.</summary>
    ValueTask<Message?> ReceiveAsync(CancellationToken cancellationToken);
}
