using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Authentication;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Channels;
using CoLibra.Protocol;
using Microsoft.Extensions.Logging;

namespace CoLibra.Transport;

/// <summary>
/// Production transport: IPv4 UDP (multicast + optional broadcast + unicast) for discovery,
/// and TLS 1.3 over TCP for the mesh. Certificate validation accepts any certificate — TLS
/// provides confidentiality; authentication is the protocol-level shared-secret handshake.
/// </summary>
internal sealed class SocketTransport : ITransport
{
    private readonly CoLibraOptions _options;
    private readonly X509Certificate2 _certificate;
    private readonly ILogger _logger;
    private readonly IPAddress _multicastAddress;
    // Bounds inbound work so an unauthenticated peer (TLS accepts any cert; the shared-secret
    // handshake happens later) cannot exhaust memory/threads by flooding connections. Concurrent
    // TLS handshakes are throttled and the queue of TLS-established-but-not-yet-authenticated
    // channels is bounded; excess connections are shed and retried by the peer's discovery.
    private const int MaxConcurrentInboundHandshakes = 128;

    private readonly Channel<ReceivedDatagram> _datagrams = Channel.CreateBounded<ReceivedDatagram>(
        new BoundedChannelOptions(1024) { FullMode = BoundedChannelFullMode.DropOldest });
    private readonly Channel<IMessageChannel> _inbound = Channel.CreateBounded<IMessageChannel>(
        new BoundedChannelOptions(256) { FullMode = BoundedChannelFullMode.Wait });
    private readonly SemaphoreSlim _inboundHandshakes = new(MaxConcurrentInboundHandshakes);
    private readonly CancellationTokenSource _stopping = new();

    private Socket? _udpSocket;
    private TcpListener? _tcpListener;
    private IPEndPoint? _meshEndpoint;

    public SocketTransport(CoLibraOptions options, X509Certificate2 certificate, ILogger logger)
    {
        _options = options;
        _certificate = certificate;
        _logger = logger;
        _multicastAddress = IPAddress.Parse(options.MulticastAddress);
    }

    public IPEndPoint MeshEndpoint => _meshEndpoint
        ?? throw new InvalidOperationException("Transport not started.");

    public ChannelReader<ReceivedDatagram> Datagrams => _datagrams.Reader;

    public ChannelReader<IMessageChannel> Inbound => _inbound.Reader;

    public ValueTask StartAsync(CancellationToken cancellationToken)
    {
        _udpSocket = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);
        _udpSocket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);
        _udpSocket.Bind(new IPEndPoint(IPAddress.Any, _options.DiscoveryPort));
        if (_options.EnableMulticast)
        {
            _udpSocket.SetSocketOption(SocketOptionLevel.IP, SocketOptionName.AddMembership,
                new MulticastOption(_multicastAddress, IPAddress.Any));
            _udpSocket.SetSocketOption(SocketOptionLevel.IP, SocketOptionName.MulticastLoopback, true);
        }

        if (_options.EnableBroadcastFallback)
            _udpSocket.EnableBroadcast = true;

        _tcpListener = new TcpListener(IPAddress.Any, _options.MeshPort);
        _tcpListener.Start();
        var actualPort = ((IPEndPoint)_tcpListener.LocalEndpoint).Port;
        _meshEndpoint = new IPEndPoint(IPAddress.Loopback, actualPort);

        _ = PumpDatagramsAsync(_stopping.Token);
        _ = PumpAcceptsAsync(_stopping.Token);
        return ValueTask.CompletedTask;
    }

    public async ValueTask SendDatagramAsync(byte[] datagram, IPEndPoint? unicastTarget, CancellationToken cancellationToken)
    {
        var socket = _udpSocket ?? throw new InvalidOperationException("Transport not started.");
        if (unicastTarget is not null)
        {
            await socket.SendToAsync(datagram, SocketFlags.None, unicastTarget, cancellationToken).ConfigureAwait(false);
            return;
        }

        if (_options.EnableMulticast)
        {
            await socket.SendToAsync(datagram, SocketFlags.None,
                new IPEndPoint(_multicastAddress, _options.DiscoveryPort), cancellationToken).ConfigureAwait(false);
        }

        if (_options.EnableBroadcastFallback)
        {
            await socket.SendToAsync(datagram, SocketFlags.None,
                new IPEndPoint(IPAddress.Broadcast, _options.DiscoveryPort), cancellationToken).ConfigureAwait(false);
        }
    }

    public async ValueTask<IMessageChannel> ConnectAsync(IPEndPoint remote, CancellationToken cancellationToken)
    {
        var client = new TcpClient(AddressFamily.InterNetwork) { NoDelay = true };
        try
        {
            await client.ConnectAsync(remote, cancellationToken).ConfigureAwait(false);
            var ssl = new SslStream(client.GetStream(), leaveInnerStreamOpen: false);
            await ssl.AuthenticateAsClientAsync(new SslClientAuthenticationOptions
            {
                TargetHost = "colibra",
                EnabledSslProtocols = SslProtocols.Tls13 | SslProtocols.Tls12,
                RemoteCertificateValidationCallback = static (_, _, _, _) => true,
                ClientCertificates = [_certificate],
            }, cancellationToken).ConfigureAwait(false);
            return new FramedMessageChannel(ssl, remote);
        }
        catch
        {
            client.Dispose();
            throw;
        }
    }

    private async Task PumpDatagramsAsync(CancellationToken ct)
    {
        var socket = _udpSocket!;
        var buffer = new byte[64 * 1024];
        EndPoint any = new IPEndPoint(IPAddress.Any, 0);
        while (!ct.IsCancellationRequested)
        {
            try
            {
                var result = await socket.ReceiveFromAsync(buffer, SocketFlags.None, any, ct).ConfigureAwait(false);
                var payload = buffer.AsSpan(0, result.ReceivedBytes).ToArray();
                _datagrams.Writer.TryWrite(new ReceivedDatagram(payload, (IPEndPoint)result.RemoteEndPoint));
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (SocketException ex)
            {
                _logger.LogDebug(ex, "Discovery socket receive error");
            }
        }
    }

    private async Task PumpAcceptsAsync(CancellationToken ct)
    {
        var listener = _tcpListener!;
        while (!ct.IsCancellationRequested)
        {
            TcpClient client;
            try
            {
                client = await listener.AcceptTcpClientAsync(ct).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (SocketException ex)
            {
                _logger.LogDebug(ex, "Mesh accept error");
                continue;
            }

            // Shed the connection if we are already at the concurrent-handshake cap, rather than
            // piling on unbounded TLS negotiations. A legitimate peer just retries via discovery.
            if (!_inboundHandshakes.Wait(0, ct))
            {
                _logger.LogDebug("Inbound handshake cap reached; dropping connection from {Remote}",
                    client.Client.RemoteEndPoint);
                client.Dispose();
                continue;
            }

            _ = SecureInboundAsync(client, ct);
        }
    }

    private async Task SecureInboundAsync(TcpClient client, CancellationToken ct)
    {
        try
        {
            client.NoDelay = true;
            var remote = (IPEndPoint)client.Client.RemoteEndPoint!;
            var ssl = new SslStream(client.GetStream(), leaveInnerStreamOpen: false);
            using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
            timeout.CancelAfter(TimeSpan.FromSeconds(10));
            await ssl.AuthenticateAsServerAsync(new SslServerAuthenticationOptions
            {
                ServerCertificate = _certificate,
                EnabledSslProtocols = SslProtocols.Tls13 | SslProtocols.Tls12,
                ClientCertificateRequired = false,
                RemoteCertificateValidationCallback = static (_, _, _, _) => true,
            }, timeout.Token).ConfigureAwait(false);
            if (!_inbound.Writer.TryWrite(new FramedMessageChannel(ssl, remote)))
            {
                // Queue full (consumer backpressured): shed rather than block the accept path.
                _logger.LogDebug("Inbound queue full; dropping connection from {Remote}", remote);
                await ssl.DisposeAsync().ConfigureAwait(false);
                client.Dispose();
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException || !ct.IsCancellationRequested)
        {
            _logger.LogDebug(ex, "Inbound TLS negotiation failed");
            client.Dispose();
        }
        finally
        {
            _inboundHandshakes.Release();
        }
    }

    public ValueTask DisposeAsync()
    {
        _stopping.Cancel();
        _tcpListener?.Stop();
        _udpSocket?.Dispose();
        _datagrams.Writer.TryComplete();
        _inbound.Writer.TryComplete();
        _inboundHandshakes.Dispose();
        _stopping.Dispose();
        return ValueTask.CompletedTask;
    }
}

/// <summary>Frames protocol messages over a stream, serializing concurrent writers.</summary>
internal sealed class FramedMessageChannel(Stream stream, EndPoint remoteEndPoint) : IMessageChannel
{
    private readonly SemaphoreSlim _writeLock = new(1, 1);

    public EndPoint RemoteEndPoint { get; } = remoteEndPoint;

    public byte[] LocalCertificateHash { get; } =
        (stream as SslStream)?.LocalCertificate?.GetCertHash(HashAlgorithmName.SHA256) ?? [];

    public byte[] RemoteCertificateHash { get; } =
        (stream as SslStream)?.RemoteCertificate?.GetCertHash(HashAlgorithmName.SHA256) ?? [];

    public async ValueTask SendAsync(Message message, CancellationToken cancellationToken)
    {
        await _writeLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await FrameCodec.WriteAsync(stream, message, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _writeLock.Release();
        }
    }

    public async ValueTask<Message?> ReceiveAsync(CancellationToken cancellationToken)
    {
        try
        {
            return await FrameCodec.ReadAsync(stream, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is IOException or EndOfStreamException or ObjectDisposedException
            or InvalidDataException or System.Text.Json.JsonException)
        {
            // A malformed or unreadable frame poisons only this connection (return null closes it),
            // never the read pump — including bad JSON from an authenticated peer with a stale DTO.
            return null;
        }
    }

    public ValueTask DisposeAsync()
    {
        stream.Dispose();
        _writeLock.Dispose();
        return ValueTask.CompletedTask;
    }
}
