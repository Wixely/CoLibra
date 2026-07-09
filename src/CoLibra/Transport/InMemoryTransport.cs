using System.Collections.Concurrent;
using System.Net;
using System.Threading.Channels;
using CoLibra.Protocol;

namespace CoLibra.Transport;

/// <summary>
/// A network fabric shared by in-process test nodes. Supports scripted partitions:
/// nodes in different partition groups can neither exchange datagrams nor keep
/// mesh connections alive, and healing reconnects the fabric.
/// </summary>
internal sealed class InMemoryHub
{
    private readonly ConcurrentDictionary<IPEndPoint, InMemoryTransport> _nodes = new();
    private readonly ConcurrentDictionary<InMemoryChannelPair, byte> _connections = new();
    private volatile Dictionary<IPEndPoint, int>? _partitionGroups;
    private int _nextHost = 1;

    public InMemoryTransport CreateTransport()
    {
        var endpoint = new IPEndPoint(new IPAddress([127, 0, 0, (byte)Interlocked.Increment(ref _nextHost)]), 41101);
        var transport = new InMemoryTransport(this, endpoint);
        _nodes[endpoint] = transport;
        return transport;
    }

    public void Remove(IPEndPoint endpoint) => _nodes.TryRemove(endpoint, out _);

    /// <summary>Splits the fabric into isolated groups, dropping mesh connections that cross groups.</summary>
    public void Partition(params IReadOnlyList<IPEndPoint>[] groups)
    {
        var map = new Dictionary<IPEndPoint, int>();
        for (var i = 0; i < groups.Length; i++)
        {
            foreach (var endpoint in groups[i])
                map[endpoint] = i;
        }

        _partitionGroups = map;
        foreach (var pair in _connections.Keys)
        {
            if (!CanTalk(pair.EndpointA, pair.EndpointB))
                pair.Sever();
        }
    }

    public void Heal() => _partitionGroups = null;

    public bool CanTalk(IPEndPoint a, IPEndPoint b)
    {
        var groups = _partitionGroups;
        if (groups is null)
            return true;
        var groupA = groups.TryGetValue(a, out var ga) ? ga : -1;
        var groupB = groups.TryGetValue(b, out var gb) ? gb : -1;
        return groupA == groupB;
    }

    public void Multicast(byte[] datagram, IPEndPoint source)
    {
        foreach (var (endpoint, node) in _nodes)
        {
            if (CanTalk(source, endpoint))
                node.DeliverDatagram(datagram, source);
        }
    }

    public bool Unicast(byte[] datagram, IPEndPoint source, IPEndPoint target)
    {
        if (!_nodes.TryGetValue(target, out var node) || !CanTalk(source, target))
            return false;
        node.DeliverDatagram(datagram, source);
        return true;
    }

    public InMemoryChannelPair Connect(IPEndPoint source, IPEndPoint target)
    {
        if (!_nodes.TryGetValue(target, out var node) || !CanTalk(source, target))
            throw new IOException($"In-memory connect refused: {source} -> {target}");
        var pair = new InMemoryChannelPair(source, target, this);
        _connections[pair] = 0;
        node.DeliverConnection(pair.Accepted);
        return pair;
    }

    public void Forget(InMemoryChannelPair pair) => _connections.TryRemove(pair, out _);
}

internal sealed class InMemoryTransport(InMemoryHub hub, IPEndPoint endpoint) : ITransport
{
    private readonly Channel<ReceivedDatagram> _datagrams = Channel.CreateUnbounded<ReceivedDatagram>();
    private readonly Channel<IMessageChannel> _inbound = Channel.CreateUnbounded<IMessageChannel>();
    private volatile bool _stopped;

    public InMemoryHub Hub => hub;

    public IPEndPoint MeshEndpoint { get; } = endpoint;

    public ChannelReader<ReceivedDatagram> Datagrams => _datagrams.Reader;

    public ChannelReader<IMessageChannel> Inbound => _inbound.Reader;

    public ValueTask StartAsync(CancellationToken cancellationToken) => ValueTask.CompletedTask;

    public ValueTask SendDatagramAsync(byte[] datagram, IPEndPoint? unicastTarget, CancellationToken cancellationToken)
    {
        if (unicastTarget is not null)
            hub.Unicast(datagram, MeshEndpoint, unicastTarget);
        else
            hub.Multicast(datagram, MeshEndpoint);
        return ValueTask.CompletedTask;
    }

    public ValueTask<IMessageChannel> ConnectAsync(IPEndPoint remote, CancellationToken cancellationToken) =>
        ValueTask.FromResult<IMessageChannel>(hub.Connect(MeshEndpoint, remote).Initiated);

    public void DeliverDatagram(byte[] datagram, IPEndPoint source)
    {
        if (!_stopped)
            _datagrams.Writer.TryWrite(new ReceivedDatagram(datagram, source));
    }

    public void DeliverConnection(IMessageChannel channel)
    {
        if (!_stopped)
            _inbound.Writer.TryWrite(channel);
    }

    public ValueTask DisposeAsync()
    {
        _stopped = true;
        hub.Remove(MeshEndpoint);
        _datagrams.Writer.TryComplete();
        _inbound.Writer.TryComplete();
        return ValueTask.CompletedTask;
    }
}

/// <summary>Two ends of an in-memory mesh connection. Messages round-trip through the codec for wire fidelity.</summary>
internal sealed class InMemoryChannelPair
{
    public IPEndPoint EndpointA { get; }
    public IPEndPoint EndpointB { get; }
    public InMemoryChannel Initiated { get; }
    public InMemoryChannel Accepted { get; }

    public InMemoryChannelPair(IPEndPoint initiator, IPEndPoint acceptor, InMemoryHub hub)
    {
        EndpointA = initiator;
        EndpointB = acceptor;
        var aToB = Channel.CreateUnbounded<Message>();
        var bToA = Channel.CreateUnbounded<Message>();
        Initiated = new InMemoryChannel(acceptor, aToB.Writer, bToA.Reader, () => Sever(hub));
        Accepted = new InMemoryChannel(initiator, bToA.Writer, aToB.Reader, () => Sever(hub));
    }

    public void Sever()
    {
        Initiated.Close();
        Accepted.Close();
    }

    private void Sever(InMemoryHub hub)
    {
        Sever();
        hub.Forget(this);
    }
}

internal sealed class InMemoryChannel(
    IPEndPoint remoteEndPoint,
    ChannelWriter<Message> outgoing,
    ChannelReader<Message> incoming,
    Action onClose) : IMessageChannel
{
    public EndPoint RemoteEndPoint { get; } = remoteEndPoint;

    public ValueTask SendAsync(Message message, CancellationToken cancellationToken)
    {
        // Round-trip through the serializer so in-memory tests exercise the real wire shapes.
        var payload = CoLibraJsonContext.Resolver.Serialize(message);
        var copy = CoLibraJsonContext.Resolver.Deserialize(message.Type, payload)
            ?? throw new InvalidDataException($"Message {message.Type} did not round-trip.");
        return outgoing.TryWrite(copy)
            ? ValueTask.CompletedTask
            : throw new IOException("In-memory channel closed.");
    }

    public async ValueTask<Message?> ReceiveAsync(CancellationToken cancellationToken)
    {
        try
        {
            return await incoming.ReadAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (ChannelClosedException)
        {
            return null;
        }
    }

    public void Close()
    {
        outgoing.TryComplete();
    }

    public ValueTask DisposeAsync()
    {
        onClose();
        return ValueTask.CompletedTask;
    }
}
