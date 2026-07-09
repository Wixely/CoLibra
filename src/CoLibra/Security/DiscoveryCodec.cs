using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;
using CoLibra.Protocol;

namespace CoLibra.Security;

/// <summary>
/// Wraps discovery messages in an authenticated datagram:
/// [1B version][1B type][8B unix-ms timestamp][16B nonce][2B sidLen][serviceId][4B bodyLen][body][32B HMAC-SHA256].
/// The HMAC covers everything before it, keyed by the service's discovery key, so packets from
/// other services or clusters (different secret) are dropped before any parsing. A timestamp
/// window plus a nonce cache rejects replays.
/// </summary>
internal sealed class DiscoveryCodec(ClusterKeys keys, string serviceId, TimeProvider timeProvider)
{
    private const int HmacLength = 32;
    private const int NonceLength = 16;
    private static readonly TimeSpan ReplayWindow = TimeSpan.FromSeconds(30);

    private readonly byte[] _serviceIdBytes = Encoding.UTF8.GetBytes(serviceId);
    private readonly HashSet<Guid> _seenNonces = [];
    private readonly Queue<(Guid Nonce, long ExpiresMs)> _nonceExpiry = new();
    private readonly Lock _nonceLock = new();

    public byte[] Encode(Message message)
    {
        var body = CoLibraJsonContext.Resolver.Serialize(message);
        var datagram = new byte[1 + 1 + 8 + NonceLength + 2 + _serviceIdBytes.Length + 4 + body.Length + HmacLength];
        var span = datagram.AsSpan();

        span[0] = ProtocolConstants.ProtocolVersion;
        span[1] = (byte)message.Type;
        BinaryPrimitives.WriteInt64LittleEndian(span[2..], timeProvider.GetUtcNow().ToUnixTimeMilliseconds());
        RandomNumberGenerator.Fill(span.Slice(10, NonceLength));
        BinaryPrimitives.WriteUInt16LittleEndian(span[(10 + NonceLength)..], (ushort)_serviceIdBytes.Length);
        _serviceIdBytes.CopyTo(span[(12 + NonceLength)..]);
        var bodyLenOffset = 12 + NonceLength + _serviceIdBytes.Length;
        BinaryPrimitives.WriteInt32LittleEndian(span[bodyLenOffset..], body.Length);
        body.CopyTo(span[(bodyLenOffset + 4)..]);

        var signed = span[..^HmacLength];
        HMACSHA256.HashData(keys.DiscoveryKey, signed, span[^HmacLength..]);
        return datagram;
    }

    /// <summary>Verifies and unwraps a datagram; returns null for foreign, tampered, or replayed packets.</summary>
    public Message? TryDecode(ReadOnlySpan<byte> datagram)
    {
        if (datagram.Length < 1 + 1 + 8 + NonceLength + 2 + 4 + HmacLength)
            return null;
        if (datagram[0] != ProtocolConstants.ProtocolVersion)
            return null;

        var sidLength = BinaryPrimitives.ReadUInt16LittleEndian(datagram[(10 + NonceLength)..]);
        var bodyLenOffset = 12 + NonceLength + sidLength;
        if (datagram.Length < bodyLenOffset + 4 + HmacLength)
            return null;
        if (!datagram.Slice(12 + NonceLength, sidLength).SequenceEqual(_serviceIdBytes))
            return null;

        var bodyLength = BinaryPrimitives.ReadInt32LittleEndian(datagram[bodyLenOffset..]);
        if (bodyLength < 0 || datagram.Length != bodyLenOffset + 4 + bodyLength + HmacLength)
            return null;

        Span<byte> expected = stackalloc byte[HmacLength];
        HMACSHA256.HashData(keys.DiscoveryKey, datagram[..^HmacLength], expected);
        if (!CryptographicOperations.FixedTimeEquals(expected, datagram[^HmacLength..]))
            return null;

        var timestamp = BinaryPrimitives.ReadInt64LittleEndian(datagram[2..]);
        var nowMs = timeProvider.GetUtcNow().ToUnixTimeMilliseconds();
        if (Math.Abs(nowMs - timestamp) > ReplayWindow.TotalMilliseconds)
            return null;

        var nonce = new Guid(datagram.Slice(10, NonceLength));
        lock (_nonceLock)
        {
            while (_nonceExpiry.TryPeek(out var oldest) && oldest.ExpiresMs < nowMs)
                _seenNonces.Remove(_nonceExpiry.Dequeue().Nonce);
            if (!_seenNonces.Add(nonce))
                return null;
            _nonceExpiry.Enqueue((nonce, nowMs + (long)ReplayWindow.TotalMilliseconds));
        }

        return CoLibraJsonContext.Resolver.Deserialize(
            (MessageType)datagram[1],
            datagram.Slice(bodyLenOffset + 4, bodyLength));
    }
}
