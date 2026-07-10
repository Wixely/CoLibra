using System.Buffers.Binary;
using System.Security.Cryptography;

namespace CoLibra.Security;

/// <summary>
/// Per-link authenticated encryption for the UDP data plane. Keys are derived from the cluster
/// secret plus both handshake nonces (exchanged over the TLS mesh), with separate keys per
/// direction so the two sides' nonce counters can never collide. Datagram layout:
/// [ver:1][flags:1][termLow:2][srcWireId:2][channelId:1][counter:8] (plaintext, authenticated
/// as AAD) followed by AES-256-GCM ciphertext + 16-byte tag.
/// </summary>
internal sealed class UdpLinkCrypto : IDisposable
{
    public const byte Version = 1;
    public const int HeaderLength = 1 + 1 + 2 + 2 + 1 + 8; // 15
    public const int TagLength = 16;
    public const int Overhead = HeaderLength + TagLength;

    public const byte FlagAck = 0b0000_0001;
    public const byte FlagWantAck = 0b0000_0010;

    /// <summary>Reserved channel id for link-control frames (app-level acks).</summary>
    public const byte ControlChannel = 0;

    private readonly AesGcm _sendAes;
    private readonly AesGcm _receiveAes;
    private readonly byte[] _sendSalt;
    private readonly byte[] _receiveSalt;
    private long _sendCounter;
    private readonly ReplayWindow _replay = new();

    private UdpLinkCrypto(byte[] sendKey, byte[] receiveKey, byte[] sendSalt, byte[] receiveSalt)
    {
        _sendAes = new AesGcm(sendKey, TagLength);
        _receiveAes = new AesGcm(receiveKey, TagLength);
        _sendSalt = sendSalt;
        _receiveSalt = receiveSalt;
    }

    /// <summary>
    /// Derives the link's directional keys. Both sides call this with the same inputs and get
    /// mirrored send/receive keys (the offerer sends with the "A" key, the acceptor with "B").
    /// </summary>
    public static UdpLinkCrypto Derive(
        ClusterKeys keys, Guid linkId, byte[] offerNonce, byte[] acceptNonce, long term, bool isOfferer)
    {
        var salt = new byte[16 + offerNonce.Length + acceptNonce.Length];
        linkId.TryWriteBytes(salt);
        offerNonce.CopyTo(salt, 16);
        acceptNonce.CopyTo(salt, 16 + offerNonce.Length);

        var info = new byte[8];
        BinaryPrimitives.WriteInt64LittleEndian(info, term);

        // 32B key + 4B nonce salt per direction.
        var material = HKDF.DeriveKey(HashAlgorithmName.SHA256, keys.UdpLinkKey, 72, salt, info);
        var keyA = material[..32];
        var keyB = material[32..64];
        var saltA = material[64..68];
        var saltB = material[68..72];

        return isOfferer
            ? new UdpLinkCrypto(keyA, keyB, saltA, saltB)
            : new UdpLinkCrypto(keyB, keyA, saltB, saltA);
    }

    /// <summary>The proof carried as the engine connection key, so the acceptor can reject strangers pre-handshake.</summary>
    public static string ConnectionProof(ClusterKeys keys, Guid linkId, byte[] offerNonce, byte[] acceptNonce)
    {
        var data = new byte[16 + offerNonce.Length + acceptNonce.Length];
        linkId.TryWriteBytes(data);
        offerNonce.CopyTo(data, 16);
        acceptNonce.CopyTo(data, 16 + offerNonce.Length);
        var proof = HMACSHA256.HashData(keys.UdpLinkKey, data);
        return $"{linkId:N}:{Convert.ToBase64String(proof)}";
    }

    public byte[] Seal(byte flags, ushort termLow, ushort srcWireId, byte channelId, ReadOnlySpan<byte> payload,
        out long counter)
    {
        counter = Interlocked.Increment(ref _sendCounter);
        var datagram = new byte[HeaderLength + payload.Length + TagLength];
        datagram[0] = Version;
        datagram[1] = flags;
        BinaryPrimitives.WriteUInt16LittleEndian(datagram.AsSpan(2), termLow);
        BinaryPrimitives.WriteUInt16LittleEndian(datagram.AsSpan(4), srcWireId);
        datagram[6] = channelId;
        BinaryPrimitives.WriteInt64LittleEndian(datagram.AsSpan(7), counter);

        Span<byte> nonce = stackalloc byte[12];
        _sendSalt.CopyTo(nonce);
        BinaryPrimitives.WriteInt64LittleEndian(nonce[4..], counter);

        lock (_sendAes)
        {
            _sendAes.Encrypt(
                nonce,
                payload,
                datagram.AsSpan(HeaderLength, payload.Length),
                datagram.AsSpan(HeaderLength + payload.Length, TagLength),
                datagram.AsSpan(0, HeaderLength));
        }

        return datagram;
    }

    public bool TryOpen(ReadOnlySpan<byte> datagram,
        out byte flags, out ushort termLow, out ushort srcWireId, out byte channelId, out long counter, out byte[] payload)
    {
        flags = 0;
        termLow = 0;
        srcWireId = 0;
        channelId = 0;
        counter = 0;
        payload = [];
        if (datagram.Length < Overhead || datagram[0] != Version)
            return false;

        flags = datagram[1];
        termLow = BinaryPrimitives.ReadUInt16LittleEndian(datagram[2..]);
        srcWireId = BinaryPrimitives.ReadUInt16LittleEndian(datagram[4..]);
        channelId = datagram[6];
        counter = BinaryPrimitives.ReadInt64LittleEndian(datagram[7..]);

        Span<byte> nonce = stackalloc byte[12];
        _receiveSalt.CopyTo(nonce);
        BinaryPrimitives.WriteInt64LittleEndian(nonce[4..], counter);

        var plaintext = new byte[datagram.Length - Overhead];
        try
        {
            lock (_receiveAes)
            {
                _receiveAes.Decrypt(
                    nonce,
                    datagram[HeaderLength..^TagLength],
                    datagram[^TagLength..],
                    plaintext,
                    datagram[..HeaderLength]);
            }
        }
        catch (AuthenticationTagMismatchException)
        {
            return false; // tampered, foreign, or mis-keyed
        }

        if (!_replay.Accept(counter))
            return false;

        payload = plaintext;
        return true;
    }

    public void Dispose()
    {
        _sendAes.Dispose();
        _receiveAes.Dispose();
    }

    /// <summary>Sliding-window duplicate/replay suppression over the received GCM counters.</summary>
    private sealed class ReplayWindow
    {
        private const int WindowBits = 1024;
        private readonly ulong[] _bitmap = new ulong[WindowBits / 64];
        private long _highest;
        private readonly Lock _lock = new();

        public bool Accept(long counter)
        {
            if (counter <= 0)
                return false;

            lock (_lock)
            {
                if (counter > _highest)
                {
                    var advance = counter - _highest;
                    if (advance >= WindowBits)
                    {
                        Array.Clear(_bitmap);
                    }
                    else
                    {
                        for (var i = 0; i < advance; i++)
                            ClearBit((_highest + 1 + i) % WindowBits);
                    }

                    _highest = counter;
                    SetBit(counter % WindowBits);
                    return true;
                }

                if (_highest - counter >= WindowBits)
                    return false; // too old to judge — reject
                if (GetBit(counter % WindowBits))
                    return false; // duplicate

                SetBit(counter % WindowBits);
                return true;
            }
        }

        private bool GetBit(long index) => (_bitmap[index / 64] & (1UL << (int)(index % 64))) != 0;

        private void SetBit(long index) => _bitmap[index / 64] |= 1UL << (int)(index % 64);

        private void ClearBit(long index) => _bitmap[index / 64] &= ~(1UL << (int)(index % 64));
    }
}
