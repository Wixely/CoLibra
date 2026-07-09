using System.Buffers.Binary;

namespace CoLibra.Protocol;

/// <summary>
/// Stream framing: [4B LE payload length][1B protocol version][1B message type][JSON payload].
/// The length covers the version and type bytes plus the payload.
/// </summary>
internal static class FrameCodec
{
    public static byte[] Encode(Message message)
    {
        var payload = CoLibraJsonContext.Resolver.Serialize(message);
        var frame = new byte[4 + 2 + payload.Length];
        BinaryPrimitives.WriteInt32LittleEndian(frame, payload.Length + 2);
        frame[4] = ProtocolConstants.ProtocolVersion;
        frame[5] = (byte)message.Type;
        payload.CopyTo(frame.AsSpan(6));
        return frame;
    }

    public static async ValueTask WriteAsync(Stream stream, Message message, CancellationToken ct)
    {
        var frame = Encode(message);
        await stream.WriteAsync(frame, ct).ConfigureAwait(false);
        await stream.FlushAsync(ct).ConfigureAwait(false);
    }

    /// <summary>Reads the next known message, skipping unknown message types; returns null on clean end-of-stream.</summary>
    public static async ValueTask<Message?> ReadAsync(Stream stream, CancellationToken ct)
    {
        while (true)
        {
            var header = new byte[4];
            if (!await TryReadExactAsync(stream, header, ct).ConfigureAwait(false))
                return null;

            var length = BinaryPrimitives.ReadInt32LittleEndian(header);
            if (length is < 2 or > ProtocolConstants.MaxFrameBytes)
                throw new InvalidDataException($"Invalid frame length {length}.");

            var body = new byte[length];
            if (!await TryReadExactAsync(stream, body, ct).ConfigureAwait(false))
                throw new EndOfStreamException("Connection closed mid-frame.");

            var version = body[0];
            if (version != ProtocolConstants.ProtocolVersion)
                throw new InvalidDataException($"Unsupported protocol version {version} (local {ProtocolConstants.ProtocolVersion}).");

            var message = CoLibraJsonContext.Resolver.Deserialize((MessageType)body[1], body.AsSpan(2));
            if (message is not null)
                return message; // unknown types are skipped for forward compatibility
        }
    }

    private static async ValueTask<bool> TryReadExactAsync(Stream stream, Memory<byte> buffer, CancellationToken ct)
    {
        var total = 0;
        while (total < buffer.Length)
        {
            var read = await stream.ReadAsync(buffer[total..], ct).ConfigureAwait(false);
            if (read == 0)
                return total == 0 ? false : throw new EndOfStreamException("Connection closed mid-frame.");
            total += read;
        }

        return true;
    }
}
