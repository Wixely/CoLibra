using System.Buffers.Binary;

namespace CoLibra.Protocol;

/// <summary>
/// Stream framing: [4B LE payload length][1B protocol version][1B message type][JSON payload].
/// The length covers the version and type bytes plus the payload.
/// Exception: <see cref="MessageType.RoutedPayload"/> uses a hybrid body —
/// [4B LE header length][JSON header][raw payload bytes] — so application payloads are never
/// base64-inflated. Every other message stays pure JSON.
/// </summary>
internal static class FrameCodec
{
    public static byte[] Encode(Message message)
    {
        if (HybridPayloadOf(message) is { } body)
            return EncodeHybrid(message, body);

        var payload = CoLibraJsonContext.Resolver.Serialize(message);
        var frame = new byte[4 + 2 + payload.Length];
        BinaryPrimitives.WriteInt32LittleEndian(frame, payload.Length + 2);
        frame[4] = ProtocolConstants.ProtocolVersion;
        frame[5] = (byte)message.Type;
        payload.CopyTo(frame.AsSpan(6));
        return frame;
    }

    /// <summary>Messages whose application payload rides as raw bytes after the JSON header.</summary>
    private static byte[]? HybridPayloadOf(Message message) => message switch
    {
        RoutedPayloadMessage m => m.Payload,
        DirectMessageMessage m => m.Payload,
        _ => null,
    };

    private static byte[] EncodeHybrid(Message message, byte[] body)
    {
        var header = CoLibraJsonContext.Resolver.Serialize(message); // payload property is [JsonIgnore]
        var frame = new byte[4 + 2 + 4 + header.Length + body.Length];
        BinaryPrimitives.WriteInt32LittleEndian(frame, 2 + 4 + header.Length + body.Length);
        frame[4] = ProtocolConstants.ProtocolVersion;
        frame[5] = (byte)message.Type;
        BinaryPrimitives.WriteInt32LittleEndian(frame.AsSpan(6), header.Length);
        header.CopyTo(frame.AsSpan(10));
        body.CopyTo(frame.AsSpan(10 + header.Length));
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

            if ((MessageType)body[1] is MessageType.RoutedPayload or MessageType.DirectMessage)
                return DecodeHybrid(body);

            var message = CoLibraJsonContext.Resolver.Deserialize((MessageType)body[1], body.AsSpan(2));
            if (message is not null)
                return message; // unknown types are skipped for forward compatibility
        }
    }

    private static Message DecodeHybrid(byte[] body)
    {
        // body = [1B ver][1B type][4B header length][JSON header][raw payload]
        var type = (MessageType)body[1];
        var headerLength = BinaryPrimitives.ReadInt32LittleEndian(body.AsSpan(2));
        if (headerLength < 2 || 6 + headerLength > body.Length)
            throw new InvalidDataException($"Invalid hybrid-frame header length {headerLength}.");

        var header = CoLibraJsonContext.Resolver.Deserialize(type, body.AsSpan(6, headerLength))
            ?? throw new InvalidDataException($"Unreadable hybrid-frame header for {type}.");
        var payload = body[(6 + headerLength)..];
        return header switch
        {
            RoutedPayloadMessage m => m with { Payload = payload },
            DirectMessageMessage m => m with { Payload = payload },
            _ => throw new InvalidDataException($"Message type {type} is not hybrid-framed."),
        };
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
