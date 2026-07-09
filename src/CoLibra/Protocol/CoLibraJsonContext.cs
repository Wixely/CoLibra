using System.Text.Json;
using System.Text.Json.Serialization;

namespace CoLibra.Protocol;

[JsonSourceGenerationOptions(
    DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    UnmappedMemberHandling = JsonUnmappedMemberHandling.Skip)]
[JsonSerializable(typeof(AnnounceMessage))]
[JsonSerializable(typeof(ProbeMessage))]
[JsonSerializable(typeof(ProbeReplyMessage))]
[JsonSerializable(typeof(HelloChallengeMessage))]
[JsonSerializable(typeof(HelloProofMessage))]
[JsonSerializable(typeof(HelloAckMessage))]
[JsonSerializable(typeof(JoinRequestMessage))]
[JsonSerializable(typeof(JoinResponseMessage))]
[JsonSerializable(typeof(JoinRejectedMessage))]
[JsonSerializable(typeof(HeartbeatMessage))]
[JsonSerializable(typeof(HeartbeatAckMessage))]
[JsonSerializable(typeof(MembershipUpdateMessage))]
[JsonSerializable(typeof(LeaseAcquireMessage))]
[JsonSerializable(typeof(LeaseGrantResultMessage))]
[JsonSerializable(typeof(LeaseReleaseMessage))]
[JsonSerializable(typeof(LeaseAvailableNotifyMessage))]
[JsonSerializable(typeof(ElectionStartMessage))]
[JsonSerializable(typeof(ElectionAliveMessage))]
[JsonSerializable(typeof(ErrorMessage))]
internal sealed partial class CoLibraJsonContext : JsonSerializerContext
{
    public static JsonTypeInfoResolverForMessages Resolver { get; } = new();
}

/// <summary>Maps the frame-header <see cref="MessageType"/> byte to concrete DTO serialization.</summary>
internal sealed class JsonTypeInfoResolverForMessages
{
    public byte[] Serialize(Message message) => message switch
    {
        AnnounceMessage m => JsonSerializer.SerializeToUtf8Bytes(m, CoLibraJsonContext.Default.AnnounceMessage),
        ProbeMessage m => JsonSerializer.SerializeToUtf8Bytes(m, CoLibraJsonContext.Default.ProbeMessage),
        ProbeReplyMessage m => JsonSerializer.SerializeToUtf8Bytes(m, CoLibraJsonContext.Default.ProbeReplyMessage),
        HelloChallengeMessage m => JsonSerializer.SerializeToUtf8Bytes(m, CoLibraJsonContext.Default.HelloChallengeMessage),
        HelloProofMessage m => JsonSerializer.SerializeToUtf8Bytes(m, CoLibraJsonContext.Default.HelloProofMessage),
        HelloAckMessage m => JsonSerializer.SerializeToUtf8Bytes(m, CoLibraJsonContext.Default.HelloAckMessage),
        JoinRequestMessage m => JsonSerializer.SerializeToUtf8Bytes(m, CoLibraJsonContext.Default.JoinRequestMessage),
        JoinResponseMessage m => JsonSerializer.SerializeToUtf8Bytes(m, CoLibraJsonContext.Default.JoinResponseMessage),
        JoinRejectedMessage m => JsonSerializer.SerializeToUtf8Bytes(m, CoLibraJsonContext.Default.JoinRejectedMessage),
        HeartbeatMessage m => JsonSerializer.SerializeToUtf8Bytes(m, CoLibraJsonContext.Default.HeartbeatMessage),
        HeartbeatAckMessage m => JsonSerializer.SerializeToUtf8Bytes(m, CoLibraJsonContext.Default.HeartbeatAckMessage),
        MembershipUpdateMessage m => JsonSerializer.SerializeToUtf8Bytes(m, CoLibraJsonContext.Default.MembershipUpdateMessage),
        LeaseAcquireMessage m => JsonSerializer.SerializeToUtf8Bytes(m, CoLibraJsonContext.Default.LeaseAcquireMessage),
        LeaseGrantResultMessage m => JsonSerializer.SerializeToUtf8Bytes(m, CoLibraJsonContext.Default.LeaseGrantResultMessage),
        LeaseReleaseMessage m => JsonSerializer.SerializeToUtf8Bytes(m, CoLibraJsonContext.Default.LeaseReleaseMessage),
        LeaseAvailableNotifyMessage m => JsonSerializer.SerializeToUtf8Bytes(m, CoLibraJsonContext.Default.LeaseAvailableNotifyMessage),
        ElectionStartMessage m => JsonSerializer.SerializeToUtf8Bytes(m, CoLibraJsonContext.Default.ElectionStartMessage),
        ElectionAliveMessage m => JsonSerializer.SerializeToUtf8Bytes(m, CoLibraJsonContext.Default.ElectionAliveMessage),
        ErrorMessage m => JsonSerializer.SerializeToUtf8Bytes(m, CoLibraJsonContext.Default.ErrorMessage),
        _ => throw new NotSupportedException($"Unknown message type {message.GetType()}"),
    };

    public Message? Deserialize(MessageType type, ReadOnlySpan<byte> payload) => type switch
    {
        MessageType.Announce => JsonSerializer.Deserialize(payload, CoLibraJsonContext.Default.AnnounceMessage),
        MessageType.Probe => JsonSerializer.Deserialize(payload, CoLibraJsonContext.Default.ProbeMessage),
        MessageType.ProbeReply => JsonSerializer.Deserialize(payload, CoLibraJsonContext.Default.ProbeReplyMessage),
        MessageType.HelloChallenge => JsonSerializer.Deserialize(payload, CoLibraJsonContext.Default.HelloChallengeMessage),
        MessageType.HelloProof => JsonSerializer.Deserialize(payload, CoLibraJsonContext.Default.HelloProofMessage),
        MessageType.HelloAck => JsonSerializer.Deserialize(payload, CoLibraJsonContext.Default.HelloAckMessage),
        MessageType.JoinRequest => JsonSerializer.Deserialize(payload, CoLibraJsonContext.Default.JoinRequestMessage),
        MessageType.JoinResponse => JsonSerializer.Deserialize(payload, CoLibraJsonContext.Default.JoinResponseMessage),
        MessageType.JoinRejected => JsonSerializer.Deserialize(payload, CoLibraJsonContext.Default.JoinRejectedMessage),
        MessageType.Heartbeat => JsonSerializer.Deserialize(payload, CoLibraJsonContext.Default.HeartbeatMessage),
        MessageType.HeartbeatAck => JsonSerializer.Deserialize(payload, CoLibraJsonContext.Default.HeartbeatAckMessage),
        MessageType.MembershipUpdate => JsonSerializer.Deserialize(payload, CoLibraJsonContext.Default.MembershipUpdateMessage),
        MessageType.LeaseAcquire => JsonSerializer.Deserialize(payload, CoLibraJsonContext.Default.LeaseAcquireMessage),
        MessageType.LeaseGrantResult => JsonSerializer.Deserialize(payload, CoLibraJsonContext.Default.LeaseGrantResultMessage),
        MessageType.LeaseRelease => JsonSerializer.Deserialize(payload, CoLibraJsonContext.Default.LeaseReleaseMessage),
        MessageType.LeaseAvailableNotify => JsonSerializer.Deserialize(payload, CoLibraJsonContext.Default.LeaseAvailableNotifyMessage),
        MessageType.ElectionStart => JsonSerializer.Deserialize(payload, CoLibraJsonContext.Default.ElectionStartMessage),
        MessageType.ElectionAlive => JsonSerializer.Deserialize(payload, CoLibraJsonContext.Default.ElectionAliveMessage),
        MessageType.Error => JsonSerializer.Deserialize(payload, CoLibraJsonContext.Default.ErrorMessage),
        _ => null, // unknown message types are ignored for forward compatibility
    };
}
