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
[JsonSerializable(typeof(CompletionSyncMessage))]
[JsonSerializable(typeof(LeaseRevokedMessage))]
[JsonSerializable(typeof(ElectionStartMessage))]
[JsonSerializable(typeof(ElectionAliveMessage))]
[JsonSerializable(typeof(ErrorMessage))]
[JsonSerializable(typeof(OwnerResolveMessage))]
[JsonSerializable(typeof(OwnerResolveReplyMessage))]
[JsonSerializable(typeof(LeaseAssignMessage))]
[JsonSerializable(typeof(LeaseAssignAckMessage))]
[JsonSerializable(typeof(RoutedPayloadMessage))]
[JsonSerializable(typeof(RoutedAckMessage))]
[JsonSerializable(typeof(DirectMessageMessage))]
[JsonSerializable(typeof(DirectMessageAckMessage))]
[JsonSerializable(typeof(UdpLinkOfferMessage))]
[JsonSerializable(typeof(UdpLinkAcceptMessage))]
[JsonSerializable(typeof(UdpPunchRequestMessage))]
[JsonSerializable(typeof(UdpPunchInstructMessage))]
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
        CompletionSyncMessage m => JsonSerializer.SerializeToUtf8Bytes(m, CoLibraJsonContext.Default.CompletionSyncMessage),
        LeaseRevokedMessage m => JsonSerializer.SerializeToUtf8Bytes(m, CoLibraJsonContext.Default.LeaseRevokedMessage),
        ElectionStartMessage m => JsonSerializer.SerializeToUtf8Bytes(m, CoLibraJsonContext.Default.ElectionStartMessage),
        ElectionAliveMessage m => JsonSerializer.SerializeToUtf8Bytes(m, CoLibraJsonContext.Default.ElectionAliveMessage),
        ErrorMessage m => JsonSerializer.SerializeToUtf8Bytes(m, CoLibraJsonContext.Default.ErrorMessage),
        OwnerResolveMessage m => JsonSerializer.SerializeToUtf8Bytes(m, CoLibraJsonContext.Default.OwnerResolveMessage),
        OwnerResolveReplyMessage m => JsonSerializer.SerializeToUtf8Bytes(m, CoLibraJsonContext.Default.OwnerResolveReplyMessage),
        LeaseAssignMessage m => JsonSerializer.SerializeToUtf8Bytes(m, CoLibraJsonContext.Default.LeaseAssignMessage),
        LeaseAssignAckMessage m => JsonSerializer.SerializeToUtf8Bytes(m, CoLibraJsonContext.Default.LeaseAssignAckMessage),
        RoutedPayloadMessage m => JsonSerializer.SerializeToUtf8Bytes(m, CoLibraJsonContext.Default.RoutedPayloadMessage),
        RoutedAckMessage m => JsonSerializer.SerializeToUtf8Bytes(m, CoLibraJsonContext.Default.RoutedAckMessage),
        DirectMessageMessage m => JsonSerializer.SerializeToUtf8Bytes(m, CoLibraJsonContext.Default.DirectMessageMessage),
        DirectMessageAckMessage m => JsonSerializer.SerializeToUtf8Bytes(m, CoLibraJsonContext.Default.DirectMessageAckMessage),
        UdpLinkOfferMessage m => JsonSerializer.SerializeToUtf8Bytes(m, CoLibraJsonContext.Default.UdpLinkOfferMessage),
        UdpLinkAcceptMessage m => JsonSerializer.SerializeToUtf8Bytes(m, CoLibraJsonContext.Default.UdpLinkAcceptMessage),
        UdpPunchRequestMessage m => JsonSerializer.SerializeToUtf8Bytes(m, CoLibraJsonContext.Default.UdpPunchRequestMessage),
        UdpPunchInstructMessage m => JsonSerializer.SerializeToUtf8Bytes(m, CoLibraJsonContext.Default.UdpPunchInstructMessage),
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
        MessageType.CompletionSync => JsonSerializer.Deserialize(payload, CoLibraJsonContext.Default.CompletionSyncMessage),
        MessageType.LeaseRevoked => JsonSerializer.Deserialize(payload, CoLibraJsonContext.Default.LeaseRevokedMessage),
        MessageType.ElectionStart => JsonSerializer.Deserialize(payload, CoLibraJsonContext.Default.ElectionStartMessage),
        MessageType.ElectionAlive => JsonSerializer.Deserialize(payload, CoLibraJsonContext.Default.ElectionAliveMessage),
        MessageType.Error => JsonSerializer.Deserialize(payload, CoLibraJsonContext.Default.ErrorMessage),
        MessageType.OwnerResolve => JsonSerializer.Deserialize(payload, CoLibraJsonContext.Default.OwnerResolveMessage),
        MessageType.OwnerResolveReply => JsonSerializer.Deserialize(payload, CoLibraJsonContext.Default.OwnerResolveReplyMessage),
        MessageType.LeaseAssign => JsonSerializer.Deserialize(payload, CoLibraJsonContext.Default.LeaseAssignMessage),
        MessageType.LeaseAssignAck => JsonSerializer.Deserialize(payload, CoLibraJsonContext.Default.LeaseAssignAckMessage),
        MessageType.RoutedPayload => JsonSerializer.Deserialize(payload, CoLibraJsonContext.Default.RoutedPayloadMessage),
        MessageType.RoutedAck => JsonSerializer.Deserialize(payload, CoLibraJsonContext.Default.RoutedAckMessage),
        MessageType.DirectMessage => JsonSerializer.Deserialize(payload, CoLibraJsonContext.Default.DirectMessageMessage),
        MessageType.DirectMessageAck => JsonSerializer.Deserialize(payload, CoLibraJsonContext.Default.DirectMessageAckMessage),
        MessageType.UdpLinkOffer => JsonSerializer.Deserialize(payload, CoLibraJsonContext.Default.UdpLinkOfferMessage),
        MessageType.UdpLinkAccept => JsonSerializer.Deserialize(payload, CoLibraJsonContext.Default.UdpLinkAcceptMessage),
        MessageType.UdpPunchRequest => JsonSerializer.Deserialize(payload, CoLibraJsonContext.Default.UdpPunchRequestMessage),
        MessageType.UdpPunchInstruct => JsonSerializer.Deserialize(payload, CoLibraJsonContext.Default.UdpPunchInstructMessage),
        _ => null, // unknown message types are ignored for forward compatibility
    };
}
