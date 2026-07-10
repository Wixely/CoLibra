namespace CoLibra.Protocol;

internal static class ProtocolConstants
{
    public const byte ProtocolVersion = 1;
    public const int MaxFrameBytes = 4 * 1024 * 1024;
}

internal enum MessageType : byte
{
    // Discovery (UDP)
    Announce = 1,
    Probe = 2,
    ProbeReply = 3,

    // Mesh handshake
    HelloChallenge = 10,
    HelloProof = 11,
    HelloAck = 12,

    // Membership
    JoinRequest = 20,
    JoinResponse = 21,
    JoinRejected = 22,
    Heartbeat = 23,
    HeartbeatAck = 24,
    MembershipUpdate = 25,

    // Leases
    LeaseAcquire = 30,
    LeaseGrantResult = 31,
    LeaseRelease = 32,
    LeaseAvailableNotify = 33,
    CompletionSync = 34,

    // Election
    ElectionStart = 40,
    ElectionAlive = 41,

    Error = 50,

    // Routed delivery
    OwnerResolve = 60,
    OwnerResolveReply = 61,
    LeaseAssign = 62,
    LeaseAssignAck = 63,
    RoutedPayload = 64,   // hybrid frame: JSON header + raw payload bytes (see FrameCodec)
    RoutedAck = 65,

    // Direct node-to-node messaging
    DirectMessage = 70,   // hybrid frame, like RoutedPayload
    DirectMessageAck = 71,
}

internal abstract record Message
{
    public abstract MessageType Type { get; }
}

// ---------------------------------------------------------------------------
// Shared DTO fragments
// ---------------------------------------------------------------------------

internal sealed record LeaseKeyDto(string Type, string Id)
{
    public LeaseKey ToKey() => new(Type, Id);
    public static LeaseKeyDto From(LeaseKey key) => new(key.Type, key.Id);
}

internal sealed record HeldLeaseDto(string Type, string Id, long Term, long Sequence)
{
    public LeaseKey ToKey() => new(Type, Id);
    public FencingToken ToToken() => new(Term, Sequence);
}

internal sealed record MemberDto(
    Guid NodeId,
    long Incarnation,
    string Host,
    int Port,
    string ServiceVersion,
    double Weight,
    bool IsCoordinator,
    string? Name = null);

// ---------------------------------------------------------------------------
// Discovery (UDP)
// ---------------------------------------------------------------------------

internal sealed record AnnounceMessage(
    Guid NodeId,
    long Incarnation,
    bool IsCoordinator,
    long Term,
    string ServiceVersion,
    int MeshPort,
    bool Forced = false) : Message
{
    public override MessageType Type => MessageType.Announce;
}

internal sealed record ProbeMessage(Guid NodeId, string ServiceVersion) : Message
{
    public override MessageType Type => MessageType.Probe;
}

internal sealed record ProbeReplyMessage(
    Guid NodeId,
    long Incarnation,
    bool IsCoordinator,
    long Term,
    string ServiceVersion,
    int MeshPort,
    string? CoordinatorHost,
    int CoordinatorPort,
    bool Forced = false) : Message
{
    public override MessageType Type => MessageType.ProbeReply;
}

// ---------------------------------------------------------------------------
// Mesh handshake (inside TLS): mutual HMAC challenge-response over HandshakeKey
// ---------------------------------------------------------------------------

internal sealed record HelloChallengeMessage(byte[] ServerNonce) : Message
{
    public override MessageType Type => MessageType.HelloChallenge;
}

internal sealed record HelloProofMessage(
    Guid NodeId,
    long Incarnation,
    byte[] ClientNonce,
    byte[] Proof) : Message
{
    public override MessageType Type => MessageType.HelloProof;
}

internal sealed record HelloAckMessage(Guid NodeId, long Incarnation, byte[] Proof) : Message
{
    public override MessageType Type => MessageType.HelloAck;
}

// ---------------------------------------------------------------------------
// Membership
// ---------------------------------------------------------------------------

internal sealed record JoinRequestMessage(
    byte ProtocolVersion,
    string ServiceVersion,
    double Weight,
    int MeshPort,
    IReadOnlyList<HeldLeaseDto> HeldLeases,
    bool SupportsCompletionSync = false,
    IReadOnlyList<string>? RoutedTypes = null,
    string? NodeName = null) : Message
{
    public override MessageType Type => MessageType.JoinRequest;
}

internal sealed record JoinResponseMessage(
    long Term,
    IReadOnlyList<MemberDto> Members,
    IReadOnlyList<LeaseKeyDto> RejectedAsserts,
    double LeaseTtlSeconds) : Message
{
    public override MessageType Type => MessageType.JoinResponse;
}

internal enum JoinRejectionReason
{
    NotCoordinator = 0,
    VersionMismatch = 1,
    DuplicateNodeId = 2,
    ProtocolIncompatible = 3,
}

internal sealed record JoinRejectedMessage(
    JoinRejectionReason Reason,
    string Detail,
    string? CoordinatorHost,
    int CoordinatorPort) : Message
{
    public override MessageType Type => MessageType.JoinRejected;
}

internal sealed record HeartbeatMessage(
    IReadOnlyList<HeldLeaseDto> HeldLeases,
    Dictionary<string, int> PerTypeCounts,
    double Weight,
    IReadOnlyList<string>? RoutedTypes = null) : Message
{
    public override MessageType Type => MessageType.Heartbeat;
}

internal sealed record HeartbeatAckMessage(
    long Term,
    double LeaseTtlSeconds,
    IReadOnlyList<LeaseKeyDto> LostKeys) : Message
{
    public override MessageType Type => MessageType.HeartbeatAck;
}

internal sealed record MembershipUpdateMessage(long Term, IReadOnlyList<MemberDto> Members) : Message
{
    public override MessageType Type => MessageType.MembershipUpdate;
}

// ---------------------------------------------------------------------------
// Leases
// ---------------------------------------------------------------------------

internal sealed record LeaseAcquireMessage(
    Guid RequestId,
    string LeaseType,
    string LeaseId,
    ProcessingPreference Preference) : Message
{
    public override MessageType Type => MessageType.LeaseAcquire;
}

internal enum LeaseOutcome
{
    Granted = 0,
    Denied = 1,
}

internal sealed record LeaseGrantResultMessage(
    Guid RequestId,
    LeaseOutcome Outcome,
    long Term,
    long Sequence,
    double TtlSeconds,
    LeaseDenialReason DenialReason,
    Guid? CurrentOwner) : Message
{
    public override MessageType Type => MessageType.LeaseGrantResult;
}

internal sealed record LeaseReleaseMessage(
    string LeaseType, string LeaseId, long Term, long Sequence, bool AsCompleted = false) : Message
{
    public override MessageType Type => MessageType.LeaseRelease;
}

internal sealed record LeaseAvailableNotifyMessage(IReadOnlyList<LeaseKeyDto> Keys) : Message
{
    public override MessageType Type => MessageType.LeaseAvailableNotify;
}

/// <summary>
/// Union-merge of completion tombstones, chunked to stay well under the frame limit. Sent
/// coordinator → members (steady-state broadcast and join-time snapshot) and member →
/// coordinator (join-time snapshot upload, which survives coordinator failover).
/// </summary>
internal sealed record CompletionSyncMessage(IReadOnlyList<LeaseKeyDto> Entries) : Message
{
    public override MessageType Type => MessageType.CompletionSync;

    /// <summary>Entries per message; 1000 ids of typical size is ~50 KB, far below MaxFrameBytes.</summary>
    public const int ChunkSize = 1000;
}

// ---------------------------------------------------------------------------
// Election
// ---------------------------------------------------------------------------

internal sealed record ElectionStartMessage(long Term, Guid CandidateNodeId) : Message
{
    public override MessageType Type => MessageType.ElectionStart;
}

internal sealed record ElectionAliveMessage(
    long Term,
    Guid NodeId,
    bool WillContest,
    bool IsCoordinator,
    string? KnownCoordinatorHost,
    int KnownCoordinatorPort) : Message
{
    public override MessageType Type => MessageType.ElectionAlive;
}

internal sealed record ErrorMessage(string Code, string Detail) : Message
{
    public override MessageType Type => MessageType.Error;
}

// ---------------------------------------------------------------------------
// Routed delivery
// ---------------------------------------------------------------------------

internal sealed record OwnerResolveMessage(Guid RequestId, string LeaseType, string LeaseId) : Message
{
    public override MessageType Type => MessageType.OwnerResolve;
}

internal enum ResolveOutcome
{
    /// <summary>The key has an owner (possibly just force-assigned).</summary>
    Resolved = 0,

    /// <summary>No node advertises a handler for the type.</summary>
    NoHandler = 1,

    /// <summary>Refused under the current split-brain / quorum policy.</summary>
    Unavailable = 2,

    /// <summary>Transient (rebuild window, election in progress); retry shortly.</summary>
    Retry = 3,

    /// <summary>The key was marked completed; there is nothing left to route to.</summary>
    Completed = 4,
}

/// <summary>Token fields are filled when the owner is the requester itself, so it can install the lease locally.</summary>
internal sealed record OwnerResolveReplyMessage(
    Guid RequestId,
    ResolveOutcome Outcome,
    Guid? OwnerNodeId,
    string? OwnerHost,
    int OwnerPort,
    bool WasAssigned,
    long TokenTerm = 0,
    long TokenSequence = 0) : Message
{
    public override MessageType Type => MessageType.OwnerResolveReply;
}

/// <summary>Coordinator → member: install this lease (forced assignment). The member acks before the coordinator commits.</summary>
internal sealed record LeaseAssignMessage(
    string LeaseType, string LeaseId, long Term, long Sequence, double TtlSeconds) : Message
{
    public override MessageType Type => MessageType.LeaseAssign;
}

internal sealed record LeaseAssignAckMessage(
    string LeaseType, string LeaseId, long Term, long Sequence, bool Accepted) : Message
{
    public override MessageType Type => MessageType.LeaseAssignAck;
}

/// <summary>
/// A routed application payload. The <see cref="Payload"/> travels as raw bytes after a JSON
/// header (never base64) — see FrameCodec's hybrid encoding. <see cref="RelayToNodeId"/> asks
/// the receiving coordinator to forward to that member (relay path).
/// </summary>
internal sealed record RoutedPayloadMessage(
    Guid RouteId,
    string LeaseType,
    string LeaseId,
    Guid OriginNodeId,
    Guid? RelayToNodeId) : Message
{
    public override MessageType Type => MessageType.RoutedPayload;

    [System.Text.Json.Serialization.JsonIgnore]
    public byte[] Payload { get; init; } = [];
}

internal enum RouteAckStatus
{
    Delivered = 0,
    NoHandler = 1,
    NotOwner = 2,
}

internal sealed record RoutedAckMessage(Guid RouteId, RouteAckStatus Status, Guid? RelayToNodeId) : Message
{
    public override MessageType Type => MessageType.RoutedAck;
}

// ---------------------------------------------------------------------------
// Direct node-to-node messaging
// ---------------------------------------------------------------------------

/// <summary>
/// A node-addressed application message. Payload travels as raw bytes after the JSON header
/// (hybrid frame). <see cref="RelayToNodeId"/> asks the receiving coordinator to forward.
/// </summary>
internal sealed record DirectMessageMessage(
    Guid MessageId,
    string Channel,
    Guid OriginNodeId,
    string? OriginName,
    Guid? RelayToNodeId) : Message
{
    public override MessageType Type => MessageType.DirectMessage;

    [System.Text.Json.Serialization.JsonIgnore]
    public byte[] Payload { get; init; } = [];
}

internal enum DirectAckStatus
{
    Delivered = 0,
    NoHandler = 1,
    Unreachable = 2,
}

internal sealed record DirectMessageAckMessage(Guid MessageId, DirectAckStatus Status, Guid? RelayToNodeId) : Message
{
    public override MessageType Type => MessageType.DirectMessageAck;
}
