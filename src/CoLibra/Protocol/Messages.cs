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

    // Election
    ElectionStart = 40,
    ElectionAlive = 41,

    Error = 50,
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
    bool IsCoordinator);

// ---------------------------------------------------------------------------
// Discovery (UDP)
// ---------------------------------------------------------------------------

internal sealed record AnnounceMessage(
    Guid NodeId,
    long Incarnation,
    bool IsCoordinator,
    long Term,
    string ServiceVersion,
    int MeshPort) : Message
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
    int CoordinatorPort) : Message
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
    IReadOnlyList<HeldLeaseDto> HeldLeases) : Message
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
    double Weight) : Message
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

internal sealed record LeaseReleaseMessage(string LeaseType, string LeaseId, long Term, long Sequence) : Message
{
    public override MessageType Type => MessageType.LeaseRelease;
}

internal sealed record LeaseAvailableNotifyMessage(IReadOnlyList<LeaseKeyDto> Keys) : Message
{
    public override MessageType Type => MessageType.LeaseAvailableNotify;
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
