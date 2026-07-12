using System.Buffers.Binary;
using CoLibra.Protocol;

namespace CoLibra.Tests;

public class FrameCodecTests
{
    [Fact]
    public async Task RoundTrips_all_message_types()
    {
        Message[] messages =
        [
            new AnnounceMessage(Guid.NewGuid(), 42, true, 7, "1.2.3", 41101),
            new ProbeMessage(Guid.NewGuid(), "1.0"),
            new ProbeReplyMessage(Guid.NewGuid(), 1, false, 3, "1.0", 5000, "10.0.0.5", 41101),
            new HelloChallengeMessage([1, 2, 3]),
            new HelloProofMessage(Guid.NewGuid(), 5, [4, 5], [6, 7]),
            new HelloAckMessage(Guid.NewGuid(), 5, [8]),
            new JoinRequestMessage(1, "2.0", 1.5, 41101, [new HeldLeaseDto("t", "i", 1, 2)]),
            new JoinResponseMessage(3, [new MemberDto(Guid.NewGuid(), 1, "10.0.0.1", 41101, "1.0", 1.0, true)], [new LeaseKeyDto("t", "x")], 15),
            new JoinRejectedMessage(JoinRejectionReason.VersionMismatch, "detail", "10.0.0.9", 4000),
            new HeartbeatMessage([new HeldLeaseDto("a", "b", 2, 9)], new Dictionary<string, int> { ["a"] = 1 }, 2.0),
            new HeartbeatAckMessage(4, 15, [new LeaseKeyDto("a", "b")]),
            new MembershipUpdateMessage(2, []),
            new LeaseAcquireMessage(Guid.NewGuid(), "src", "s1", ProcessingPreference.Other),
            new LeaseGrantResultMessage(Guid.NewGuid(), LeaseOutcome.Denied, 0, 0, 0, LeaseDenialReason.Rebalance, Guid.NewGuid()),
            new LeaseReleaseMessage("src", "s1", 3, 4),
            new LeaseAvailableNotifyMessage([new LeaseKeyDto("src", "s1")]),
            new ElectionStartMessage(9, Guid.NewGuid()),
            new ElectionAliveMessage(9, Guid.NewGuid(), true, false, null, 0),
            new ErrorMessage("code", "detail"),
        ];

        foreach (var message in messages)
        {
            using var stream = new MemoryStream(FrameCodec.Encode(message));
            var decoded = await FrameCodec.ReadAsync(stream, TestContext.Current.CancellationToken);
            Assert.NotNull(decoded);
            Assert.Equal(message.Type, decoded.Type);
            // Records with collection members don't compare structurally; compare wire shapes instead.
            Assert.Equal(
                CoLibraJsonContext.Resolver.Serialize(message),
                CoLibraJsonContext.Resolver.Serialize(decoded));
        }
    }

    [Fact]
    public async Task Skips_unknown_message_types()
    {
        var known = FrameCodec.Encode(new ProbeMessage(Guid.NewGuid(), "1.0"));
        var unknownPayload = "{}"u8.ToArray();
        var unknown = new byte[4 + 2 + unknownPayload.Length];
        BinaryPrimitives.WriteInt32LittleEndian(unknown, unknownPayload.Length + 2);
        unknown[4] = ProtocolConstants.ProtocolVersion;
        unknown[5] = 200; // not a defined MessageType
        unknownPayload.CopyTo(unknown, 6);

        using var stream = new MemoryStream([.. unknown, .. known]);
        var decoded = await FrameCodec.ReadAsync(stream, TestContext.Current.CancellationToken);
        Assert.IsType<ProbeMessage>(decoded);
    }

    [Fact]
    public async Task Returns_null_on_clean_end_of_stream()
    {
        using var stream = new MemoryStream();
        Assert.Null(await FrameCodec.ReadAsync(stream, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task Rejects_wrong_protocol_version()
    {
        var frame = FrameCodec.Encode(new ProbeMessage(Guid.NewGuid(), "1.0"));
        frame[4] = 99;
        using var stream = new MemoryStream(frame);
        await Assert.ThrowsAsync<InvalidDataException>(async () =>
            await FrameCodec.ReadAsync(stream, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task Rejects_oversized_frames()
    {
        var frame = new byte[8];
        BinaryPrimitives.WriteInt32LittleEndian(frame, int.MaxValue);
        using var stream = new MemoryStream(frame);
        await Assert.ThrowsAsync<InvalidDataException>(async () =>
            await FrameCodec.ReadAsync(stream, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task Rejects_hybrid_frame_with_overflowing_header_length()
    {
        // A hostile hybrid header length near int.MaxValue must be rejected cleanly, not overflow
        // `6 + headerLength` into a negative that slips past the bound check (then over-reads).
        var body = new byte[10];
        body[0] = ProtocolConstants.ProtocolVersion;
        body[1] = 64; // RoutedPayload -> hybrid decode path
        BinaryPrimitives.WriteInt32LittleEndian(body.AsSpan(2), int.MaxValue);
        var frame = new byte[4 + body.Length];
        BinaryPrimitives.WriteInt32LittleEndian(frame, body.Length);
        body.CopyTo(frame, 4);

        using var stream = new MemoryStream(frame);
        await Assert.ThrowsAsync<InvalidDataException>(async () =>
            await FrameCodec.ReadAsync(stream, TestContext.Current.CancellationToken));
    }
}
