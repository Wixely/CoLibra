using CoLibra.Leasing;
using CoLibra.Protocol;
using Microsoft.Extensions.Time.Testing;

namespace CoLibra.Tests;

public class RoutingProtocolTests
{
    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(64)]
    [InlineData(1_048_576)]
    public async Task RoutedPayload_round_trips_raw_bytes(int size)
    {
        var payload = new byte[size];
        Random.Shared.NextBytes(payload);
        var message = new RoutedPayloadMessage(Guid.NewGuid(), "evt", "e1", Guid.NewGuid(), Guid.NewGuid())
        {
            Payload = payload,
        };

        using var stream = new MemoryStream(FrameCodec.Encode(message));
        var decoded = Assert.IsType<RoutedPayloadMessage>(
            await FrameCodec.ReadAsync(stream, TestContext.Current.CancellationToken));

        Assert.Equal(message.RouteId, decoded.RouteId);
        Assert.Equal(message.LeaseType, decoded.LeaseType);
        Assert.Equal(message.LeaseId, decoded.LeaseId);
        Assert.Equal(message.OriginNodeId, decoded.OriginNodeId);
        Assert.Equal(message.RelayToNodeId, decoded.RelayToNodeId);
        Assert.Equal(payload, decoded.Payload);
    }

    [Fact]
    public void RoutedPayload_frame_does_not_base64_the_payload()
    {
        var payload = new byte[1000];
        Random.Shared.NextBytes(payload);
        var frame = FrameCodec.Encode(new RoutedPayloadMessage(Guid.NewGuid(), "t", "i", Guid.NewGuid(), null)
        {
            Payload = payload,
        });

        // Raw framing: total is header-ish bytes + exactly payload.Length, far below base64's 4/3 inflation.
        Assert.True(frame.Length < payload.Length + 250,
            $"frame {frame.Length} bytes suggests the payload was string-encoded");
    }

    [Fact]
    public async Task Other_routing_messages_round_trip()
    {
        Message[] messages =
        [
            new OwnerResolveMessage(Guid.NewGuid(), "evt", "e1"),
            new OwnerResolveReplyMessage(Guid.NewGuid(), ResolveOutcome.Resolved, Guid.NewGuid(), "10.0.0.4", 41101, true, 3, 17),
            new LeaseAssignMessage("evt", "e1", 3, 17, 15),
            new LeaseAssignAckMessage("evt", "e1", 3, 17, true),
            new RoutedAckMessage(Guid.NewGuid(), RouteAckStatus.NotOwner, Guid.NewGuid()),
        ];

        foreach (var message in messages)
        {
            using var stream = new MemoryStream(FrameCodec.Encode(message));
            var decoded = await FrameCodec.ReadAsync(stream, TestContext.Current.CancellationToken);
            Assert.NotNull(decoded);
            Assert.Equal(message.Type, decoded.Type);
            Assert.Equal(message, decoded);
        }
    }

    [Fact]
    public void PickLeastLoaded_prefers_the_emptiest_candidate()
    {
        var options = new CoLibraOptions { ServiceId = "svc", SharedSecret = "s" };
        var table = new CoordinatorLeaseTable(1, options, TimeProvider.System);
        var (a, b) = (NodeId.NewId(), NodeId.NewId());
        table.NodeUp(a, 1.0);
        table.NodeUp(b, 1.0);
        table.Assign(a, new LeaseKey("evt", "1"), table.NextToken(), 0);
        table.Assign(a, new LeaseKey("evt", "2"), table.NextToken(), 0);

        Assert.Equal(b, table.PickLeastLoaded("evt", [a, b]));
        Assert.Null(table.PickLeastLoaded("evt", []));
    }

    [Fact]
    public void PickLeastLoaded_normalizes_by_weight()
    {
        var options = new CoLibraOptions { ServiceId = "svc", SharedSecret = "s" };
        var table = new CoordinatorLeaseTable(1, options, TimeProvider.System);
        var (big, small) = (NodeId.NewId(), NodeId.NewId());
        table.NodeUp(big, 4.0);
        table.NodeUp(small, 1.0);
        table.Assign(big, new LeaseKey("evt", "1"), table.NextToken(), 0);
        table.Assign(big, new LeaseKey("evt", "2"), table.NextToken(), 0);
        table.Assign(small, new LeaseKey("evt", "3"), table.NextToken(), 0);

        // big: 2/4 = 0.5 load; small: 1/1 = 1.0 load — big is still the lighter choice.
        Assert.Equal(big, table.PickLeastLoaded("evt", [big, small]));
    }

    [Fact]
    public void Assigned_keys_are_owned_and_deny_other_requesters()
    {
        var time = new FakeTimeProvider();
        var options = new CoLibraOptions { ServiceId = "svc", SharedSecret = "s" };
        var table = new CoordinatorLeaseTable(1, options, time);
        var (owner, other) = (NodeId.NewId(), NodeId.NewId());
        table.NodeUp(owner, 1.0);
        table.NodeUp(other, 1.0);

        var key = new LeaseKey("evt", "1");
        var token = table.NextToken();
        table.Assign(owner, key, token, time.GetTimestamp());

        Assert.True(table.TryGetOwner(key, out var resolved, out var resolvedToken));
        Assert.Equal(owner, resolved);
        Assert.Equal(token, resolvedToken);

        var outcome = table.Acquire(other, Guid.NewGuid(), key, ProcessingPreference.This, time.GetTimestamp());
        Assert.False(outcome.Immediate!.Value.Granted);
        Assert.Equal(LeaseDenialReason.HeldByOther, outcome.Immediate.Value.Reason);
    }

    [Fact]
    public void Routing_options_validation()
    {
        var options = new CoLibraOptions { ServiceId = "svc", SharedSecret = "s" };
        options.Routing.Enabled = true;
        Assert.True(new CoLibraOptionsValidator().Validate(null, options).Succeeded);

        options.Routing.MaxPayloadBytes = 4 * 1024 * 1024; // no frame headroom
        Assert.True(new CoLibraOptionsValidator().Validate(null, options).Failed);

        options.Routing.MaxPayloadBytes = 1024;
        options.Routing.AssignmentAckTimeout = options.Routing.DeliveryTimeout;
        Assert.True(new CoLibraOptionsValidator().Validate(null, options).Failed);
    }
}
