using CoLibra.Protocol;
using CoLibra.Security;
using Microsoft.Extensions.Time.Testing;

namespace CoLibra.Tests;

public class DiscoveryCodecTests
{
    private static DiscoveryCodec Codec(string serviceId = "svc", string secret = "s3cret", FakeTimeProvider? time = null) =>
        new(new ClusterKeys(serviceId, secret), serviceId, time ?? new FakeTimeProvider());

    private static ProbeMessage Probe() => new(Guid.NewGuid(), "1.0");

    [Fact]
    public void RoundTrips_signed_datagram()
    {
        var time = new FakeTimeProvider();
        var codec = Codec(time: time);
        var decoded = codec.TryDecode(codec.Encode(Probe()));
        Assert.IsType<ProbeMessage>(decoded);
    }

    [Fact]
    public void Rejects_wrong_secret()
    {
        var sender = Codec(secret: "alpha");
        var receiver = Codec(secret: "beta");
        Assert.Null(receiver.TryDecode(sender.Encode(Probe())));
    }

    [Fact]
    public void Rejects_other_service_id()
    {
        var sender = Codec(serviceId: "orders");
        var receiver = Codec(serviceId: "payments");
        Assert.Null(receiver.TryDecode(sender.Encode(Probe())));
    }

    [Fact]
    public void Rejects_replayed_datagram()
    {
        var time = new FakeTimeProvider();
        var codec = Codec(time: time);
        var datagram = codec.Encode(Probe());
        Assert.NotNull(codec.TryDecode(datagram));
        Assert.Null(codec.TryDecode(datagram));
    }

    [Fact]
    public void Rejects_stale_timestamp()
    {
        var senderTime = new FakeTimeProvider();
        var receiverTime = new FakeTimeProvider();
        var keys = new ClusterKeys("svc", "s3cret");
        var sender = new DiscoveryCodec(keys, "svc", senderTime);
        var receiver = new DiscoveryCodec(keys, "svc", receiverTime);

        var datagram = sender.Encode(Probe());
        receiverTime.Advance(TimeSpan.FromMinutes(2));
        Assert.Null(receiver.TryDecode(datagram));
    }

    [Fact]
    public void Rejects_tampered_payload()
    {
        var codec = Codec();
        var datagram = codec.Encode(Probe());
        datagram[^40] ^= 0xFF; // flip a byte in the body
        Assert.Null(codec.TryDecode(datagram));
    }

    [Fact]
    public void Rejects_truncated_datagram()
    {
        var codec = Codec();
        var datagram = codec.Encode(Probe());
        Assert.Null(codec.TryDecode(datagram.AsSpan(0, 10)));
    }
}
