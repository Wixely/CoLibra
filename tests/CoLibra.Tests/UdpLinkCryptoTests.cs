using System.Security.Cryptography;
using CoLibra.Security;

namespace CoLibra.Tests;

public class UdpLinkCryptoTests
{
    private static (UdpLinkCrypto Offerer, UdpLinkCrypto Acceptor) Pair(long term = 7)
    {
        var keys = new ClusterKeys("svc", "secret");
        var linkId = Guid.NewGuid();
        var nonceA = RandomNumberGenerator.GetBytes(32);
        var nonceB = RandomNumberGenerator.GetBytes(32);
        return (
            UdpLinkCrypto.Derive(keys, linkId, nonceA, nonceB, term, isOfferer: true),
            UdpLinkCrypto.Derive(keys, linkId, nonceA, nonceB, term, isOfferer: false));
    }

    [Fact]
    public void Round_trips_between_the_two_directions()
    {
        var (offerer, acceptor) = Pair();
        var payload = new byte[] { 1, 2, 3, 4, 5 };

        var datagram = offerer.Seal(UdpLinkCrypto.FlagWantAck, termLow: 7, srcWireId: 2, channelId: 3, payload, out var counter);
        Assert.True(acceptor.TryOpen(datagram, out var flags, out var term, out var wireId, out var channel, out var openedCounter, out var opened));

        Assert.Equal(UdpLinkCrypto.FlagWantAck, flags);
        Assert.Equal(7, term);
        Assert.Equal(2, wireId);
        Assert.Equal(3, channel);
        Assert.Equal(counter, openedCounter);
        Assert.Equal(payload, opened);

        // And the reverse direction with independent keys.
        var reply = acceptor.Seal(UdpLinkCrypto.FlagAck, 7, 1, UdpLinkCrypto.ControlChannel, [9], out _);
        Assert.True(offerer.TryOpen(reply, out _, out _, out _, out _, out _, out var replyPayload));
        Assert.Equal([9], replyPayload);
    }

    [Fact]
    public void Tampering_with_header_or_body_is_rejected()
    {
        var (offerer, acceptor) = Pair();
        var datagram = offerer.Seal(0, 7, 2, 3, [1, 2, 3], out _);

        var headerTampered = (byte[])datagram.Clone();
        headerTampered[4] ^= 0xFF; // srcWireId is AAD — flipping it must break the tag
        Assert.False(acceptor.TryOpen(headerTampered, out _, out _, out _, out _, out _, out _));

        var bodyTampered = (byte[])datagram.Clone();
        bodyTampered[^1] ^= 0x01;
        Assert.False(acceptor.TryOpen(bodyTampered, out _, out _, out _, out _, out _, out _));
    }

    [Fact]
    public void Replayed_datagrams_are_rejected()
    {
        var (offerer, acceptor) = Pair();
        var datagram = offerer.Seal(0, 7, 2, 3, [1], out _);

        Assert.True(acceptor.TryOpen(datagram, out _, out _, out _, out _, out _, out _));
        Assert.False(acceptor.TryOpen(datagram, out _, out _, out _, out _, out _, out _));
    }

    [Fact]
    public void Out_of_order_delivery_within_the_window_is_accepted()
    {
        var (offerer, acceptor) = Pair();
        var first = offerer.Seal(0, 7, 2, 3, [1], out _);
        var second = offerer.Seal(0, 7, 2, 3, [2], out _);

        Assert.True(acceptor.TryOpen(second, out _, out _, out _, out _, out _, out _));
        Assert.True(acceptor.TryOpen(first, out _, out _, out _, out _, out _, out _)); // late but new
        Assert.False(acceptor.TryOpen(first, out _, out _, out _, out _, out _, out _)); // now a replay
    }

    [Fact]
    public void Different_link_parameters_produce_incompatible_keys()
    {
        var keys = new ClusterKeys("svc", "secret");
        var (nonceA, nonceB) = (RandomNumberGenerator.GetBytes(32), RandomNumberGenerator.GetBytes(32));
        var linkId = Guid.NewGuid();
        var offerer = UdpLinkCrypto.Derive(keys, linkId, nonceA, nonceB, term: 7, isOfferer: true);
        var wrongTerm = UdpLinkCrypto.Derive(keys, linkId, nonceA, nonceB, term: 8, isOfferer: false);

        var datagram = offerer.Seal(0, 7, 2, 3, [1], out _);
        Assert.False(wrongTerm.TryOpen(datagram, out _, out _, out _, out _, out _, out _));
    }

    [Fact]
    public void Connection_proof_is_deterministic_and_secret_bound()
    {
        var keys = new ClusterKeys("svc", "secret");
        var other = new ClusterKeys("svc", "different-secret");
        var linkId = Guid.NewGuid();
        var (nonceA, nonceB) = (RandomNumberGenerator.GetBytes(32), RandomNumberGenerator.GetBytes(32));

        Assert.Equal(
            UdpLinkCrypto.ConnectionProof(keys, linkId, nonceA, nonceB),
            UdpLinkCrypto.ConnectionProof(keys, linkId, nonceA, nonceB));
        Assert.NotEqual(
            UdpLinkCrypto.ConnectionProof(keys, linkId, nonceA, nonceB),
            UdpLinkCrypto.ConnectionProof(other, linkId, nonceA, nonceB));
    }
}
