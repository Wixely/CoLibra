using System.Security.Cryptography;
using CoLibra.Protocol;
using CoLibra.Transport;

namespace CoLibra.Security;

/// <summary>
/// Mutual authentication inside the (already encrypted) mesh channel: HMAC challenge-response
/// keyed by the handshake key derived from the shared secret. TLS provides confidentiality;
/// this proves both ends hold the cluster secret and binds their claimed NodeIds.
/// </summary>
internal static class Handshake
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);

    public static async Task<(NodeId NodeId, long Incarnation)> AsServerAsync(
        IMessageChannel channel, ClusterKeys keys, NodeId localId, long localIncarnation, CancellationToken ct)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeout.CancelAfter(Timeout);
        var token = timeout.Token;

        var serverNonce = RandomNumberGenerator.GetBytes(32);
        await channel.SendAsync(new HelloChallengeMessage(serverNonce), token).ConfigureAwait(false);

        if (await channel.ReceiveAsync(token).ConfigureAwait(false) is not HelloProofMessage proof)
            throw new InvalidDataException("Handshake: expected HelloProof.");

        var expected = ComputeProof(keys.HandshakeKey, "client", serverNonce, proof.ClientNonce, proof.NodeId, proof.Incarnation);
        if (!CryptographicOperations.FixedTimeEquals(expected, proof.Proof))
            throw new UnauthorizedAccessException("Handshake: client proof rejected (shared secret mismatch).");

        var serverProof = ComputeProof(keys.HandshakeKey, "server", proof.ClientNonce, serverNonce, localId.Value, localIncarnation);
        await channel.SendAsync(new HelloAckMessage(localId.Value, localIncarnation, serverProof), token).ConfigureAwait(false);

        return (new NodeId(proof.NodeId), proof.Incarnation);
    }

    public static async Task<(NodeId NodeId, long Incarnation)> AsClientAsync(
        IMessageChannel channel, ClusterKeys keys, NodeId localId, long localIncarnation, CancellationToken ct)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeout.CancelAfter(Timeout);
        var token = timeout.Token;

        if (await channel.ReceiveAsync(token).ConfigureAwait(false) is not HelloChallengeMessage challenge)
            throw new InvalidDataException("Handshake: expected HelloChallenge.");

        var clientNonce = RandomNumberGenerator.GetBytes(32);
        var clientProof = ComputeProof(keys.HandshakeKey, "client", challenge.ServerNonce, clientNonce, localId.Value, localIncarnation);
        await channel.SendAsync(new HelloProofMessage(localId.Value, localIncarnation, clientNonce, clientProof), token).ConfigureAwait(false);

        if (await channel.ReceiveAsync(token).ConfigureAwait(false) is not HelloAckMessage ack)
            throw new InvalidDataException("Handshake: expected HelloAck.");

        var expected = ComputeProof(keys.HandshakeKey, "server", clientNonce, challenge.ServerNonce, ack.NodeId, ack.Incarnation);
        if (!CryptographicOperations.FixedTimeEquals(expected, ack.Proof))
            throw new UnauthorizedAccessException("Handshake: server proof rejected (shared secret mismatch).");

        return (new NodeId(ack.NodeId), ack.Incarnation);
    }

    private static byte[] ComputeProof(byte[] key, string role, byte[] nonce1, byte[] nonce2, Guid nodeId, long incarnation)
    {
        var material = new byte[role.Length + nonce1.Length + nonce2.Length + 16 + 8];
        var offset = 0;
        foreach (var c in role)
            material[offset++] = (byte)c;
        nonce1.CopyTo(material, offset);
        offset += nonce1.Length;
        nonce2.CopyTo(material, offset);
        offset += nonce2.Length;
        nodeId.TryWriteBytes(material.AsSpan(offset));
        offset += 16;
        BitConverter.TryWriteBytes(material.AsSpan(offset), incarnation);
        return HMACSHA256.HashData(key, material);
    }
}
