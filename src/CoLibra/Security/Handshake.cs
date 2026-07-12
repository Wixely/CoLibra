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

        // Channel binding: the cert WE presented over TLS. A relaying MITM would present its own cert
        // to the client, so the client's binding would differ and its proof would not verify here.
        var binding = channel.LocalCertificateHash;
        var expected = ComputeProof(keys.HandshakeKey, "client", serverNonce, proof.ClientNonce, proof.NodeId, proof.Incarnation, binding);
        if (!CryptographicOperations.FixedTimeEquals(expected, proof.Proof))
            throw new UnauthorizedAccessException("Handshake: client proof rejected (shared secret mismatch or MITM).");

        var serverProof = ComputeProof(keys.HandshakeKey, "server", proof.ClientNonce, serverNonce, localId.Value, localIncarnation, binding);
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

        // Channel binding: the server cert WE saw over TLS. Under a relaying MITM this is the
        // attacker's cert, not the real server's, so our proof won't verify at the real server.
        var binding = channel.RemoteCertificateHash;
        var clientNonce = RandomNumberGenerator.GetBytes(32);
        var clientProof = ComputeProof(keys.HandshakeKey, "client", challenge.ServerNonce, clientNonce, localId.Value, localIncarnation, binding);
        await channel.SendAsync(new HelloProofMessage(localId.Value, localIncarnation, clientNonce, clientProof), token).ConfigureAwait(false);

        if (await channel.ReceiveAsync(token).ConfigureAwait(false) is not HelloAckMessage ack)
            throw new InvalidDataException("Handshake: expected HelloAck.");

        var expected = ComputeProof(keys.HandshakeKey, "server", clientNonce, challenge.ServerNonce, ack.NodeId, ack.Incarnation, binding);
        if (!CryptographicOperations.FixedTimeEquals(expected, ack.Proof))
            throw new UnauthorizedAccessException("Handshake: server proof rejected (shared secret mismatch).");

        return (new NodeId(ack.NodeId), ack.Incarnation);
    }

    private static byte[] ComputeProof(byte[] key, string role, byte[] nonce1, byte[] nonce2, Guid nodeId, long incarnation, byte[] binding)
    {
        var material = new byte[role.Length + nonce1.Length + nonce2.Length + 16 + 8 + binding.Length];
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
        offset += 8;
        binding.CopyTo(material.AsSpan(offset));
        return HMACSHA256.HashData(key, material);
    }
}
