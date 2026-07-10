using System.Security.Cryptography;
using System.Text;

namespace CoLibra.Security;

/// <summary>
/// Derives the per-purpose HMAC keys from the configured shared secret via HKDF-SHA256,
/// so discovery signing and handshake proofs can never be replayed across purposes.
/// </summary>
internal sealed class ClusterKeys
{
    private const int KeyLength = 32;

    public byte[] DiscoveryKey { get; }
    public byte[] HandshakeKey { get; }
    public byte[] UdpLinkKey { get; }

    public ClusterKeys(string serviceId, string sharedSecret)
    {
        var ikm = Encoding.UTF8.GetBytes(sharedSecret);
        var salt = SHA256.HashData(Encoding.UTF8.GetBytes("colibra:" + serviceId));
        DiscoveryKey = HKDF.DeriveKey(HashAlgorithmName.SHA256, ikm, KeyLength, salt, "colibra-discovery"u8.ToArray());
        HandshakeKey = HKDF.DeriveKey(HashAlgorithmName.SHA256, ikm, KeyLength, salt, "colibra-handshake"u8.ToArray());
        UdpLinkKey = HKDF.DeriveKey(HashAlgorithmName.SHA256, ikm, KeyLength, salt, "colibra-udp-link"u8.ToArray());
    }
}
