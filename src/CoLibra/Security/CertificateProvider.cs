using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Microsoft.Extensions.Logging;

namespace CoLibra.Security;

/// <summary>
/// Loads the node's TLS certificate, generating a self-signed one on first startup and
/// persisting it next to the service so encryption works with zero certificate setup.
/// The certificate provides confidentiality only; authentication comes from the shared secret.
/// </summary>
internal static class CertificateProvider
{
    // SChannel (Windows TLS) cannot use ephemeral private keys for server authentication,
    // so on Windows the key goes into a temporary user key container instead.
    private static X509KeyStorageFlags StorageFlags => OperatingSystem.IsWindows()
        ? X509KeyStorageFlags.Exportable | X509KeyStorageFlags.DefaultKeySet
        : X509KeyStorageFlags.Exportable | X509KeyStorageFlags.EphemeralKeySet;

    public static X509Certificate2 GetOrCreate(string path, string serviceId, ILogger logger)
    {
        if (File.Exists(path))
        {
            try
            {
                var existing = X509CertificateLoader.LoadPkcs12FromFile(path, password: null, StorageFlags);
                if (existing.NotAfter > DateTimeOffset.UtcNow.AddDays(30))
                    return existing;
                logger.LogInformation("CoLibra certificate at {Path} expires soon; regenerating", path);
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "Could not load CoLibra certificate at {Path}; regenerating", path);
            }
        }

        var certificate = Generate(serviceId);
        try
        {
            Directory.CreateDirectory(Path.GetDirectoryName(path)!);
            File.WriteAllBytes(path, certificate.Export(X509ContentType.Pkcs12));
            if (!OperatingSystem.IsWindows())
                File.SetUnixFileMode(path, UnixFileMode.UserRead | UnixFileMode.UserWrite);
            logger.LogInformation("Generated self-signed CoLibra certificate at {Path}", path);
        }
        catch (Exception ex)
        {
            // A read-only install directory is survivable: the cert just regenerates per run.
            logger.LogWarning(ex, "Could not persist CoLibra certificate to {Path}; using an ephemeral one", path);
        }

        return certificate;
    }

    private static X509Certificate2 Generate(string serviceId)
    {
        using var key = ECDsa.Create(ECCurve.NamedCurves.nistP256);
        var request = new CertificateRequest($"CN=colibra-{serviceId}", key, HashAlgorithmName.SHA256);
        request.CertificateExtensions.Add(new X509KeyUsageExtension(
            X509KeyUsageFlags.DigitalSignature | X509KeyUsageFlags.KeyAgreement, critical: false));
        request.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension(
            [new Oid("1.3.6.1.5.5.7.3.1"), new Oid("1.3.6.1.5.5.7.3.2")], critical: false)); // server + client auth

        var now = DateTimeOffset.UtcNow;
        using var ephemeral = request.CreateSelfSigned(now.AddDays(-1), now.AddYears(5));
        // Round-trip through PKCS#12 so the private key is usable by SslStream on Windows.
        return X509CertificateLoader.LoadPkcs12(ephemeral.Export(X509ContentType.Pkcs12), password: null, StorageFlags);
    }
}
