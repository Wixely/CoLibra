using CoLibra.Security;
using Microsoft.Extensions.Logging.Abstractions;

namespace CoLibra.Tests;

public class CertificateProviderTests : IDisposable
{
    private readonly string _directory = Path.Combine(Path.GetTempPath(), "colibra-tests", Guid.NewGuid().ToString("N"));

    [Fact]
    public void Generates_and_persists_on_first_start_then_reloads()
    {
        var path = Path.Combine(_directory, "svc.pfx");

        var first = CertificateProvider.GetOrCreate(path, "svc", NullLogger.Instance);
        Assert.True(File.Exists(path));
        Assert.True(first.HasPrivateKey);
        Assert.Contains("colibra-svc", first.Subject);

        var second = CertificateProvider.GetOrCreate(path, "svc", NullLogger.Instance);
        Assert.Equal(first.Thumbprint, second.Thumbprint);
    }

    [Fact]
    public void Falls_back_to_ephemeral_when_path_is_unwritable()
    {
        // A directory path that cannot be a file: persisting fails, but a certificate is still returned.
        var path = Path.Combine(_directory, "made", "\0invalid", "svc.pfx");
        var certificate = CertificateProvider.GetOrCreate(path, "svc", NullLogger.Instance);
        Assert.True(certificate.HasPrivateKey);
    }

    public void Dispose()
    {
        if (Directory.Exists(_directory))
            Directory.Delete(_directory, recursive: true);
    }
}
