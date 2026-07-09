namespace CoLibra.Tests;

public class VersionCompatibilityTests
{
    [Theory]
    [InlineData("1.2.3", "1.2.3", true)]
    [InlineData("1.2.3", "1.2.4", false)]
    public void Strict_requires_exact_match(string local, string peer, bool expected) =>
        Assert.Equal(expected, VersionCompatibility.Strict.IsCompatible(Version.Parse(local), Version.Parse(peer)));

    [Theory]
    [InlineData("2.0.0", "2.9.1", true)]
    [InlineData("2.0.0", "3.0.0", false)]
    public void MajorMatch_requires_same_major(string local, string peer, bool expected) =>
        Assert.Equal(expected, VersionCompatibility.MajorMatch.IsCompatible(Version.Parse(local), Version.Parse(peer)));

    [Theory]
    [InlineData("1.5.0", true)]
    [InlineData("1.4.9", false)]
    [InlineData("2.0.0", true)]
    public void Minimum_requires_at_least_the_floor(string peer, bool expected) =>
        Assert.Equal(expected, VersionCompatibility.Minimum(new Version(1, 5)).IsCompatible(new Version(9, 9), Version.Parse(peer)));

    [Fact]
    public void Any_accepts_everything() =>
        Assert.True(VersionCompatibility.Any.IsCompatible(new Version(1, 0), new Version(99, 0)));
}

public class CoLibraOptionsValidatorTests
{
    private static CoLibraOptions Valid() => new() { ServiceId = "svc", SharedSecret = "secret" };

    [Fact]
    public void Accepts_valid_options()
    {
        var result = new CoLibraOptionsValidator().Validate(null, Valid());
        Assert.True(result.Succeeded, string.Join("; ", result.Failures ?? []));
    }

    [Theory]
    [InlineData("")]
    [InlineData("has space")]
    public void Rejects_bad_service_ids(string serviceId)
    {
        var options = Valid();
        options.ServiceId = serviceId;
        Assert.True(new CoLibraOptionsValidator().Validate(null, options).Failed);
    }

    [Fact]
    public void Rejects_missing_secret()
    {
        var options = Valid();
        options.SharedSecret = "";
        Assert.True(new CoLibraOptionsValidator().Validate(null, options).Failed);
    }

    [Theory]
    [InlineData("10.0.0.1")]
    [InlineData("not-an-ip")]
    public void Rejects_non_multicast_addresses(string address)
    {
        var options = Valid();
        options.MulticastAddress = address;
        Assert.True(new CoLibraOptionsValidator().Validate(null, options).Failed);
    }

    [Fact]
    public void Rejects_bad_seed_entries()
    {
        var options = Valid();
        options.StaticSeeds.Add("no-port-here");
        Assert.True(new CoLibraOptionsValidator().Validate(null, options).Failed);
    }

    [Fact]
    public void Rejects_lease_ttl_shorter_than_heartbeats()
    {
        var options = Valid();
        options.LeaseTtl = TimeSpan.FromSeconds(1);
        Assert.True(new CoLibraOptionsValidator().Validate(null, options).Failed);
    }

    [Fact]
    public void Certificate_path_defaults_next_to_the_service()
    {
        var options = Valid();
        var path = options.ResolveCertificatePath();
        Assert.StartsWith(AppContext.BaseDirectory, path);
        Assert.EndsWith(Path.Combine("colibra", "svc.pfx"), path);
    }
}
