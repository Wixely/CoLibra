using System.Net;
using Microsoft.Extensions.Options;

namespace CoLibra;

internal sealed class CoLibraOptionsValidator : IValidateOptions<CoLibraOptions>
{
    public ValidateOptionsResult Validate(string? name, CoLibraOptions options)
    {
        var failures = new List<string>();

        if (string.IsNullOrWhiteSpace(options.ServiceId))
            failures.Add("ServiceId is required.");
        else if (options.ServiceId.Any(char.IsWhiteSpace))
            failures.Add("ServiceId must not contain whitespace.");

        if (string.IsNullOrEmpty(options.SharedSecret))
            failures.Add("SharedSecret is required.");

        if (options.DiscoveryPort is < 1 or > 65535)
            failures.Add("DiscoveryPort must be 1-65535.");

        if (options.MeshPort is < 0 or > 65535)
            failures.Add("MeshPort must be 0 (OS-assigned) or 1-65535.");

        if (!IPAddress.TryParse(options.MulticastAddress, out var multicast) ||
            multicast.AddressFamily != System.Net.Sockets.AddressFamily.InterNetwork ||
            multicast.GetAddressBytes()[0] is < 224 or > 239)
            failures.Add("MulticastAddress must be a valid IPv4 multicast address (224.0.0.0-239.255.255.255).");

        foreach (var seed in options.StaticSeeds)
        {
            var colon = seed.LastIndexOf(':');
            if (colon <= 0 || !int.TryParse(seed[(colon + 1)..], out var port) || port is < 1 or > 65535)
                failures.Add($"StaticSeeds entry '{seed}' must be 'host:port'.");
        }

        if (options.HeartbeatInterval <= TimeSpan.Zero)
            failures.Add("HeartbeatInterval must be positive.");

        if (options.MemberTimeout <= options.HeartbeatInterval)
            failures.Add("MemberTimeout must exceed HeartbeatInterval.");

        if (options.LeaseTtl <= options.HeartbeatInterval * 2)
            failures.Add("LeaseTtl must be at least twice HeartbeatInterval.");

        if (options.LeaseRenewSafetyMargin <= TimeSpan.Zero || options.LeaseRenewSafetyMargin >= options.LeaseTtl)
            failures.Add("LeaseRenewSafetyMargin must be positive and smaller than LeaseTtl.");

        if (options.Weight <= 0)
            failures.Add("Weight must be positive.");

        if (options.LoadBalanceTolerance < 0)
            failures.Add("LoadBalanceTolerance must be non-negative.");

        if (options.DecisionCacheMaxEntries < 1)
            failures.Add("DecisionCacheMaxEntries must be at least 1.");

        return failures.Count > 0
            ? ValidateOptionsResult.Fail(failures)
            : ValidateOptionsResult.Success;
    }
}
