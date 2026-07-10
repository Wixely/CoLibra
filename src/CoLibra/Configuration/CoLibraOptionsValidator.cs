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

        if (options.CompletionTracking.Enabled)
        {
            if (options.CompletionTracking.MaxEntriesPerType < 1_000)
                failures.Add("CompletionTracking.MaxEntriesPerType must be at least 1000.");

            if (options.CompletionTracking.Retention is { } retention && retention <= options.LeaseTtl)
                failures.Add("CompletionTracking.Retention must exceed LeaseTtl when set.");
        }

        if (options.Routing.Enabled)
        {
            if (options.Routing.MaxPayloadBytes is < 1 or > 3 * 1024 * 1024)
                failures.Add("Routing.MaxPayloadBytes must be 1 byte to 3 MiB (frame-limit headroom).");

            if (options.Routing.DeliveryTimeout <= TimeSpan.Zero)
                failures.Add("Routing.DeliveryTimeout must be positive.");

            if (options.Routing.AssignmentAckTimeout <= TimeSpan.Zero ||
                options.Routing.AssignmentAckTimeout >= options.Routing.DeliveryTimeout)
                failures.Add("Routing.AssignmentAckTimeout must be positive and smaller than DeliveryTimeout.");

            if (options.Routing.IdleChannelTimeout <= TimeSpan.Zero)
                failures.Add("Routing.IdleChannelTimeout must be positive.");

            if (options.Routing.PayloadSerializer is null)
                failures.Add("Routing.PayloadSerializer must not be null.");
        }

        if (options.Messaging.Enabled)
        {
            if (options.Messaging.MaxPayloadBytes is < 1 or > 3 * 1024 * 1024)
                failures.Add("Messaging.MaxPayloadBytes must be 1 byte to 3 MiB (frame-limit headroom).");

            if (options.Messaging.DeliveryTimeout <= TimeSpan.Zero)
                failures.Add("Messaging.DeliveryTimeout must be positive.");

            if (options.Messaging.PayloadSerializer is null)
                failures.Add("Messaging.PayloadSerializer must not be null.");
        }

        if (options.NodeName is { } nodeName && (nodeName.Length == 0 || nodeName.Length > 256))
            failures.Add("NodeName must be 1-256 characters when set.");

        return failures.Count > 0
            ? ValidateOptionsResult.Fail(failures)
            : ValidateOptionsResult.Success;
    }
}
