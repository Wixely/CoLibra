using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace CoLibra.Sample.Router;

// CoLibra routed-delivery sample: the single-receiver pattern.
//
// Unlike the broadcast-feed pattern (every instance sees every message), here each instance
// receives updates that ONLY IT sees — as if a load balancer sprayed requests across the
// fleet. Instances hand each update to CoLibra's Router, which delivers it to whichever node
// owns the update's order id, force-assigning an owner on first contact. The result: every
// order's updates are processed by exactly one node, in arrival order per origin, no matter
// which node received them from the outside world.
//
// Try it: run 2-3 instances, watch orders spread across nodes and every update reach its
// owner. Kill the instance owning an order and route again — a survivor is assigned.
internal static class Program
{
    private static async Task Main(string[] args)
    {
        var builder = Host.CreateApplicationBuilder(args);

        builder.Logging.ClearProviders();
        builder.Logging.AddSimpleConsole(o =>
        {
            o.SingleLine = true;
            o.TimestampFormat = "HH:mm:ss ";
        });
        // Quieten the library's own logger (exact category "CoLibra") so the demo output stays readable.
        builder.Logging.AddFilter((category, level) =>
            category == "CoLibra" ? level >= LogLevel.Warning : level >= LogLevel.Information);

        builder.Services.AddCoLibra(options =>
        {
            options.ServiceId = "colibra-sample-router";
            options.SharedSecret = "router-demo-secret-change-me";
            options.ServiceVersion = new Version(1, 0, 0);
            options.Routing.Enabled = true;

            // Snappier demo timings than the production defaults.
            options.LeaseTtl = TimeSpan.FromSeconds(6);
            options.LeaseRenewSafetyMargin = TimeSpan.FromSeconds(2);
            options.MemberTimeout = TimeSpan.FromSeconds(3);
        });

        builder.Services.AddHostedService<OrderRouterWorker>();

        await builder.Build().RunAsync();
    }
}
