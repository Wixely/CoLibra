using CoLibra;
using CoLibra.Sample.PrimeGrid;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

// CoLibra prime-grid sample: distributed prime counting with zero orchestration code.
//
// The number space up to --Target is carved into buckets of --RangeSize numbers. Every
// instance runs the same scan loop and asks CoLibra for each bucket; exactly one instance
// gets to sieve any given bucket, and a finished bucket's lease is deliberately kept — a
// held lease doubles as the cluster-wide "done" marker. Kill an instance and its buckets
// (done and in-progress) are released by the cluster and recomputed by the survivors.
//
// Try it: run 2-3 instances (same settings!) and watch them split the work; the per-node
// prime counts always sum to the true value because no bucket is ever double-counted.
//
//   dotnet run                                        # 2 billion numbers, 1M buckets
//   dotnet run -- --RangeSize 250000                  # smaller buckets for slower machines
//   dotnet run -- --Target 100000000 --RangeSize 500000
//
// All instances must use the same RangeSize: the bucket size is embedded in the lease type,
// so instances with mismatched settings simply never share work (they won't corrupt each other).

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

var rangeSize = builder.Configuration.GetValue("RangeSize", 1_000_000L);
var target = builder.Configuration.GetValue("Target", 2_000_000_000L);
if (rangeSize is < 1_000 or > 1_000_000_000)
    throw new ArgumentException("RangeSize must be between 1,000 and 1,000,000,000.");
if (target <= rangeSize)
    throw new ArgumentException("Target must be larger than RangeSize.");

builder.Services.AddSingleton(new PrimeGridOptions { RangeSize = rangeSize, Target = target });

builder.Services.AddCoLibra(options =>
{
    options.ServiceId = "colibra-sample-primegrid";
    options.SharedSecret = "primegrid-demo-secret-change-me";
    options.ServiceVersion = new Version(1, 0, 0);

    // Snappier demo timings than the production defaults, so a killed instance's
    // buckets are reclaimed within seconds instead of within LeaseTtl=15s.
    options.LeaseTtl = TimeSpan.FromSeconds(6);
    options.LeaseRenewSafetyMargin = TimeSpan.FromSeconds(2);
    options.MemberTimeout = TimeSpan.FromSeconds(3);
});

builder.Services.AddHostedService<PrimeWorker>();

await builder.Build().RunAsync();
