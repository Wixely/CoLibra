namespace CoLibra.Sample.PrimeGrid;

/// <summary>Segmented sieve of Eratosthenes: counts primes in an arbitrary [start, end) window.</summary>
internal static class SegmentedSieve
{
    /// <summary>All primes up to sqrt(<paramref name="max"/>), enough to sieve any segment below max.</summary>
    public static IReadOnlyList<long> BasePrimes(long max)
    {
        var limit = (int)Math.Sqrt(max) + 1;
        var composite = new bool[limit + 1];
        var primes = new List<long>();
        for (var p = 2; p <= limit; p++)
        {
            if (composite[p])
                continue;
            primes.Add(p);
            for (var multiple = (long)p * p; multiple <= limit; multiple += p)
                composite[(int)multiple] = true;
        }

        return primes;
    }

    public static (long Count, long Largest) SieveRange(long start, long end, IReadOnlyList<long> basePrimes)
    {
        var length = checked((int)(end - start));
        var composite = new bool[length];
        foreach (var prime in basePrimes)
        {
            if (prime * prime >= end)
                break;
            // First multiple of prime in the window, but never the prime itself.
            var first = Math.Max(prime * prime, (start + prime - 1) / prime * prime);
            for (var multiple = first; multiple < end; multiple += prime)
                composite[multiple - start] = true;
        }

        long count = 0, largest = 0;
        for (var i = 0; i < length; i++)
        {
            var n = start + i;
            if (n < 2 || composite[i])
                continue;
            count++;
            largest = n;
        }

        return (count, largest);
    }
}
