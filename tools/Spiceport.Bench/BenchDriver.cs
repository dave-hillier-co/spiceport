using System.Diagnostics;
using Spiceport.Core;

namespace Spiceport.Bench;

/// <summary>Percentile summary of one op class's measured latencies (microseconds).</summary>
/// <param name="Count">Measured (post-warmup) completions.</param>
/// <param name="Errors">Measured completions that threw.</param>
/// <param name="SampleCount">
/// Samples the percentiles were actually computed over. Equals <paramref name="Count"/> unless the
/// recorder's ring wrapped, in which case only the newest <paramref name="SampleCount"/> samples
/// survived (see <see cref="Wrapped"/>).
/// </param>
/// <param name="P50Micros">Median, microseconds.</param>
/// <param name="P90Micros">90th percentile.</param>
/// <param name="P99Micros">99th percentile.</param>
/// <param name="P999Micros">99.9th percentile (the sawtooth proxy: compare against P50).</param>
/// <param name="MaxMicros">Largest observed.</param>
/// <param name="FirstError">The first thrown op's message (diagnosis aid), or null when error-free.</param>
public readonly record struct LatencySummary(
    long Count, long Errors, long SampleCount,
    long P50Micros, long P90Micros, long P99Micros, long P999Micros, long MaxMicros,
    string? FirstError = null)
{
    /// <summary>Formats a microsecond value as milliseconds with three decimals (the table cell format).</summary>
    public string Ms(long micros) => (micros / 1000.0).ToString("0.000");

    /// <summary>True when the recorder's ring wrapped: percentiles cover only the newest <see cref="SampleCount"/> of <see cref="Count"/>.</summary>
    public bool Wrapped => Count > SampleCount;

    /// <summary>
    /// A warning line to print when the ring wrapped (percentiles are then over a sample, not the
    /// full measured population), or null when every recorded op is in the percentile set.
    /// </summary>
    public string? SampleWarning(string opClass) => Wrapped
        ? $"warning: {opClass} latency ring wrapped ({Count} recorded > {SampleCount} capacity); " +
          $"percentiles are computed over a SAMPLE (the newest {SampleCount} ops)"
        : null;
}

/// <summary>
/// Fixed-capacity latency ring per op class: a preallocated <c>long[]</c> of microsecond samples
/// written with an interlocked cursor (wrapping overwrites the oldest — capacity defaults high
/// enough that harness-scale runs never wrap). Percentiles are computed AFTER the run by sorting a
/// snapshot — no histogram package, no on-path bucketing cost. Warmup exclusion is the caller's:
/// drivers only call <see cref="Record"/> for ops that STARTED after the warmup window.
/// </summary>
public sealed class LatencyRecorder(int capacity = 1 << 20)
{
    private readonly long[] _micros = new long[capacity];
    private long _next;
    private long _errors;
    private string? _firstError;

    /// <summary>Records one successful op's latency in microseconds (thread-safe, wait-free).</summary>
    public void Record(long micros)
    {
        var i = Interlocked.Increment(ref _next) - 1;
        _micros[i % _micros.Length] = micros;
    }

    /// <summary>Counts one failed op and retains the FIRST failure's message as the diagnosis aid.</summary>
    public void RecordError(Exception ex)
    {
        Interlocked.Increment(ref _errors);
        Interlocked.CompareExchange(ref _firstError, ex.Message, null);
    }

    /// <summary>
    /// Sorts a snapshot of the ring and computes the percentile summary. When the ring wrapped
    /// (recorded > capacity) the summary's <see cref="LatencySummary.SampleCount"/> is smaller than
    /// its <see cref="LatencySummary.Count"/> — callers print <see cref="LatencySummary.SampleWarning"/>.
    /// </summary>
    public LatencySummary Summarize()
    {
        var total = Interlocked.Read(ref _next);
        var n = (int)Math.Min(total, _micros.Length);
        var samples = new long[n];
        Array.Copy(_micros, samples, n);
        Array.Sort(samples);
        long At(double p) => n == 0 ? 0 : samples[Math.Max(0, Math.Min(n - 1, (int)Math.Ceiling(p * n) - 1))];
        return new LatencySummary(
            total, Interlocked.Read(ref _errors), n,
            At(0.50), At(0.90), At(0.99), At(0.999), n == 0 ? 0 : samples[n - 1],
            Volatile.Read(ref _firstError));
    }
}

/// <summary>Throughput observed during the measured (post-warmup) window of one driver run.</summary>
/// <param name="MeasuredCompletionsPerSecond">Completions per second inside the measured window.</param>
/// <param name="MaxInFlight">The high-water mark of concurrently in-flight ops (open-loop backlog signal).</param>
public readonly record struct DriverResult(double MeasuredCompletionsPerSecond, int MaxInFlight);

/// <summary>
/// The two workload drivers. OPEN-LOOP paces arrivals on an absolute schedule and never waits for
/// the previous op — a late op does NOT shift subsequent arrivals, which is precisely the defense
/// against coordinated omission (a closed loop silently stops sampling while it is stuck waiting,
/// under-reporting the tail). CLOSED-LOOP runs N workers back-to-back; use it for saturation-style
/// load, but read its tail latencies with the coordinated-omission caveat in mind (see --help).
/// </summary>
public static class BenchDriver
{
    private static long ElapsedMicros(long fromTimestamp) =>
        (long)Stopwatch.GetElapsedTime(fromTimestamp).TotalMicroseconds;

    /// <summary>
    /// Open-loop pacing at <paramref name="ratePerSecond"/> for warmup+duration; ops that start
    /// inside the warmup window run but are not recorded. All in-flight ops are awaited before
    /// returning, so metrics snapshots taken afterwards cover every measured op.
    /// </summary>
    public static async Task<DriverResult> RunOpenLoop(
        double ratePerSecond, TimeSpan warmup, TimeSpan duration, Func<Task> op, LatencyRecorder recorder)
    {
        if (ratePerSecond <= 0)
            return new DriverResult(0, 0);

        var total = warmup + duration;
        var inFlight = 0;
        var maxInFlight = 0;
        long measuredCompletions = 0;
        var pending = new List<Task>();
        var sw = Stopwatch.StartNew();

        async Task Fire(bool measured)
        {
            var t0 = Stopwatch.GetTimestamp();
            try
            {
                await op().ConfigureAwait(false);
                if (measured)
                {
                    recorder.Record(ElapsedMicros(t0));
                    Interlocked.Increment(ref measuredCompletions);
                }
            }
            catch (Exception ex)
            {
                if (measured)
                    recorder.RecordError(ex);
            }
            finally
            {
                Interlocked.Decrement(ref inFlight);
            }
        }

        for (long tick = 0; ; tick++)
        {
            var target = TimeSpan.FromSeconds(tick / ratePerSecond);
            if (target >= total)
                break;
            var wait = target - sw.Elapsed;
            if (wait > TimeSpan.Zero)
                await Task.Delay(wait).ConfigureAwait(false);
            // The arrival fires at its ABSOLUTE scheduled time regardless of prior ops' fates.
            var current = Interlocked.Increment(ref inFlight);
            InterlockedMax(ref maxInFlight, current);
            var measured = sw.Elapsed >= warmup;
            // Task.Run, not a direct call: an async method runs its SYNCHRONOUS PREFIX (everything up
            // to the op's first true suspension) inline on the caller — here, the pacer loop — so an
            // op with an expensive prefix would delay every SUBSEQUENT arrival and skew the schedule.
            // Queuing the whole body to the pool keeps arrival times independent of op internals.
            pending.Add(Task.Run(() => Fire(measured)));
        }

        await Task.WhenAll(pending).ConfigureAwait(false);
        return new DriverResult(measuredCompletions / duration.TotalSeconds, maxInFlight);
    }

    /// <summary>
    /// Closed-loop: <paramref name="workers"/> workers each issue ops back-to-back until
    /// warmup+duration elapses; only ops started after warmup are recorded. The op receives the
    /// worker index (for per-worker RNGs) and a per-worker op counter.
    /// </summary>
    public static async Task<DriverResult> RunClosedLoop(
        int workers, TimeSpan warmup, TimeSpan duration, Func<int, long, Task> op, LatencyRecorder recorder)
    {
        var total = warmup + duration;
        long measuredCompletions = 0;
        var sw = Stopwatch.StartNew();

        async Task Worker(int worker)
        {
            for (long i = 0; sw.Elapsed < total; i++)
            {
                var measured = sw.Elapsed >= warmup;
                var t0 = Stopwatch.GetTimestamp();
                try
                {
                    await op(worker, i).ConfigureAwait(false);
                    if (measured)
                    {
                        recorder.Record(ElapsedMicros(t0));
                        Interlocked.Increment(ref measuredCompletions);
                    }
                }
                catch (Exception ex)
                {
                    if (measured)
                        recorder.RecordError(ex);
                }
            }
        }

        await Task.WhenAll(Enumerable.Range(0, workers).Select(Worker)).ConfigureAwait(false);
        return new DriverResult(measuredCompletions / duration.TotalSeconds, workers);
    }

    private static void InterlockedMax(ref int location, int value)
    {
        int current;
        while (value > (current = Volatile.Read(ref location)))
        {
            if (Interlocked.CompareExchange(ref location, value, current) == current)
                break;
        }
    }
}

/// <summary>
/// Hand-rolled seeded Zipf sampler over ranks 0..n-1 (rank 0 hottest): the CDF of
/// 1/(rank+1)^exponent is precomputed and each draw binary-searches it — deterministic given the
/// caller's <see cref="Random"/>, no package. Exponent ~1 is the classic web-skew shape.
/// </summary>
public sealed class ZipfSampler
{
    private readonly double[] _cdf;

    /// <summary>Precomputes the CDF over ranks 0..<paramref name="n"/>-1 with the given skew exponent.</summary>
    /// <param name="n">Alphabet size (number of ranks); must be at least 1.</param>
    /// <param name="exponent">Skew: larger concentrates more mass on the hottest ranks; ~1 is web-like.</param>
    public ZipfSampler(int n, double exponent = 1.0)
    {
        if (n < 1)
            throw new ArgumentOutOfRangeException(nameof(n));
        _cdf = new double[n];
        var sum = 0.0;
        for (var i = 0; i < n; i++)
            _cdf[i] = sum += 1.0 / Math.Pow(i + 1, exponent);
        for (var i = 0; i < n; i++)
            _cdf[i] /= sum;
    }

    /// <summary>Draws a rank using the caller's RNG (pass a per-worker RNG for lock-free concurrency).</summary>
    public int Next(Random rng)
    {
        var u = rng.NextDouble();
        var i = Array.BinarySearch(_cdf, u);
        return i >= 0 ? i : Math.Min(~i, _cdf.Length - 1);
    }
}

/// <summary>
/// A consistency mix as integer percentages (summing to 100) over the three read modes the program
/// sweeps: minimize-latency / at-least-as-fresh / fully-consistent. At-least-as-fresh uses the
/// token from the CLIENT'S OWN last observed write (read-your-writes, the realistic shape); before
/// any write completes it falls back to the world-load token the caller seeds.
/// </summary>
public sealed record ConsistencyMix(int MinimizeLatencyPercent, int AtLeastAsFreshPercent, int FullyConsistentPercent)
{
    public static ConsistencyMix Parse(string spec)
    {
        var parts = spec.Split('/');
        if (parts.Length != 3
            || !int.TryParse(parts[0], out var min)
            || !int.TryParse(parts[1], out var fresh)
            || !int.TryParse(parts[2], out var full)
            || min + fresh + full != 100 || min < 0 || fresh < 0 || full < 0)
        {
            throw new BenchUsageException(
                $"Consistency mix '{spec}' must be three non-negative percentages summing to 100, " +
                "e.g. 70/20/10 (minimize-latency/at-least-as-fresh/fully-consistent).");
        }
        return new ConsistencyMix(min, fresh, full);
    }

    /// <summary>Rolls one op's consistency; <paramref name="lastWriteToken"/> supplies the freshness floor.</summary>
    public ConsistencyRequirement Pick(Random rng, Func<string> lastWriteToken)
    {
        var roll = rng.Next(100);
        if (roll < MinimizeLatencyPercent)
            return ConsistencyRequirement.MinimizeLatency;
        if (roll < MinimizeLatencyPercent + AtLeastAsFreshPercent)
            return ConsistencyRequirement.AtLeastAsFresh(new ZedToken(lastWriteToken()));
        return ConsistencyRequirement.FullyConsistent;
    }

    public override string ToString() =>
        $"{MinimizeLatencyPercent}/{AtLeastAsFreshPercent}/{FullyConsistentPercent}";
}
