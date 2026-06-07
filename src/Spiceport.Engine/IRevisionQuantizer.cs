using Spiceport.Core;

namespace Spiceport.Engine;

/// <summary>
/// Maps an <see cref="IRevision"/> to a stable, lower-cardinality cache bucket so that checks made
/// within a configurable time window share cache entries.
/// </summary>
/// <remarks>
/// Caching the pre-context dispatch branch is only useful if near-in-time checks collide on the same
/// key. A quantizer floors a revision to a window boundary (e.g. 5s) producing a stable bucket string
/// used as part of the cache key. The bucket need not be a valid revision; it only needs to be stable
/// for all revisions that fall in the same window.
/// </remarks>
public interface IRevisionQuantizer
{
    /// <summary>Returns a stable bucket token for the given revision.</summary>
    /// <param name="revision">The revision to quantize.</param>
    string Quantize(IRevision revision);
}

/// <summary>
/// A <see cref="IRevisionQuantizer"/> that floors a <see cref="TimestampRevision"/> (nanoseconds
/// since the Unix epoch) to a fixed window boundary: <c>floor(nanos / windowNanos) * windowNanos</c>.
/// </summary>
/// <remarks>
/// Non-timestamp revisions (e.g. the in-process sentinel) are not bucketed by time; their stable
/// string form is used verbatim so they still produce a deterministic, collision-correct key.
/// </remarks>
public sealed class TimestampRevisionQuantizer : IRevisionQuantizer
{
    /// <summary>The default quantization window.</summary>
    public static readonly TimeSpan DefaultWindow = TimeSpan.FromSeconds(5);

    private readonly long _windowNanos;

    /// <summary>Creates a quantizer with the given window (defaults to <see cref="DefaultWindow"/>).</summary>
    /// <param name="window">The quantization window; must be positive.</param>
    public TimestampRevisionQuantizer(TimeSpan? window = null)
    {
        var w = window ?? DefaultWindow;
        if (w <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(window), "Quantization window must be positive.");
        // 100ns ticks -> nanoseconds.
        _windowNanos = w.Ticks * 100L;
    }

    /// <inheritdoc/>
    public string Quantize(IRevision revision)
    {
        ArgumentNullException.ThrowIfNull(revision);
        if (revision is TimestampRevision t)
        {
            var bucket = t.TimestampNanosSinceEpoch - Mod(t.TimestampNanosSinceEpoch, _windowNanos);
            return "ts:" + bucket.ToString(System.Globalization.CultureInfo.InvariantCulture);
        }

        // Non-timestamp revision: not time-bucketable; use its stable string form as the bucket.
        return "rev:" + revision.ToString();
    }

    // Floor-division remainder (handles negative nanos correctly so flooring is toward -infinity).
    private static long Mod(long value, long modulus)
    {
        var r = value % modulus;
        return r < 0 ? r + modulus : r;
    }
}
