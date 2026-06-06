namespace Spiceport.Core;

/// <summary>
/// A revision based on a nanosecond timestamp since the Unix epoch.
/// </summary>
/// <param name="TimestampNanosSinceEpoch">Nanoseconds since 1970-01-01T00:00:00Z.</param>
public sealed record TimestampRevision(long TimestampNanosSinceEpoch) : IRevision
{
    /// <inheritdoc />
    public bool ByteSortable => true;

    /// <summary>This revision as a <see cref="DateTimeOffset"/> (truncated to 100ns ticks).</summary>
    public DateTimeOffset AsDateTimeOffset =>
        DateTimeOffset.UnixEpoch.AddTicks(TimestampNanosSinceEpoch / 100);

    /// <inheritdoc />
    public override string ToString() => TimestampNanosSinceEpoch.ToString(System.Globalization.CultureInfo.InvariantCulture);

    /// <summary>Parses a timestamp revision from its string form.</summary>
    public static TimestampRevision Parse(string value) =>
        new(long.Parse(value, System.Globalization.CultureInfo.InvariantCulture));

    /// <inheritdoc />
    public int CompareTo(IRevision? other) => other switch
    {
        null => 1,
        TimestampRevision t => TimestampNanosSinceEpoch.CompareTo(t.TimestampNanosSinceEpoch),
        _ => string.CompareOrdinal(ToString(), other.ToString()),
    };

    /// <inheritdoc />
    public bool Equals(IRevision? other) =>
        other is TimestampRevision t && t.TimestampNanosSinceEpoch == TimestampNanosSinceEpoch;

    /// <inheritdoc />
    public override int GetHashCode() => TimestampNanosSinceEpoch.GetHashCode();
}
