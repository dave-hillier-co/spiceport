namespace Spiceport.Engine;

/// <summary>Supplies the evaluation "now" used to filter expired relationships during a check.</summary>
public interface IClock
{
    /// <summary>The current instant.</summary>
    DateTimeOffset UtcNow { get; }
}

/// <summary>An <see cref="IClock"/> backed by the system clock (<see cref="DateTimeOffset.UtcNow"/>).</summary>
public sealed class SystemClock : IClock
{
    /// <summary>A shared system-clock instance.</summary>
    public static readonly SystemClock Instance = new();

    /// <inheritdoc/>
    public DateTimeOffset UtcNow => DateTimeOffset.UtcNow;
}

/// <summary>An <see cref="IClock"/> pinned to a fixed instant, for deterministic tests.</summary>
/// <param name="Now">The instant this clock always returns.</param>
public sealed record FixedClock(DateTimeOffset Now) : IClock
{
    /// <inheritdoc/>
    public DateTimeOffset UtcNow => Now;
}
