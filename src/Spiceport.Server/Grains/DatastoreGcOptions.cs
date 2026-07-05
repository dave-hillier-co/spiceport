namespace Spiceport.Grains;

/// <summary>
/// Configuration for the datastore grain's reminder-driven MVCC garbage collection (see
/// <see cref="DatastoreGrain.RunGc"/>). Bound via the standard Orleans/host options pattern
/// (<c>IOptions&lt;DatastoreGcOptions&gt;</c>); a host that never configures it gets these defaults.
/// </summary>
public sealed class DatastoreGcOptions
{
    /// <summary>
    /// How far back MVCC history is retained. A GC run collects everything strictly below
    /// <c>min(head, now - Window)</c>. Default 24h (mirrors the existing <c>ReferenceDatastore</c>/
    /// <c>DatastoreGrain</c> retention window).
    /// </summary>
    public TimeSpan Window { get; init; } = TimeSpan.FromHours(24);

    /// <summary>
    /// How often the grain's own reminder fires <see cref="DatastoreGrain.RunGc"/>. Orleans reminders
    /// reject a period under one minute, so this is clamped to that floor regardless of what is
    /// configured. Default 1h.
    /// </summary>
    public TimeSpan ReminderPeriod { get; init; } = TimeSpan.FromHours(1);

    /// <summary>
    /// Whether the grain registers its GC reminder on activation. Default true. A host with no reminder
    /// service configured still activates the grain (registration failure is caught and logged, not
    /// fatal) — this flag is for deliberately opting a cluster out (e.g. a test that must not run GC).
    /// </summary>
    public bool ReminderEnabled { get; init; } = true;

    /// <summary>The minimum reminder period Orleans accepts.</summary>
    public static readonly TimeSpan MinimumReminderPeriod = TimeSpan.FromMinutes(1);
}
