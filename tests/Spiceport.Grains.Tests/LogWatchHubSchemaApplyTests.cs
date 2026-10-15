using System.Text;

namespace Spiceport.Grains.Tests;

/// <summary>
/// The <see cref="LogWatchHub"/> schema-apply dedup: a stored hash is recorded as applied only once the
/// apply has succeeded, so a transient apply failure is retried by the next push (or heartbeat) carrying
/// the same hash instead of pinning the silo on its stale schema. Pure unit tests: the constructor never
/// touches the grain or the grain factory, and <see cref="LogWatchHub.SchemaAdvanced"/> only drives the
/// apply callback.
/// </summary>
public class LogWatchHubSchemaApplyTests
{
    private static byte[] Bytes(string text) => Encoding.UTF8.GetBytes(text);

    [Fact]
    public async Task Failed_apply_is_retried_on_next_push_of_same_hash()
    {
        var invocations = 0;
        var applied = new List<string>();
        var hub = new LogWatchHub(grain: null!, grainFactory: null!, applySchema: text =>
        {
            invocations++;
            if (invocations == 1)
                throw new InvalidOperationException("transient provider failure");
            applied.Add(text);
        });

        await hub.SchemaAdvanced(Bytes("schema-A"), "h1");
        await hub.SchemaAdvanced(Bytes("schema-A"), "h1");

        Assert.Equal(2, invocations);
        Assert.Equal(["schema-A"], applied);
    }

    [Fact]
    public async Task Successful_apply_is_not_repeated_for_same_hash()
    {
        var invocations = 0;
        var hub = new LogWatchHub(grain: null!, grainFactory: null!, applySchema: _ => invocations++);

        await hub.SchemaAdvanced(Bytes("schema-A"), "h1");
        await hub.SchemaAdvanced(Bytes("schema-A"), "h1");

        Assert.Equal(1, invocations);
    }
}
