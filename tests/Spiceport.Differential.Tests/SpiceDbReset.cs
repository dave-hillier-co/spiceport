using System.Text.RegularExpressions;
using Grpc.Core;
using V1 = Authzed.Api.V1;

namespace Spiceport.Differential.Tests;

/// <summary>
/// Pre-write reset of the collection-shared real-SpiceDB container, for directed suites that write
/// their own schema. Other classes in <see cref="SpiceDbCollection"/> can leave relationships live
/// (xUnit gives no class ordering guarantee, and their cleanup is best-effort), and real SpiceDB
/// rejects a <c>WriteSchema</c> that would orphan existing relationships ("cannot delete object
/// definition ... as at least one relationship exists under it", observed as <c>InvalidArgument</c>).
/// That residue rejection would make accept-path setup fail spuriously and pollute reject-path
/// status-code assertions with a cause other than the rule under test. Clearing every type the
/// CURRENTLY active schema defines (read back from the container, so no other class's type list is
/// hard-coded here) guarantees the subsequent write's verdict reflects schema validation alone.
/// Best-effort by design: a container with no schema yet has nothing to reset.
/// </summary>
internal static class SpiceDbReset
{
    public static async Task ResetAsync(SpiceDbGrpcClient client)
    {
        string schemaText;
        try
        {
            schemaText = (await client.ReadSchemaAsync(new V1::ReadSchemaRequest())).SchemaText;
        }
        catch (RpcException)
        {
            return;
        }

        foreach (Match match in Regex.Matches(schemaText, @"(?m)^\s*definition\s+([A-Za-z0-9_/]+)"))
        {
            try
            {
                await client.DeleteRelationshipsAsync(new V1::DeleteRelationshipsRequest
                {
                    RelationshipFilter = new V1::RelationshipFilter { ResourceType = match.Groups[1].Value },
                });
            }
            catch (RpcException)
            {
                // Not deletable under whatever schema is active; nothing to reset for this type.
            }
        }
    }
}
