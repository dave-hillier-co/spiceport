namespace Spiceport.Silo;

/// <summary>
/// The schema compiled at silo startup. For this slice it is a fixed fixture; a later phase resolves
/// schema (and its derived dispatch identity) per revision from the datastore.
/// </summary>
public static class SiloSchema
{
    public const string SchemaText = """
        definition user {}

        definition document {
            relation viewer: user
            relation editor: user
            permission view = viewer + editor
        }
        """;
}
