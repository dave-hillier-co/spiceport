using System.Collections.Immutable;
using System.Security.Cryptography;
using System.Text;
using Spiceport.Core;

namespace Spiceport.Engine;

/// <summary>
/// Computes a stable hash of a compiled schema model (namespaces + caveats), used as part of the
/// caching dispatcher's key (so a schema change invalidates cached branches) and as part of the
/// per-request grain routing key (see <see cref="Spiceport.Grains.GrainKey"/>).
/// </summary>
/// <remarks>
/// The hash is a SHA-256 over an explicit, faithful structural rendering of the namespace and caveat
/// definitions, ordered by name so the result is independent of enumeration order. The rendering walks
/// the full model (relations, type information, userset rewrites and their nested set operations) so
/// that two schemas sharing namespace names but differing in their relations/permissions hash
/// differently. (The default record <c>ToString()</c> cannot be relied on here: it renders nested
/// <see cref="ImmutableList{T}"/> properties as their type name rather than their contents.)
/// </remarks>
public static class SchemaHash
{
    /// <summary>Computes a lowercase hex SHA-256 over the given namespaces and caveats.</summary>
    /// <param name="namespaces">The compiled namespace definitions.</param>
    /// <param name="caveats">The compiled caveat definitions, or null.</param>
    public static string Compute(
        IEnumerable<NamespaceDefinition> namespaces,
        IEnumerable<CaveatDefinition>? caveats = null)
    {
        ArgumentNullException.ThrowIfNull(namespaces);

        var sb = new StringBuilder();
        sb.Append("namespaces:\n");
        foreach (var ns in namespaces.OrderBy(n => n.Name, StringComparer.Ordinal))
        {
            AppendNamespace(sb, ns);
        }

        sb.Append("caveats:\n");
        if (caveats is not null)
        {
            foreach (var c in caveats.OrderBy(c => c.Name, StringComparer.Ordinal))
            {
                sb.Append(c).Append('\n');
            }
        }

        var bytes = Encoding.UTF8.GetBytes(sb.ToString());
        return Convert.ToHexStringLower(SHA256.HashData(bytes));
    }

    /// <summary>Computes a lowercase hex SHA-256 over an already-keyed namespace dictionary.</summary>
    public static string Compute(
        ImmutableDictionary<string, NamespaceDefinition> namespaces,
        IEnumerable<CaveatDefinition>? caveats = null) =>
        Compute(namespaces.Values, caveats);

    private static void AppendNamespace(StringBuilder sb, NamespaceDefinition ns)
    {
        sb.Append("def ").Append(ns.Name).Append('\n');
        foreach (var relation in ns.Relations.OrderBy(r => r.Name, StringComparer.Ordinal))
        {
            AppendRelation(sb, relation);
        }
    }

    private static void AppendRelation(StringBuilder sb, Relation relation)
    {
        sb.Append("  rel ").Append(relation.Name);
        if (relation.AliasingRelation is { } alias)
        {
            sb.Append(" alias=").Append(alias);
        }

        sb.Append('\n');

        if (relation.TypeInformation is { } typeInfo)
        {
            foreach (var allowed in typeInfo.AllowedDirectRelations)
            {
                AppendAllowed(sb, allowed);
            }
        }

        if (relation.UsersetRewrite is { } rewrite)
        {
            sb.Append("    rewrite:");
            AppendSetOperation(sb, rewrite.Operation);
            sb.Append('\n');
        }
    }

    private static void AppendAllowed(StringBuilder sb, AllowedRelation allowed)
    {
        sb.Append("    allow ")
          .Append(allowed.ObjectType)
          .Append('#')
          .Append(allowed.IsPublicWildcard ? "*" : allowed.RelationName ?? CoreConstants.Ellipsis);
        if (allowed.RequiredCaveat is { } caveat)
        {
            sb.Append(" with ").Append(caveat.CaveatName);
        }

        if (allowed.RequiresExpiration)
        {
            sb.Append(" +expiration");
        }

        sb.Append('\n');
    }

    private static void AppendSetOperation(StringBuilder sb, SetOperation operation)
    {
        sb.Append('(').Append(operation.Type);
        foreach (var child in operation.Children)
        {
            sb.Append(' ');
            AppendChild(sb, child);
        }

        sb.Append(')');
    }

    private static void AppendChild(StringBuilder sb, SetOperationChild child)
    {
        switch (child)
        {
            case SetOperationChild.This:
                sb.Append("this");
                break;
            case SetOperationChild.Nil:
                sb.Append("nil");
                break;
            case SetOperationChild.Self:
                sb.Append("self");
                break;
            case SetOperationChild.ComputedUsersetChild cu:
                sb.Append("cu[").Append(cu.Value.Object).Append(':').Append(cu.Value.Relation).Append(']');
                break;
            case SetOperationChild.TupleToUsersetChild ttu:
                sb.Append("ttu[")
                  .Append(ttu.Value.TuplesetRelation).Append("->")
                  .Append(ttu.Value.ComputedUserset.Object).Append(':')
                  .Append(ttu.Value.ComputedUserset.Relation).Append(']');
                break;
            case SetOperationChild.FunctionedTupleToUsersetChild fttu:
                sb.Append("fttu[").Append(fttu.Value.Function).Append(' ')
                  .Append(fttu.Value.TuplesetRelation).Append("->")
                  .Append(fttu.Value.ComputedUserset.Object).Append(':')
                  .Append(fttu.Value.ComputedUserset.Relation).Append(']');
                break;
            case SetOperationChild.NestedRewrite nested:
                AppendSetOperation(sb, nested.Value.Operation);
                break;
        }
    }
}
