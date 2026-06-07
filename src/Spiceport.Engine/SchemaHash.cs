using System.Collections.Immutable;
using System.Security.Cryptography;
using System.Text;
using Spiceport.Core;

namespace Spiceport.Engine;

/// <summary>
/// Computes a stable hash of a compiled schema model (namespaces + caveats), used as part of the
/// caching dispatcher's key so that a schema change invalidates cached branches.
/// </summary>
/// <remarks>
/// The hash is a SHA-256 over a canonical rendering of the namespace and caveat definitions, ordered
/// by name so the result is independent of enumeration order. The schema records have deterministic
/// structural <c>ToString()</c> forms, which makes this a faithful canonical hash of the model.
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
            sb.Append(ns).Append('\n');
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
}
