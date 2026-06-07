using Spiceport.Core;
using Spiceport.Schema;

namespace Spiceport.Grains;

/// <summary>
/// A single structural difference between two compiled schemas. A closed discriminated union mirroring
/// the cases of the <c>authzed.api.v1.ReflectionSchemaDiff</c> proto <c>oneof</c>.
/// </summary>
/// <remarks>
/// Doc-comment-change variants are intentionally absent: the compiled Core model carries no comment
/// metadata (the lexer/AST drops comments), so they can never be detected.
/// </remarks>
public abstract record SchemaDelta
{
    private SchemaDelta() { }

    /// <summary>A whole definition was added in the comparison schema.</summary>
    public sealed record DefinitionAdded(NamespaceDefinition Definition) : SchemaDelta;

    /// <summary>A whole definition was removed in the comparison schema.</summary>
    public sealed record DefinitionRemoved(NamespaceDefinition Definition) : SchemaDelta;

    /// <summary>A base relation was added to a kept definition.</summary>
    public sealed record RelationAdded(string DefinitionName, Relation Relation) : SchemaDelta;

    /// <summary>A base relation was removed from a kept definition (or turned into a permission).</summary>
    public sealed record RelationRemoved(string DefinitionName, Relation Relation) : SchemaDelta;

    /// <summary>An allowed subject type was added to a kept relation.</summary>
    public sealed record RelationSubjectTypeAdded(
        string DefinitionName, Relation Relation, AllowedRelation SubjectType) : SchemaDelta;

    /// <summary>An allowed subject type was removed from a kept relation.</summary>
    public sealed record RelationSubjectTypeRemoved(
        string DefinitionName, Relation Relation, AllowedRelation SubjectType) : SchemaDelta;

    /// <summary>A permission was added to a kept definition.</summary>
    public sealed record PermissionAdded(string DefinitionName, Relation Permission) : SchemaDelta;

    /// <summary>A permission was removed from a kept definition (or turned into a base relation).</summary>
    public sealed record PermissionRemoved(string DefinitionName, Relation Permission) : SchemaDelta;

    /// <summary>A permission's userset-rewrite expression changed.</summary>
    public sealed record PermissionExprChanged(string DefinitionName, Relation Permission) : SchemaDelta;

    /// <summary>A whole caveat was added.</summary>
    public sealed record CaveatAdded(CaveatDefinition Caveat) : SchemaDelta;

    /// <summary>A whole caveat was removed.</summary>
    public sealed record CaveatRemoved(CaveatDefinition Caveat) : SchemaDelta;

    /// <summary>A kept caveat's expression changed.</summary>
    public sealed record CaveatExprChanged(CaveatDefinition Caveat) : SchemaDelta;

    /// <summary>A parameter was added to a kept caveat.</summary>
    public sealed record CaveatParameterAdded(
        string CaveatName, string ParameterName, CaveatTypeReference Type) : SchemaDelta;

    /// <summary>A parameter was removed from a kept caveat.</summary>
    public sealed record CaveatParameterRemoved(
        string CaveatName, string ParameterName, CaveatTypeReference Type) : SchemaDelta;

    /// <summary>A kept caveat parameter's type changed.</summary>
    public sealed record CaveatParameterTypeChanged(
        string CaveatName, string ParameterName, CaveatTypeReference Type, CaveatTypeReference PreviousType)
        : SchemaDelta;
}

/// <summary>
/// The pure, datastore-free diff engine shared by <see cref="SchemaChangeValidator"/> (which runs orphan
/// checks against the removal subset) and the <c>DiffSchema</c> RPC (which translates the full delta list
/// into proto). Diffs <c>existing -&gt; next</c>, so <c>Added</c>/<c>Removed</c> are relative to the
/// existing schema as the base, matching SpiceDB's orientation.
/// </summary>
public static class SchemaDiff
{
    /// <summary>Computes the structural deltas turning <paramref name="existing"/> into <paramref name="next"/>.</summary>
    public static IReadOnlyList<SchemaDelta> Compute(CompiledSchema existing, CompiledSchema next)
    {
        ArgumentNullException.ThrowIfNull(existing);
        ArgumentNullException.ThrowIfNull(next);

        var deltas = new List<SchemaDelta>();

        DiffDefinitions(existing, next, deltas);
        DiffCaveats(existing, next, deltas);

        return deltas;
    }

    private static void DiffDefinitions(CompiledSchema existing, CompiledSchema next, List<SchemaDelta> deltas)
    {
        var existingByName = existing.Namespaces.ToDictionary(n => n.Name);
        var nextByName = next.Namespaces.ToDictionary(n => n.Name);

        foreach (var nextDef in next.Namespaces)
        {
            if (!existingByName.ContainsKey(nextDef.Name))
                deltas.Add(new SchemaDelta.DefinitionAdded(nextDef));
        }

        foreach (var existingDef in existing.Namespaces)
        {
            if (!nextByName.TryGetValue(existingDef.Name, out var nextDef))
            {
                deltas.Add(new SchemaDelta.DefinitionRemoved(existingDef));
                continue;
            }

            DiffRelations(existingDef, nextDef, deltas);
        }
    }

    private static void DiffRelations(
        NamespaceDefinition existingDef, NamespaceDefinition nextDef, List<SchemaDelta> deltas)
    {
        var existingRels = existingDef.Relations.ToDictionary(r => r.Name);
        var nextRels = nextDef.Relations.ToDictionary(r => r.Name);

        // Added (present only in next).
        foreach (var nextRel in nextDef.Relations)
        {
            if (existingRels.ContainsKey(nextRel.Name))
                continue;
            deltas.Add(nextRel.IsPermission
                ? new SchemaDelta.PermissionAdded(nextDef.Name, nextRel)
                : new SchemaDelta.RelationAdded(nextDef.Name, nextRel));
        }

        foreach (var existingRel in existingDef.Relations)
        {
            if (!nextRels.TryGetValue(existingRel.Name, out var nextRel))
            {
                deltas.Add(existingRel.IsPermission
                    ? new SchemaDelta.PermissionRemoved(existingDef.Name, existingRel)
                    : new SchemaDelta.RelationRemoved(existingDef.Name, existingRel));
                continue;
            }

            // Relation <-> permission flip: removal of the old kind + addition of the new kind.
            if (existingRel.IsPermission != nextRel.IsPermission)
            {
                if (existingRel.IsPermission)
                {
                    deltas.Add(new SchemaDelta.PermissionRemoved(existingDef.Name, existingRel));
                    deltas.Add(new SchemaDelta.RelationAdded(nextDef.Name, nextRel));
                }
                else
                {
                    deltas.Add(new SchemaDelta.RelationRemoved(existingDef.Name, existingRel));
                    deltas.Add(new SchemaDelta.PermissionAdded(nextDef.Name, nextRel));
                }
                continue;
            }

            if (existingRel.IsPermission)
            {
                // Both permissions: compare the rewrite trees structurally. (Record equality is not enough
                // here: the trees hold ImmutableList children, whose Equals is reference, not value, based.)
                if (!RewriteEquals(existingRel.UsersetRewrite, nextRel.UsersetRewrite))
                    deltas.Add(new SchemaDelta.PermissionExprChanged(nextDef.Name, nextRel));
            }
            else
            {
                DiffAllowedTypes(existingDef.Name, existingRel, nextRel, deltas);
            }
        }
    }

    private static void DiffAllowedTypes(
        string defName, Relation existingRel, Relation nextRel, List<SchemaDelta> deltas)
    {
        var existingAllowed = existingRel.TypeInformation?.AllowedDirectRelations ?? [];
        var nextAllowed = nextRel.TypeInformation?.AllowedDirectRelations ?? [];

        foreach (var added in nextAllowed)
        {
            if (!existingAllowed.Any(e => SameAllowedType(e, added)))
                deltas.Add(new SchemaDelta.RelationSubjectTypeAdded(defName, nextRel, added));
        }

        foreach (var removed in existingAllowed)
        {
            if (!nextAllowed.Any(n => SameAllowedType(n, removed)))
                deltas.Add(new SchemaDelta.RelationSubjectTypeRemoved(defName, existingRel, removed));
        }
    }

    private static void DiffCaveats(CompiledSchema existing, CompiledSchema next, List<SchemaDelta> deltas)
    {
        var existingByName = existing.Caveats.ToDictionary(c => c.Name);
        var nextByName = next.Caveats.ToDictionary(c => c.Name);

        foreach (var nextCaveat in next.Caveats)
        {
            if (!existingByName.ContainsKey(nextCaveat.Name))
                deltas.Add(new SchemaDelta.CaveatAdded(nextCaveat));
        }

        foreach (var existingCaveat in existing.Caveats)
        {
            if (!nextByName.TryGetValue(existingCaveat.Name, out var nextCaveat))
            {
                deltas.Add(new SchemaDelta.CaveatRemoved(existingCaveat));
                continue;
            }

            if (!existingCaveat.SerializedExpression.AsSpan().SequenceEqual(nextCaveat.SerializedExpression))
                deltas.Add(new SchemaDelta.CaveatExprChanged(nextCaveat));

            DiffCaveatParameters(existingCaveat, nextCaveat, deltas);
        }
    }

    private static void DiffCaveatParameters(
        CaveatDefinition existing, CaveatDefinition next, List<SchemaDelta> deltas)
    {
        foreach (var (name, type) in next.ParameterTypes)
        {
            if (!existing.ParameterTypes.ContainsKey(name))
                deltas.Add(new SchemaDelta.CaveatParameterAdded(next.Name, name, type));
        }

        foreach (var (name, existingType) in existing.ParameterTypes)
        {
            if (!next.ParameterTypes.TryGetValue(name, out var nextType))
            {
                deltas.Add(new SchemaDelta.CaveatParameterRemoved(existing.Name, name, existingType));
                continue;
            }

            if (nextType != existingType)
                deltas.Add(new SchemaDelta.CaveatParameterTypeChanged(next.Name, name, nextType, existingType));
        }
    }

    /// <summary>
    /// Structural equality of two userset rewrites. Needed because the rewrite tree holds
    /// <see cref="System.Collections.Immutable.ImmutableList{T}"/> children whose <c>Equals</c> is
    /// reference-based, so default record equality reports separately-compiled-but-identical trees as
    /// different.
    /// </summary>
    public static bool RewriteEquals(UsersetRewrite? a, UsersetRewrite? b)
    {
        if (a is null || b is null)
            return ReferenceEquals(a, b);
        return OperationEquals(a.Operation, b.Operation);
    }

    private static bool OperationEquals(SetOperation a, SetOperation b)
    {
        if (a.Type != b.Type || a.Children.Count != b.Children.Count)
            return false;
        for (var i = 0; i < a.Children.Count; i++)
        {
            if (!ChildEquals(a.Children[i], b.Children[i]))
                return false;
        }
        return true;
    }

    private static bool ChildEquals(SetOperationChild a, SetOperationChild b) => (a, b) switch
    {
        (SetOperationChild.NestedRewrite na, SetOperationChild.NestedRewrite nb) =>
            OperationEquals(na.Value.Operation, nb.Value.Operation),
        (SetOperationChild.NestedRewrite, _) or (_, SetOperationChild.NestedRewrite) => false,
        // All other child kinds hold only scalars/records without collection fields, so value equality holds.
        _ => a == b,
    };

    /// <summary>
    /// Structural equality of two allowed subject types: same object type, kind, and subrelation
    /// (ellipsis-normalized). Mirrors the validator's original <c>SameAllowedType</c>.
    /// </summary>
    public static bool SameAllowedType(AllowedRelation a, AllowedRelation b) =>
        a.ObjectType == b.ObjectType
        && a.Kind == b.Kind
        && (a.RelationName ?? CoreConstants.Ellipsis) == (b.RelationName ?? CoreConstants.Ellipsis);
}
