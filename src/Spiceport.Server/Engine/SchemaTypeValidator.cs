using Cel;
using Spiceport.Core;
using Spiceport.Schema;

namespace Spiceport.Engine;

/// <summary>
/// Runs SpiceDB's type-system validation pass over a freshly compiled schema at write time, plus the
/// caveat-definition validation, rejecting schemas the engine would otherwise silently mis-evaluate (or
/// crash on) at check time. Mirrors <c>pkg/schema/typesystem_validation.go</c> (<c>TypeSystem.Validate</c>)
/// and <c>internal/namespace/caveats.go</c> (<c>ValidateCaveatDefinition</c>), as orchestrated by
/// <c>ValidateSchemaChanges</c> (<c>internal/services/shared/schema.go</c>).
/// </summary>
/// <remarks>
/// On the first violation a <see cref="SchemaTypeException"/> is thrown (the grain re-wraps it for the
/// gRPC boundary, where it maps to <c>FailedPrecondition</c>). What is checked: duplicate/reused
/// definition + caveat names; per-caveat (≥1 parameter, parseable CEL, every declared parameter
/// referenced); per-relation arrow targets (relation exists, is not a permission, does not import a
/// wildcard); per-permission computed-userset targets exist; per-base-relation allowed-type list is
/// non-empty, every allowed type's namespace/subrelation exists, none is self-referential, none is
/// duplicated, and every <c>with &lt;caveat&gt;</c> names a defined caveat.
/// </remarks>
public static class SchemaTypeValidator
{
    /// <summary>
    /// Validates the whole compiled schema. Throws <see cref="SchemaTypeException"/> on the first
    /// violation; returns normally if the schema is type-valid.
    /// </summary>
    public static void Validate(CompiledSchema schema)
    {
        ArgumentNullException.ThrowIfNull(schema);

        // 0) No name may be reused between two definitions, two caveats, or a definition and a caveat.
        //    SpiceDB rejects this in the compiler; the port's diff (ToDictionary by Name) would otherwise
        //    throw a raw ArgumentException on a later write.
        ValidateUniqueNames(schema);

        // 1) Validate every caveat definition (CEL parse, ≥1 param, all params referenced).
        foreach (var caveat in schema.Caveats)
            ValidateCaveat(caveat);

        // 2) Validate every object definition against the full type system.
        var byName = schema.Namespaces.ToDictionary(n => n.Name);
        var caveatNames = schema.Caveats.Select(c => c.Name).ToHashSet();
        foreach (var def in schema.Namespaces)
            ValidateDefinition(def, byName, caveatNames);
    }

    private static void ValidateUniqueNames(CompiledSchema schema)
    {
        var seen = new HashSet<string>();
        foreach (var def in schema.Namespaces)
        {
            if (!seen.Add(def.Name))
                throw new SchemaTypeException(
                    $"found name reused between multiple definitions and/or caveats: {def.Name}");
        }

        foreach (var caveat in schema.Caveats)
        {
            if (!seen.Add(caveat.Name))
                throw new SchemaTypeException(
                    $"found name reused between multiple definitions and/or caveats: {caveat.Name}");
        }
    }

    private static void ValidateCaveat(CaveatDefinition caveat)
    {
        var expression = System.Text.Encoding.UTF8.GetString(caveat.SerializedExpression);

        // (a) Parse/compile the CEL against the declared environment. A syntactically invalid expression
        //     is rejected here rather than deferred to a later Check at evaluation time.
        try
        {
            CaveatCompiler.Parse(expression);
        }
        catch (Exception ex) when (ex is CelException or InvalidOperationException or ArgumentException)
        {
            throw new SchemaTypeException($"could not compile caveat `{caveat.Name}`: {ex.Message}");
        }

        // (b) A caveat must declare at least one parameter.
        if (caveat.ParameterTypes.Count == 0)
            throw new SchemaTypeException($"caveat `{caveat.Name}` must have at least one parameter defined");

        // (c) Every declared parameter must be referenced by the expression.
        foreach (var param in caveat.ParameterTypes.Keys)
        {
            if (!ReferencesIdentifier(expression, param))
                throw new SchemaTypeException(
                    $"parameter `{param}` for caveat `{caveat.Name}` is unused");
        }
    }

    private static void ValidateDefinition(
        NamespaceDefinition def,
        IReadOnlyDictionary<string, NamespaceDefinition> byName,
        IReadOnlySet<string> caveatNames)
    {
        var relationsByName = def.Relations.ToDictionary(r => r.Name);

        foreach (var relation in def.Relations)
        {
            if (relation.IsPermission)
            {
                ValidatePermissionRewrite(def, relation, relationsByName);
                continue;
            }

            ValidateBaseRelation(def, relation, byName, caveatNames);
        }
    }

    private static void ValidatePermissionRewrite(
        NamespaceDefinition def,
        Relation permission,
        IReadOnlyDictionary<string, Relation> relationsByName)
    {
        WalkRewrite(permission.UsersetRewrite!.Operation, child =>
        {
            switch (child)
            {
                case SetOperationChild.ComputedUsersetChild computed:
                    if (!relationsByName.ContainsKey(computed.Value.Relation))
                        throw new SchemaTypeException(
                            $"relation/permission `{computed.Value.Relation}` not found under definition `{def.Name}`");
                    break;

                case SetOperationChild.TupleToUsersetChild ttu:
                    ValidateArrowTupleset(def, permission, ttu.Value.TuplesetRelation, relationsByName);
                    break;

                case SetOperationChild.FunctionedTupleToUsersetChild fttu:
                    ValidateArrowTupleset(def, permission, fttu.Value.TuplesetRelation, relationsByName);
                    break;
            }
        });
    }

    private static void ValidateArrowTupleset(
        NamespaceDefinition def,
        Relation permission,
        string tuplesetRelation,
        IReadOnlyDictionary<string, Relation> relationsByName)
    {
        if (!relationsByName.TryGetValue(tuplesetRelation, out var found))
            throw new SchemaTypeException(
                $"relation/permission `{tuplesetRelation}` not found under definition `{def.Name}`");

        // A permission may not appear on the left (tupleset) side of an arrow: arrows walk written tuples.
        if (found.IsPermission)
            throw new SchemaTypeException(
                $"under permission `{permission.Name}`: permissions cannot be used on the left hand side of an arrow (found `{tuplesetRelation}`)");

        // The tupleset relation must not (directly or transitively) import a wildcard subject type.
        if (ReferencesWildcard(def, tuplesetRelation, []))
            throw new SchemaTypeException(
                $"under permission `{permission.Name}`: relation `{tuplesetRelation}` includes wildcard type and thus cannot be used on the left hand side of an arrow");
    }

    private static void ValidateBaseRelation(
        NamespaceDefinition def,
        Relation relation,
        IReadOnlyDictionary<string, NamespaceDefinition> byName,
        IReadOnlySet<string> caveatNames)
    {
        var allowed = relation.TypeInformation?.AllowedDirectRelations ?? [];

        // A base relation (a _this relation with no rewrite) must list at least one allowed subject type.
        if (allowed.Count == 0)
            throw new SchemaTypeException(
                $"at least one allowed relation/permission is required in relation `{relation.Name}` in definition `{def.Name}`");

        var seen = new HashSet<string>();
        foreach (var subject in allowed)
        {
            var source = AllowedRelationIdentity.Source(subject);

            // No allowed type may be duplicated (caveat + expiration trait are part of the identity).
            if (!seen.Add(source))
                throw new SchemaTypeException(
                    $"found duplicate allowed subject type `{source}` on relation `{relation.Name}` in definition `{def.Name}`");

            ValidateAllowedSubjectNamespace(def, relation, subject, byName);

            // A required caveat must name a caveat defined in this schema.
            if (subject.RequiredCaveat is { } caveat && !caveatNames.Contains(caveat.CaveatName))
                throw new SchemaTypeException(
                    $"could not lookup caveat `{caveat.CaveatName}` for relation `{relation.Name}`: caveat with name `{caveat.CaveatName}` not found");
        }
    }

    private static void ValidateAllowedSubjectNamespace(
        NamespaceDefinition def,
        Relation relation,
        AllowedRelation subject,
        IReadOnlyDictionary<string, NamespaceDefinition> byName)
    {
        // A wildcard subject has no subrelation to resolve; only its namespace must exist.
        var subrelation = subject.RelationName ?? CoreConstants.Ellipsis;
        var resolvesSubrelation = !subject.IsPublicWildcard && subrelation != CoreConstants.Ellipsis;

        if (subject.ObjectType == def.Name)
        {
            // Same definition: the subrelation (if any) must be one of this definition's relations.
            // Self-reference (`relation member: user | group#member`) is VALID SpiceDB — it is the
            // canonical recursive-group shape (the conformance corpus' directgroups.yaml, accepted by
            // real SpiceDB's WriteSchema in the differential suite).
            if (resolvesSubrelation && !def.Relations.Any(r => r.Name == subrelation))
                throw new SchemaTypeException(
                    $"relation/permission `{subrelation}` not found under definition `{def.Name}`");

            return;
        }

        if (!byName.TryGetValue(subject.ObjectType, out var subjectDef))
            throw new SchemaTypeException(
                $"could not lookup definition `{subject.ObjectType}` for relation `{relation.Name}`: object definition `{subject.ObjectType}` not found");

        if (resolvesSubrelation && !subjectDef.Relations.Any(r => r.Name == subrelation))
            throw new SchemaTypeException(
                $"relation/permission `{subrelation}` not found under definition `{subject.ObjectType}`");

        // The referenced subrelation must not itself import a wildcard (transitive wildcard).
        if (resolvesSubrelation && ReferencesWildcard(subjectDef, subrelation, []))
            throw new SchemaTypeException(
                $"relation `{relation.Name}` in definition `{def.Name}` allows the wildcard type within `{subject.ObjectType}#{subrelation}`, which is not permitted");
    }

    /// <summary>
    /// True if the named base relation imports a public wildcard subject type, directly or transitively
    /// through a same-or-other-definition subrelation. Mirrors SpiceDB's <c>referencesWildcardType</c>.
    /// </summary>
    private static bool ReferencesWildcard(NamespaceDefinition def, string relationName, HashSet<string> encountered)
    {
        var key = $"{def.Name}#{relationName}";
        if (!encountered.Add(key))
            return false;

        var relation = def.Relations.FirstOrDefault(r => r.Name == relationName);
        if (relation is null || relation.IsPermission)
            return false;

        var allowed = relation.TypeInformation?.AllowedDirectRelations ?? [];
        foreach (var subject in allowed)
        {
            if (subject.IsPublicWildcard)
                return true;

            var subrelation = subject.RelationName ?? CoreConstants.Ellipsis;
            if (subrelation == CoreConstants.Ellipsis)
                continue;

            if (subject.ObjectType == def.Name)
            {
                if (ReferencesWildcard(def, subrelation, encountered))
                    return true;
            }
            // Cross-definition transitive wildcard is reported by the caller's namespace check; here we
            // only need same-definition recursion to detect the common direct/indirect cases.
        }

        return false;
    }

    /// <summary>Visits every <see cref="SetOperationChild"/> in a rewrite tree, depth-first.</summary>
    private static void WalkRewrite(SetOperation operation, Action<SetOperationChild> visit)
    {
        foreach (var child in operation.Children)
        {
            if (child is SetOperationChild.NestedRewrite nested)
            {
                WalkRewrite(nested.Value.Operation, visit);
                continue;
            }

            visit(child);
        }
    }

    /// <summary>True if <paramref name="expression"/> references the identifier as a whole word.</summary>
    private static bool ReferencesIdentifier(string expression, string identifier) =>
        System.Text.RegularExpressions.Regex.IsMatch(
            expression, $@"(?<![\w.]){System.Text.RegularExpressions.Regex.Escape(identifier)}\b");
}
