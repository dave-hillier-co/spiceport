using System.Collections.Immutable;
using System.Text;
using Spiceport.Core;
using Spiceport.Engine;

namespace Spiceport.Schema;

/// <summary>
/// The full result of compiling a schema: both object-type namespace definitions
/// and caveat definitions, each in source order.
/// </summary>
/// <param name="Namespaces">Compiled object-type definitions, in source order.</param>
/// <param name="Caveats">Compiled caveat definitions, in source order.</param>
public sealed record CompiledSchema(
    ImmutableList<NamespaceDefinition> Namespaces,
    ImmutableList<CaveatDefinition> Caveats);

/// <summary>
/// Compiles SpiceDB schema DSL text into <see cref="NamespaceDefinition"/> objects
/// from the Spiceport.Core schema model.
/// </summary>
/// <remarks>
/// Supported: definitions, relations (type refs with <c>#subrelation</c> and <c>:*</c>
/// wildcards), and permissions (union <c>+</c>, intersection <c>&amp;</c>, exclusion
/// <c>-</c>, arrows <c>-&gt;</c> / <c>.any()</c> / <c>.all()</c>, and <c>nil</c>).
/// Caveat blocks and <c>with</c> / <c>expiration</c> traits are parsed; caveat names
/// and expiration requirements are attached to allowed relations. <see cref="Compile"/> ignores caveat
/// bodies; <see cref="CompileSchema"/> compiles them (see there).
/// </remarks>
public static class SchemaCompiler
{
    /// <summary>
    /// Compiles the given schema DSL text into a list of namespace definitions,
    /// in source order.
    /// </summary>
    /// <exception cref="SchemaCompileException">If the schema cannot be parsed or compiled.</exception>
    public static ImmutableList<NamespaceDefinition> Compile(string schemaText)
    {
        if (schemaText is null)
        {
            throw new ArgumentNullException(nameof(schemaText));
        }

        SchemaFileNode file = Parser.Parse(schemaText);
        return [.. file.Definitions.Select(CompileDefinition)];
    }

    /// <summary>
    /// Compiles the given schema DSL text into a <see cref="CompiledSchema"/> exposing
    /// both namespace definitions and caveat definitions, each in source order.
    /// </summary>
    /// <remarks>
    /// Caveats are compiled as SpiceDB's schema compiler compiles them
    /// (<c>pkg/schemadsl/compiler/translator.go</c> <c>translateCaveatDefinition</c>): every parameter type
    /// must be a known caveat type, and the CEL body must parse, type-check against the parameters and
    /// produce a boolean (<see cref="CaveatCompiler"/>). A failure is a <see cref="SchemaCompileException"/>
    /// carrying upstream's message. The body is still stored verbatim (UTF-8) in
    /// <see cref="CaveatDefinition.SerializedExpression"/>; the compiled form is not kept.
    /// </remarks>
    /// <exception cref="SchemaCompileException">If the schema cannot be parsed or compiled.</exception>
    public static CompiledSchema CompileSchema(string schemaText) => Compile(schemaText, compileCaveats: true);

    /// <summary>
    /// Compiles schema DSL text that was already compiled when it was written (a stored schema), without
    /// compiling its caveat bodies again. A stored schema must keep loading even if this compiler's caveat
    /// checks grow stricter than the ones it was written under.
    /// </summary>
    /// <exception cref="SchemaCompileException">If the schema cannot be parsed or compiled.</exception>
    public static CompiledSchema CompileStoredSchema(string schemaText) => Compile(schemaText, compileCaveats: false);

    private static CompiledSchema Compile(string schemaText, bool compileCaveats)
    {
        if (schemaText is null)
        {
            throw new ArgumentNullException(nameof(schemaText));
        }

        SchemaFileNode file = Parser.Parse(schemaText);
        return new CompiledSchema(
            [.. file.Definitions.Select(CompileDefinition)],
            [.. file.Caveats.Select(c => CompileCaveat(c, compileCaveats))]);
    }

    private static CaveatDefinition CompileCaveat(CaveatNode caveat, bool compileCaveat)
    {
        var parameterTypes = ImmutableDictionary.CreateBuilder<string, CaveatTypeReference>();
        foreach (var parameter in caveat.Parameters)
        {
            if (parameterTypes.ContainsKey(parameter.Name))
            {
                throw new SchemaCompileException(
                    $"duplicate parameter `{parameter.Name}` defined on caveat `{caveat.Name}`",
                    parameter.Line,
                    parameter.Column);
            }

            var type = CompileCaveatType(parameter.Type);
            if (compileCaveat)
            {
                try
                {
                    CaveatParameterTypes.Validate(type);
                }
                catch (CaveatCompileException ex)
                {
                    throw new SchemaCompileException(
                        $"invalid type for caveat parameter `{parameter.Name}` on caveat `{caveat.Name}`: {ex.Message}",
                        parameter.Line,
                        parameter.Column);
                }
            }

            parameterTypes.Add(parameter.Name, type);
        }

        // A parameterless caveat is left to SchemaTypeValidator, which rejects it by name.
        if (compileCaveat && parameterTypes.Count > 0)
            CompileCaveatExpression(caveat, parameterTypes);

        // The CEL expression is stored verbatim (UTF-8) rather than evaluated here.
        byte[] serialized = Encoding.UTF8.GetBytes(caveat.Expression);

        return new CaveatDefinition(caveat.Name, serialized, parameterTypes.ToImmutable())
        {
            Comments = NormalizeComments(caveat.Comments),
        };
    }

    private static void CompileCaveatExpression(
        CaveatNode caveat, IReadOnlyDictionary<string, CaveatTypeReference> parameterTypes)
    {
        try
        {
            CaveatCompiler.Compile(
                caveat.Name, caveat.CelSource, parameterTypes, caveat.CelStartLine, caveat.CelStartColumn);
        }
        catch (CaveatCompileException ex)
        {
            throw new SchemaCompileException(
                $"invalid expression for caveat `{caveat.Name}`: {ex.Message}",
                caveat.CelStartLine + 1,
                caveat.CelStartColumn + 1);
        }
    }

    private static CaveatTypeReference CompileCaveatType(CaveatTypeRefNode type)
    {
        ImmutableList<CaveatTypeReference>? children = type.ChildTypes.IsEmpty
            ? null
            : [.. type.ChildTypes.Select(CompileCaveatType)];

        return new CaveatTypeReference(type.Name, children);
    }

    private static NamespaceDefinition CompileDefinition(DefinitionNode def)
    {
        var relations = ImmutableList.CreateBuilder<Relation>();

        foreach (RelationNode rel in def.Relations)
        {
            relations.Add(CompileRelation(rel));
        }

        foreach (PermissionNode perm in def.Permissions)
        {
            relations.Add(CompilePermission(perm));
        }

        return new NamespaceDefinition(def.Name, relations.ToImmutable())
        {
            Comments = NormalizeComments(def.Comments),
        };
    }

    private static Relation CompileRelation(RelationNode rel)
    {
        var allowed = rel.AllowedTypes.Select(CompileTypeRef).ToImmutableList();
        return new Relation(rel.Name, TypeInformation: new TypeInformation(allowed))
        {
            Comments = NormalizeComments(rel.Comments),
        };
    }

    private static AllowedRelation CompileTypeRef(TypeRefNode t)
    {
        AllowedCaveat? caveat = t.CaveatName is null ? null : new AllowedCaveat(t.CaveatName);

        if (t.IsWildcard)
        {
            return AllowedRelation.Wildcard(t.TypeName, caveat, t.RequiresExpiration);
        }

        string subrelation = t.Subrelation ?? CoreConstants.Ellipsis;
        return AllowedRelation.Direct(t.TypeName, subrelation, caveat, t.RequiresExpiration);
    }

    private static Relation CompilePermission(PermissionNode perm)
    {
        SetOperationChild child = CompileExpr(perm.Expression);

        // The top-level rewrite is always a single SetOperation. When the expression
        // itself is a set operation we lift it; otherwise wrap the operand in a union.
        SetOperation operation = child is SetOperationChild.NestedRewrite nested
            ? nested.Value.Operation
            : SetOperation.Union(child);

        return Relation.Permission(perm.Name, new UsersetRewrite(operation)) with
        {
            Comments = NormalizeComments(perm.Comments),
        };
    }

    /// <summary>
    /// Port of SpiceDB's <c>addComments</c>/<c>normalizeComment</c> (pkg/schemadsl/compiler/translator.go):
    /// each comment becomes one doc comment, kept verbatim with its markers but with every line
    /// whitespace-trimmed. The comments are metadata; nothing semantic reads them.
    /// </summary>
    private static ImmutableList<string> NormalizeComments(ImmutableList<string> comments) =>
        [.. comments.Select(NormalizeComment)];

    private static string NormalizeComment(string value) =>
        string.Join('\n', value.Split('\n').Select(line => line.Trim()));

    private static SetOperationChild CompileExpr(ExprNode expr) => expr switch
    {
        NilExpr => new SetOperationChild.Nil(),
        SelfExpr => new SetOperationChild.Self(),
        ReferenceExpr r => new SetOperationChild.ComputedUsersetChild(ComputedUserset.OnResource(r.Name)),
        ArrowExpr a => CompileArrow(a),
        BinaryExpr b => CompileBinary(b),
        _ => throw new SchemaCompileException($"unsupported expression node '{expr.GetType().Name}'"),
    };

    private static SetOperationChild CompileArrow(ArrowExpr a)
    {
        var computed = new ComputedUserset(ComputedUsersetObject.TupleUsersetObject, a.Computed);

        if (a.Function is null)
        {
            return new SetOperationChild.TupleToUsersetChild(new TupleToUserset(a.Tupleset, computed));
        }

        TupleToUsersetFunction fn = a.Function switch
        {
            "any" => TupleToUsersetFunction.Any,
            "all" => TupleToUsersetFunction.All,
            _ => throw new SchemaCompileException($"unsupported arrow function '{a.Function}'"),
        };

        return new SetOperationChild.FunctionedTupleToUsersetChild(
            new FunctionedTupleToUserset(fn, a.Tupleset, computed));
    }

    private static SetOperationChild CompileBinary(BinaryExpr b)
    {
        SetOperationType type = b.Op switch
        {
            SetOp.Union => SetOperationType.Union,
            SetOp.Intersection => SetOperationType.Intersection,
            SetOp.Exclusion => SetOperationType.Exclusion,
            _ => throw new SchemaCompileException($"unsupported operator '{b.Op}'"),
        };

        // Flatten associative operators (union/intersection) into a single n-ary
        // SetOperation; exclusion is left-nested because order is significant.
        var children = ImmutableList.CreateBuilder<SetOperationChild>();
        FlattenOperand(b.Left, type, children);
        FlattenOperand(b.Right, type, children);

        var op = new SetOperation(type, children.ToImmutable());
        return new SetOperationChild.NestedRewrite(new UsersetRewrite(op));
    }

    private static void FlattenOperand(
        ExprNode node,
        SetOperationType parentType,
        ImmutableList<SetOperationChild>.Builder children)
    {
        if (parentType != SetOperationType.Exclusion
            && node is BinaryExpr inner
            && OperatorType(inner.Op) == parentType)
        {
            FlattenOperand(inner.Left, parentType, children);
            FlattenOperand(inner.Right, parentType, children);
            return;
        }

        children.Add(CompileExpr(node));
    }

    private static SetOperationType OperatorType(SetOp op) => op switch
    {
        SetOp.Union => SetOperationType.Union,
        SetOp.Intersection => SetOperationType.Intersection,
        _ => SetOperationType.Exclusion,
    };
}
