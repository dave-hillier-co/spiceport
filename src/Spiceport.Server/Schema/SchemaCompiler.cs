using System.Collections.Immutable;
using System.Text;
using Spiceport.Core;

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
/// and expiration requirements are attached to allowed relations, but caveat bodies
/// are not modelled (their CEL is ignored).
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
    /// Caveat bodies are captured verbatim as CEL text and stored UTF-8 encoded in
    /// <see cref="CaveatDefinition.SerializedExpression"/>; they are not type-checked
    /// or evaluated here. Parameter types are modelled, including generic
    /// <c>list&lt;T&gt;</c> / <c>map&lt;K,V&gt;</c> forms.
    /// </remarks>
    /// <exception cref="SchemaCompileException">If the schema cannot be parsed or compiled.</exception>
    public static CompiledSchema CompileSchema(string schemaText)
    {
        if (schemaText is null)
        {
            throw new ArgumentNullException(nameof(schemaText));
        }

        SchemaFileNode file = Parser.Parse(schemaText);
        return new CompiledSchema(
            [.. file.Definitions.Select(CompileDefinition)],
            [.. file.Caveats.Select(CompileCaveat)]);
    }

    private static CaveatDefinition CompileCaveat(CaveatNode caveat)
    {
        var parameterTypes = caveat.Parameters.ToImmutableDictionary(
            p => p.Name,
            p => CompileCaveatType(p.Type));

        // The CEL expression is stored verbatim (UTF-8) rather than evaluated here.
        byte[] serialized = Encoding.UTF8.GetBytes(caveat.Expression);

        return new CaveatDefinition(caveat.Name, serialized, parameterTypes);
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

        return new NamespaceDefinition(def.Name, relations.ToImmutable());
    }

    private static Relation CompileRelation(RelationNode rel)
    {
        var allowed = rel.AllowedTypes.Select(CompileTypeRef).ToImmutableList();
        return new Relation(rel.Name, TypeInformation: new TypeInformation(allowed));
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

        return Relation.Permission(perm.Name, new UsersetRewrite(operation));
    }

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
