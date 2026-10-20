using Google.Protobuf.Reflection;
using Spiceport.Api;
using V1 = Authzed.Api.V1;

namespace Spiceport.Grains.Tests;

/// <summary>
/// Pins the premise of <see cref="ProtoReshape"/>: each ExperimentalService <c>Exp*</c> reflection message has
/// the same wire schema as its SchemaService counterpart, so re-encoding one as the other loses nothing.
/// </summary>
public class ProtoReshapeTests
{
    private static readonly IReadOnlyDictionary<string, string> Counterparts = new Dictionary<string, string>
    {
        [V1::ExperimentalReflectSchemaResponse.Descriptor.FullName] = V1::ReflectSchemaResponse.Descriptor.FullName,
        [V1::ExperimentalDiffSchemaResponse.Descriptor.FullName] = V1::DiffSchemaResponse.Descriptor.FullName,
        [V1::ExperimentalComputablePermissionsResponse.Descriptor.FullName] = V1::ComputablePermissionsResponse.Descriptor.FullName,
        [V1::ExperimentalDependentRelationsResponse.Descriptor.FullName] = V1::DependentRelationsResponse.Descriptor.FullName,
        [V1::ExpSchemaFilter.Descriptor.FullName] = V1::ReflectionSchemaFilter.Descriptor.FullName,
        [V1::ExpDefinition.Descriptor.FullName] = V1::ReflectionDefinition.Descriptor.FullName,
        [V1::ExpCaveat.Descriptor.FullName] = V1::ReflectionCaveat.Descriptor.FullName,
        [V1::ExpCaveatParameter.Descriptor.FullName] = V1::ReflectionCaveatParameter.Descriptor.FullName,
        [V1::ExpRelation.Descriptor.FullName] = V1::ReflectionRelation.Descriptor.FullName,
        [V1::ExpTypeReference.Descriptor.FullName] = V1::ReflectionTypeReference.Descriptor.FullName,
        [V1::ExpPermission.Descriptor.FullName] = V1::ReflectionPermission.Descriptor.FullName,
        [V1::ExpRelationReference.Descriptor.FullName] = V1::ReflectionRelationReference.Descriptor.FullName,
        [V1::ExpRelationSubjectTypeChange.Descriptor.FullName] = V1::ReflectionRelationSubjectTypeChange.Descriptor.FullName,
        [V1::ExpCaveatParameterTypeChange.Descriptor.FullName] = V1::ReflectionCaveatParameterTypeChange.Descriptor.FullName,
        [V1::ExpSchemaDiff.Descriptor.FullName] = V1::ReflectionSchemaDiff.Descriptor.FullName,
    };

    public static TheoryData<string> ExpMessages() => new(Counterparts.Keys);

    [Theory]
    [MemberData(nameof(ExpMessages))]
    public void Exp_message_has_the_same_wire_schema_as_its_v1_counterpart(string expName)
    {
        var exp = V1::ExperimentalServiceReflection.Descriptor.FindTypeByName<MessageDescriptor>(expName["authzed.api.v1.".Length..]);
        var v1 = V1::SchemaServiceReflection.Descriptor.FindTypeByName<MessageDescriptor>(Counterparts[expName]["authzed.api.v1.".Length..]);
        Assert.NotNull(exp);
        Assert.NotNull(v1);

        var expFields = exp.Fields.InFieldNumberOrder();
        var v1Fields = v1.Fields.InFieldNumberOrder();
        Assert.Equal(v1Fields.Count, expFields.Count);

        for (var i = 0; i < expFields.Count; i++)
        {
            var e = expFields[i];
            var r = v1Fields[i];
            Assert.Equal(r.FieldNumber, e.FieldNumber);
            Assert.Equal(r.Name, e.Name);
            Assert.Equal(r.FieldType, e.FieldType);
            Assert.Equal(r.IsRepeated, e.IsRepeated);
            Assert.Equal(r.RealContainingOneof?.Name, e.RealContainingOneof?.Name);

            if (e.FieldType == FieldType.Message)
            {
                var expected = Counterparts.TryGetValue(e.MessageType.FullName, out var counterpart)
                    ? counterpart
                    : e.MessageType.FullName;
                Assert.Equal(expected, r.MessageType.FullName);
            }
        }
    }

    [Fact]
    public void As_round_trips_a_nested_oneof_message()
    {
        var source = new V1::ReflectionSchemaDiff
        {
            RelationSubjectTypeAdded = new V1::ReflectionRelationSubjectTypeChange
            {
                Relation = new V1::ReflectionRelation { Name = "viewer", ParentDefinitionName = "document" },
                ChangedSubjectType = new V1::ReflectionTypeReference { SubjectDefinitionName = "group", IsTerminalSubject = true },
            },
        };

        var reshaped = ProtoReshape.As<V1::ExpSchemaDiff>(source);

        Assert.Equal(V1::ExpSchemaDiff.DiffOneofCase.RelationSubjectTypeAdded, reshaped.DiffCase);
        Assert.Equal("viewer", reshaped.RelationSubjectTypeAdded.Relation.Name);
        Assert.Equal("document", reshaped.RelationSubjectTypeAdded.Relation.ParentDefinitionName);
        Assert.Equal("group", reshaped.RelationSubjectTypeAdded.ChangedSubjectType.SubjectDefinitionName);
        Assert.True(reshaped.RelationSubjectTypeAdded.ChangedSubjectType.IsTerminalSubject);
    }
}
