namespace Spiceport.Core;

/// <summary>
/// The identifying core of a relationship (tuple): the resource and subject,
/// without any caveat, expiration, or integrity metadata.
/// </summary>
/// <param name="Resource">The resource ONR (e.g. document:readme#viewer).</param>
/// <param name="Subject">The subject ONR (e.g. user:alice#...).</param>
public sealed record RelationshipReference(ObjectAndRelation Resource, ObjectAndRelation Subject);
