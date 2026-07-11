# SpiceDB Check Algorithm - C# Porting Specification for Spiceport.Engine

## Executive Summary

This document specifies the precise recursive algorithm for Check(resource, relation, subject, revision) 
as implemented in SpiceDB (`internal/graph/check.go`), targeting C# implementation in Spiceport.Engine 
against the Core schema model and Datastore Reader interface.

**Scope:** Check evaluation only (permission resolution tree traversal)
**Caveats:** Deferred (treat as plain membership)
**Expiration:** Deferred (treat as plain membership)
**Note:** No code implementation provided; this is a specification only.

---

## 1. Core Algorithm Structure

### 1.1 Signature

```
Check(
    resource: ObjectAndRelation,        // Target resource (namespace, object_id, relation)
    subject: ObjectAndRelation,         // Target subject (namespace, object_id, relation)
    revision: DatastoreRevision,        // Snapshot for consistent reads
    depth: uint32,                      // Current recursion depth; max enforced
    visitedSet: ExactVisitedSet         // Traversal cycle detection
) -> MembershipResult
```

### 1.2 Return Type: MembershipResult

```
record MembershipResult {
    bool IsMember;                      // Definite membership yes/no
    Expression? Caveat;                 // Optional caveat expression (deferred)
    Map<string, MembershipResult> ResultsByResourceId;  // Multi-resource results
    ResponseMetadata Metadata;          // Dispatch count, depth, debug info
    Exception? Error;                   // Error during evaluation
}
```

### 1.3 Depth Limiting

- Depth is decremented on each recursive call (dispatch).
- If depth reaches 0 before completion, return error: ErrDepthExceeded.
- Initial depth typically 50 (configurable).

### 1.4 Cycle Detection via Exact Visited Set

- The exact visited set prevents infinite loops during relation walks.
- On entry, check if (resource.Namespace, resource.Relation, subject.Namespace, subject.Relation) 
  is already in the visited set.
- If present, return NO_MEMBER (assumed cyclic).
- Add key to the visited set on entry.

---

## 2. High-Level Flow

```
Check(resource, relation, subject, revision, depth, visited):
  1. Depth validation
  2. Wildcard validation (reject if subject is `*:*`)
  3. Load relation definition from schema at revision
  4. Early exit: direct member check (resource#relation matches subject exactly)
  5. Set operation dispatch:
     a. If relation has NO userset_rewrite → checkDirect()
     b. If relation has userset_rewrite → checkUsersetRewrite()
  6. Combine results, handle check hints (deferred), return MembershipResult
```

---

## 3. Direct Tuple Matching (checkDirect)

### 3.1 Purpose
Find direct relationships where subject or wildcard form matches allowed direct relations.

### 3.2 Algorithm

```
checkDirect(
    resource: ObjectAndRelation,
    relation: Relation,
    subject: ObjectAndRelation,
    revision: DatastoreRevision,
    depth: uint32,
    visited: ExactVisitedSet
) -> MembershipResult:
  
  foundMembers = {}  // Set of (resourceId → caveat) direct matches
  
  // Phase 1: Query direct subject matches
  FOR EACH allowedDirectRelation IN relation.TypeInformation.AllowedDirectRelations:
    
    IF allowedDirectRelation.Namespace == subject.Namespace:
      IF allowedDirectRelation.Relation == subject.Relation:
        // Exact subject match is possible; add to query
      ELSE IF allowedDirectRelation.IsPublicWildcard:
        // Wildcard match is possible; add to query (subject.ObjectId as "*")
    
    IF allowedDirectRelation.Relation != "...":
      // Non-ellipsis: subject relation to walk (dispatch later)
      nonTerminalsExist = true
  
  // Query datastore for direct matches
  tuples = QueryRelationships(
    resource: resource,
    subjectSelectors: [
      { 
        subjectNamespace: subject.Namespace,
        subjectIds: [subject.ObjectId],
        relationFilter: subject.Relation  // Exact relation
      },
      {
        subjectNamespace: subject.Namespace,
        subjectIds: ["*"],
        relationFilter: "..."  // Wildcard + ellipsis
      }
    ]
  )
  
  FOR EACH tuple IN tuples:
    foundMembers.Add(tuple.resource.ObjectId, tuple.caveat)
    IF resultsSetting == ALLOW_SINGLE_RESULT && foundMembers.HasDeterminedMember():
      RETURN checkResultsForMembership(foundMembers)
  
  // Phase 2: Query non-terminal subjects (if any remain unchecked)
  IF !nonTerminalsExist || len(filteredResourceIds) == 0:
    RETURN checkResultsForMembership(foundMembers)
  
  nonTerminalTuples = QueryRelationships(
    resource: resource,
    subjectSelectors: [
      { relationFilter: "non-ellipsis" }  // Only walk non-terminal relations
    ]
  )
  
  // Dispatch non-terminals for recursive checking
  FOR EACH tuple IN nonTerminalTuples:
    subjectRR = (tuple.subject.Namespace, tuple.subject.Relation)
    
    // Recursively check: subject.Relation @ subject's type, looking for target subject
    childResult = Check(
      resource: ObjectAndRelation(subjectRR.Namespace, tuple.subject.ObjectId, subjectRR.Relation),
      subject: subject,
      revision: revision,
      depth: depth - 1,
      visited: visited.Copy()
    )
    
    // Map results back to original resource ID(s)
    FOR EACH (subjectId, membershipResult) IN childResult.ResultsByResourceId:
      IF membershipResult.IsMember:
        foundMembers.Add(tuple.resource.ObjectId, membershipResult.Caveat)
  
  RETURN checkResultsForMembership(foundMembers)
```

### 3.3 Ellipsis and Wildcard Handling

- **Ellipsis (`...`)**: Marker for terminal subjects; no further relation traversal.
- **Wildcard (`*`)**: Public wildcard subject `*:*#...` means "any authenticated user."
- Query filters:
  - Direct match: `subject.Namespace:subject.ObjectId#subject.Relation`
  - Wildcard: `subject.Namespace:*#...`
  - Non-terminal: Relations where `relation != "..."` are walked recursively.

---

## 4. Computed Userset (checkComputedUserset)

### 4.1 Purpose
Rewrite check on resource to check on a derived relation within the same or different namespace.

### 4.2 Algorithm

```
checkComputedUserset(
    resource: ObjectAndRelation,
    computedUserset: ComputedUserset,
    subject: ObjectAndRelation,
    revision: DatastoreRevision,
    depth: uint32,
    visited: ExactVisitedSet
) -> MembershipResult:
  
  // Determine target namespace and resource IDs based on object type
  SWITCH computedUserset.Object:
    CASE TUPLE_OBJECT:
      targetNamespace = resource.Namespace
      targetResourceIds = [resource.ObjectId]
    
    CASE TUPLE_USERSET_OBJECT:
      // TTU variant: targetNamespace and IDs come from caller context
      targetNamespace = <from caller>
      targetResourceIds = <from caller>
  
  targetRR = (targetNamespace, computedUserset.Relation)
  
  // Check if target relation exists in schema
  IF computedUserset.Object == TUPLE_USERSET_OBJECT:
    targetRelation = LoadRelation(targetNamespace, computedUserset.Relation, revision)
    IF targetRelation == nil:
      RETURN NoMembers()
  
  // Dispatch recursive check
  childResult = Check(
    resource: (targetNamespace, resource.ObjectId, computedUserset.Relation),
    subject: subject,
    revision: revision,
    depth: depth - 1,
    visited: visited
  )
  
  RETURN childResult
```

### 4.3 Multi-Resource Handling
If multiple resource IDs are being checked, filter for found matches early:
- If subject matches resource directly via relation equivalence, add as direct member.
- Only dispatch for remaining resource IDs.

---

## 5. Tuple-to-Userset Arrow (`->`)

### 5.1 Purpose
Walk from resource → intermediate subject → target relation.

### 5.2 Algorithm (FUNCTION_ANY)

```
checkTupleToUserset(
    resource: ObjectAndRelation,
    ttuRelation: TupleToUserset,
    subject: ObjectAndRelation,
    revision: DatastoreRevision,
    depth: uint32,
    visited: ExactVisitedSet
) -> MembershipResult:
  
  // Query for intermediate subjects from tupleset relation
  intermediates = QueryRelationships(
    resource: resource,
    relation: ttuRelation.Tupleset.Relation
  )
  
  dispatchedResults = {}
  
  FOR EACH intermediate IN intermediates:
    intermediateSubject = (intermediate.subject.Namespace, intermediate.subject.ObjectId)
    
    // Dispatch: walk from intermediate to computed userset
    childResult = Check(
      resource: (intermediate.subject.Namespace, intermediate.subject.ObjectId, 
                 ttuRelation.ComputedUserset.Relation),
      subject: subject,
      revision: revision,
      depth: depth - 1,
      visited: visited
    )
    
    IF childResult.IsMember:
      // Map result back to original resource ID
      dispatchedResults[resource.ObjectId] = childResult
      IF resultsSetting == ALLOW_SINGLE_RESULT:
        RETURN checkResultsForMembership(dispatchedResults)
  
  RETURN checkResultsForMembership(dispatchedResults)
```

### 5.3 Intersection Variant (FUNCTION_ALL)

For `tupleset_to_userset` with `function: ALL`:

```
checkIntersectionTupleToUserset(...) -> MembershipResult:
  
  // Query all intermediate subjects for resource
  intermediates = QueryRelationships(...)
  
  dispatchResults = {}  // Per subject-type results
  
  FOR EACH intermediate IN intermediates:
    childResult = checkComputedUserset(
      resource: (intermediate.subject.Namespace, intermediate.subject.ObjectId),
      subject: subject
    )
    dispatchResults[(intermediate.subject.Namespace, intermediate.subject.Relation)] = childResult
  
  // Check: for resource to be member, ALL subjects must be members
  resourcesFound = {}
  
  FOR EACH resource.ObjectId IN originalResources:
    subjectsForResource = intermediates[resource.ObjectId]  // All intermediate subjects
    
    hasAllSubjects = true
    caveats = []
    
    FOR EACH subject IN subjectsForResource:
      subjectTypeKey = (subject.Namespace, subject.Relation)
      IF !dispatchResults[subjectTypeKey].IsMember:
        hasAllSubjects = false
        BREAK
      
      IF dispatchResults[subjectTypeKey].Caveat != nil:
        caveats.Add(dispatchResults[subjectTypeKey].Caveat)
    
    IF hasAllSubjects:
      resourcesFound[resource.ObjectId] = Caveat.AND(caveats...)
  
  RETURN checkResultsForMembership(resourcesFound)
```

---

## 6. Set Operations on Userset (checkUsersetRewrite)

### 6.1 Union Operation

**Semantics:** A resource is a member if ANY child evaluation returns member.

```
checkUnion(
    resource: ObjectAndRelation,
    children: SetOperation[],
    subject: ObjectAndRelation,
    revision: DatastoreRevision,
    depth: uint32,
    visited: ExactVisitedSet
) -> MembershipResult:
  
  membershipSet = {}
  errors = []
  
  FOR EACH child IN children (parallel dispatch):
    childResult = runSetOperation(resource, child, subject, revision, depth, visited)
    
    IF childResult.Error != nil:
      errors.Add(childResult.Error)
      CONTINUE
    
    membershipSet.UnionWith(childResult.ResultsByResourceId)
    
    IF resultsSetting == ALLOW_SINGLE_RESULT && membershipSet.HasDeterminedMember():
      RETURN checkResultsForMembership(membershipSet)
  
  IF len(errors) > 0:
    RETURN CheckResult.Error(firstError)
  
  RETURN checkResultsForMembership(membershipSet)
```

### 6.2 Intersection Operation

**Semantics:** A resource is a member if ALL child evaluations return member.

```
checkIntersection(
    resource: ObjectAndRelation,
    children: SetOperation[],
    subject: ObjectAndRelation,
    revision: DatastoreRevision,
    depth: uint32,
    visited: ExactVisitedSet
) -> MembershipResult:
  
  membershipSet = null
  errors = []
  
  FOR EACH child IN children (parallel, resultsSetting = REQUIRE_ALL_RESULTS):
    childResult = runSetOperation(resource, child, subject, revision, depth, visited)
    
    IF childResult.Error != nil:
      RETURN CheckResult.Error(childResult.Error)
    
    IF membershipSet == null:
      membershipSet = NewMembershipSet()
      membershipSet.UnionWith(childResult.ResultsByResourceId)
    ELSE:
      membershipSet.IntersectWith(childResult.ResultsByResourceId)
    
    IF membershipSet.IsEmpty():
      RETURN NoMembers()
  
  RETURN checkResultsForMembership(membershipSet)
```

### 6.3 Exclusion Operation

**Semantics:** A resource is a member if first child is member AND no subsequent child is member.

```
checkExclusion(
    resource: ObjectAndRelation,
    children: SetOperation[],
    subject: ObjectAndRelation,
    revision: DatastoreRevision,
    depth: uint32,
    visited: ExactVisitedSet
) -> MembershipResult:
  
  // Base: first child determines initial membership
  baseResult = runSetOperation(resource, children[0], subject, revision, depth, visited)
  
  IF baseResult.Error != nil:
    RETURN baseResult
  
  membershipSet = NewMembershipSet()
  membershipSet.UnionWith(baseResult.ResultsByResourceId)
  
  IF membershipSet.IsEmpty():
    RETURN NoMembers()
  
  // Subtract all other children
  FOR i = 1 TO len(children) - 1:
    childResult = runSetOperation(resource, children[i], subject, revision, depth, visited)
    
    IF childResult.Error != nil:
      RETURN childResult
    
    membershipSet.Subtract(childResult.ResultsByResourceId)
    
    IF membershipSet.IsEmpty():
      RETURN NoMembers()
  
  RETURN checkResultsForMembership(membershipSet)
```

---

## 7. Special Cases

### 7.1 _self Operator

Checks if resource itself matches subject (when subject is the resource with ellipsis relation).

```
checkSelf(resource: ObjectAndRelation, subject: ObjectAndRelation) -> MembershipResult:
  
  IF subject.Relation != "...":
    RETURN NoMembers()
  
  IF resource.Namespace != subject.Namespace:
    RETURN NoMembers()
  
  IF resource.ObjectId == subject.ObjectId:
    RETURN checkResultsForMembership({ resource.ObjectId: true })
  
  RETURN NoMembers()
```

### 7.2 _nil Operator

Always returns no members.

```
checkNil() -> MembershipResult:
  RETURN NoMembers()
```

---

## 8. MembershipSet Data Structure

### 8.1 Purpose
Accumulate membership results across multiple resource IDs with caveat tracking.

### 8.2 API

```
record MembershipSet {
  // Add a direct member (no intermediate dispatch)
  void AddDirectMember(resourceId: string, caveat: CaveatExpression?)
  
  // Add a member found via parent caveat (e.g., TTU mapping)
  void AddMemberWithParentCaveat(resourceId: string, expression: Expression?, parentCaveat: CaveatExpression?)
  
  // Union with another result set
  void UnionWith(Map<string, MembershipResult> results)
  
  // Intersection: keep only resource IDs present in both
  void IntersectWith(Map<string, MembershipResult> results)
  
  // Subtraction: remove resource IDs present in other
  void Subtract(Map<string, MembershipResult> results)
  
  // Query membership status
  bool HasConcreteResourceID(resourceId: string)
  bool HasDeterminedMember()
  bool IsEmpty()
  
  // Export as map for response
  Map<string, MembershipResult> AsCheckResultsMap()
}
```

### 8.3 Implementation Notes
- Track exact membership vs. caveated membership separately.
- Caveat expressions combine (AND semantics) on multi-level dispatch.
- Intersection logic: keep only resource IDs present in all sets.

---

## 9. Datastore Reader Interface Requirements

### 9.1 QueryRelationships

```csharp
interface IDatastoreReader {
  IAsyncEnumerable<Relationship> QueryRelationships(
    RelationshipsFilter filter,
    QueryOptions options
  );
}

record RelationshipsFilter {
  string? OptionalResourceType;           // Namespace filter
  string? OptionalResourceRelation;       // Relation filter
  List<string>? OptionalResourceIds;      // Resource ID filter
  List<SubjectsSelector>? OptionalSubjectsSelectors;  // Subject multi-match
}

record SubjectsSelector {
  string? OptionalSubjectType;            // Subject namespace
  List<string>? OptionalSubjectIds;       // Subject object IDs
  SubjectRelationFilter RelationFilter;   // Relation constraints
}

record SubjectRelationFilter {
  // Returns true for ellipsis (...) relation
  WithEllipsisRelation()
  
  // Returns true for exact relation match
  WithRelation(string relationName)
  
  // Returns true for any non-ellipsis relation
  WithOnlyNonEllipsisRelations()
}

record Relationship {
  ObjectAndRelation Resource;
  ObjectAndRelation Subject;
  CaveatExpression? OptionalCaveat;
  DateTime? ExpirationTs;
}
```

### 9.2 ReadSchema

```csharp
interface IDatastoreReader {
  Task<ISchemaReader> ReadSchema(CancellationToken ct);
}

interface ISchemaReader {
  Task<(NamespaceDefinition Definition, bool Found)> 
    LookupTypeDefByName(string namespaceName, CancellationToken ct);
}
```

---

## 10. Request/Response Models

### 10.1 CheckRequest

```csharp
record CheckRequest {
  ObjectAndRelation ResourceRelation;  // Resource + relation to check
  List<string> ResourceIds;             // Multiple resource IDs
  ObjectAndRelation Subject;            // Subject for membership test
  Revision AtRevision;                  // Snapshot timestamp
  uint32 Depth;                         // Max recursion depth
  ResultsSetting Settings;              // ALLOW_SINGLE vs REQUIRE_ALL
  Debug DebugMode;                      // Debug tracing level
  List<CheckHint> Hints;                // Hint optimizations (deferred)
}

enum ResultsSetting {
  ALLOW_SINGLE_RESULT,    // Early exit on first member found
  REQUIRE_ALL_RESULTS     // Must evaluate all branches
}
```

### 10.2 CheckResponse

```csharp
record CheckResponse {
  Map<string, MembershipResult> ResultsByResourceId;
  ResponseMetadata Metadata;
}

record ResponseMetadata {
  uint32 DispatchCount;
  uint32 CachedDispatchCount;
  uint32 DepthRequired;
  DebugInformation? DebugInfo;
}
```

---

## 11. Exact Visited Set (Cycle Detection)

### 11.1 Purpose
Prevent infinite recursion on cyclic schema definitions.

### 11.2 Key Composition

```
visitKey = (
  resource.Namespace +
  resource.Relation +
  subject.Namespace +
  subject.Relation
)
```

### 11.2 Implementation

```csharp
class ExactVisitedSet {
  private HashSet<string> visited;
  
  bool Contains(string resourceNs, string resourceRel, 
                string subjectNs, string subjectRel) {
    string key = $"{resourceNs}#{resourceRel}@{subjectNs}#{subjectRel}";
    return visited.Contains(key);
  }
  
  void Add(string resourceNs, string resourceRel, 
           string subjectNs, string subjectRel) {
    string key = $"{resourceNs}#{resourceRel}@{subjectNs}#{subjectRel}";
    visited.Add(key);
  }
  
  ExactVisitedSet Copy() {
    return new ExactVisitedSet(new HashSet<string>(visited));
  }
}
```

---

## 12. Dispatch and Concurrency

### 12.1 Dispatch Chunking

When dispatching multiple non-terminal subjects:
- Group into chunks of size `dispatchChunkSize` (default 100).
- Each chunk becomes one recursive `Check()` call.
- Chunks execute in parallel (limited by `concurrencyLimit`).

```csharp
List<List<string>> DispatchChunks(
  List<Relationship> relationships,
  uint16 chunkSize
) {
  List<List<string>> chunks = new();
  List<string> currentChunk = new();
  
  foreach (var rel in relationships) {
    currentChunk.Add(rel.Subject.ObjectId);
    if (currentChunk.Count >= chunkSize) {
      chunks.Add(currentChunk);
      currentChunk = new();
    }
  }
  
  if (currentChunk.Count > 0) {
    chunks.Add(currentChunk);
  }
  
  return chunks;
}
```

### 12.2 Parallel Execution Pattern

```csharp
async Task<MembershipResult> DispatchAllAsync(
  List<Chunk> chunks,
  Func<Chunk, Task<CheckResult>> handler,
  uint16 concurrencyLimit
) {
  var tasks = chunks.Select(handler);
  var results = await TaskEx.WhenAll(tasks, concurrencyLimit);
  
  // Union/intersect/exclude results per operation type
  return CombineResults(results);
}
```

---

## 13. Caveat Expression Handling (Deferred)

For now, treat caveats as opaque containers. The spec defers actual caveat evaluation.

### 13.1 Caveat Combination Rules (Future)

- **Union (any)**: `OR` combination of child caveats.
- **Intersection (all)**: `AND` combination of child caveats.
- **Exclusion (not)**: Negate excluded branch caveats.

### 13.2 Storage in MembershipResult

```csharp
record MembershipResult {
  bool IsMember;
  CaveatExpression? Caveat;  // May combine multiple caveats
}
```

---

## 14. Algorithm Pseudo-Code Summary

```
CHECK(resource, relation, subject, revision, depth, visited):
  IF depth == 0:
    RETURN ERROR(ErrDepthExceeded)
  
  IF subject == "*:*":
    RETURN ERROR(ErrWildcardNotAllowed)
  
  // Load relation definition
  relationDef = LoadRelation(resource.Namespace, relation, revision)
  
  // Direct member fast path
  IF DirectMemberMatch(resource, relation, subject):
    RETURN MEMBER
  
  // Delegate to handler
  IF relationDef.HasUsersetRewrite:
    RETURN CheckUsersetRewrite(relationDef.UsersetRewrite, ...)
  ELSE:
    RETURN CheckDirect(relationDef, ...)

CHECKUSERSET(rewrite, ...):
  SWITCH rewrite.Operation:
    CASE UNION:
      RETURN Union(children, ...)
    CASE INTERSECTION:
      RETURN Intersection(children, ...)
    CASE EXCLUSION:
      RETURN Exclusion(children, ...)

CHECKDIRECT(relation, ...):
  foundMembers = {}
  
  // Phase 1: Direct + wildcard
  directTuples = QueryRelationships(directSubjectFilter)
  FOR tuple IN directTuples:
    foundMembers.Add(tuple.Resource.ObjectId)
  
  // Phase 2: Non-terminal dispatch
  IF nonTerminalsExist:
    nonTerminalTuples = QueryRelationships(nonTerminalFilter)
    FOR chunk IN Chunk(nonTerminalTuples, chunkSize):
      childResult = Check(chunk, ..., depth-1, visited)
      foundMembers.UnionWith(childResult)
  
  RETURN foundMembers

CHECKCOMPUTEDUSERSET(cu, ...):
  targetRR = (cu.Object == TUPLE_OBJECT ? 
              resource.Namespace : 
              parentNamespace, 
              cu.Relation)
  
  RETURN Check(targetRR, subject, ..., depth-1, visited)

CHECKTTUPLEUSERSET(ttu, ...):
  intermediates = QueryRelationships(ttu.Tupleset)
  
  FOR intermediate IN intermediates:
    childResult = Check(
      (intermediate.Subject.Namespace, intermediate.Subject.ObjectId, ttu.ComputedUserset.Relation),
      subject, ..., depth-1, visited)
    
    IF childResult.IsMember:
      foundMembers.Add(originalResource, childResult.Caveat)
  
  RETURN foundMembers
```

---

## 15. Implementation Checklist for C#

- [ ] Define `CheckRequest` and `CheckResponse` records
- [ ] Implement `MembershipSet` with union/intersection/subtract semantics
- [ ] Implement `ExactVisitedSet` for cycle detection
- [ ] Create `ICheckEvaluator` interface with `Check()` method
- [ ] Implement `CheckDirect()` with two-phase datastore querying
- [ ] Implement `CheckComputedUserset()` dispatcher
- [ ] Implement `CheckTupleToUserset()` and intersection variant
- [ ] Implement `CheckUsersetRewrite()` router
- [ ] Implement set operations: `Union()`, `Intersection()`, `Exclusion()`
- [ ] Implement special operators: `_self`, `_nil`
- [ ] Add depth validation and limiting
- [ ] Add result chunking and parallel dispatch
- [ ] Wire to `IDatastoreReader` for queries
- [ ] Add comprehensive error handling
- [ ] Add structured logging/tracing

---

## 16. Key Differences from SpiceDB Go Implementation

1. **Orleans Grain Distribution**: Dispatch may target remote grains; calls are async.
2. **Async/Await**: All datastore calls are async; use `IAsyncEnumerable` for streaming.
3. **Records vs Classes**: Use C# records for immutable data structures.
4. **Nullable Reference Types**: Leverage C# nullable annotations for caveat/optional fields.
5. **No Generics Varargs**: Use explicit overloads for `runSetOperation` variants.
6. **Visited Set**: A plain `HashSet<string>` (or immutable equivalent) from `System.Collections.Generic`/`System.Collections.Immutable`.

---

## 17. Caveats and Future Work

**Out of Scope (Deferred):**
- Caveat expression evaluation
- Expiration timestamp checking
- Check hints optimization
- Debug trace construction

**In Scope:**
- Direct tuple matching with ellipsis and wildcard
- Computed userset navigation
- Tuple-to-userset arrow walking
- Set operations (union, intersection, exclusion)
- Depth limiting
- Cycle detection via exact visited set
- Parallel dispatch with concurrency control
- Error propagation and context handling

---

## 18. Source References

**SpiceDB Check Implementation:**
- `/Users/davehillier/repos/spicedb/internal/graph/check.go` - Core check algorithm
- `/Users/davehillier/repos/spicedb/internal/dispatch/graph/graph.go` - Dispatch routing

**Spiceport Target:**
- `/Users/davehillier/repos/spiceport/src/Spiceport.Engine/` - Implementation location

End of Specification
