using Spiceport.Core;

namespace Spiceport.Datastore;

/// <summary>Base type for datastore errors.</summary>
public class DatastoreException : Exception
{
    /// <summary>Creates a datastore exception.</summary>
    public DatastoreException(string message) : base(message) { }

    /// <summary>Creates a datastore exception with an inner exception.</summary>
    public DatastoreException(string message, Exception inner) : base(message, inner) { }
}

/// <summary>Thrown when a requested revision is no longer available (garbage collected or never existed).</summary>
public sealed class RevisionNotFoundException : DatastoreException
{
    /// <summary>The revision that could not be found.</summary>
    public IRevision Revision { get; }

    /// <summary>Creates the exception for the given revision.</summary>
    public RevisionNotFoundException(IRevision revision)
        : base($"revision {revision} is no longer available") => Revision = revision;
}

/// <summary>
/// Thrown when a write conflicts with a concurrent write (a genuine write-write serialization
/// failure at commit). SpiceDB maps this to gRPC <c>Aborted</c> so the client retries the whole
/// transaction. This is distinct from <see cref="CreateRelationshipExistsException"/>, which is a
/// permanent CREATE-on-existing conflict mapped to <c>AlreadyExists</c>.
/// </summary>
public sealed class SerializationException : DatastoreException
{
    /// <summary>Creates a serialization exception.</summary>
    public SerializationException(string message = "transaction conflicted with a concurrent write") : base(message) { }
}

/// <summary>
/// Thrown when a CREATE update targets a relationship that already exists. SpiceDB models this as
/// <c>CreateRelationshipExistsError</c> and maps it to gRPC <c>AlreadyExists</c> (a permanent
/// duplicate-create error the client must NOT blindly retry as a transient conflict).
/// </summary>
public sealed class CreateRelationshipExistsException : DatastoreException
{
    /// <summary>Creates a create-conflict exception for the given (already-formatted) relationship.</summary>
    public CreateRelationshipExistsException(string relationship)
        : base($"relationship already exists: {relationship}") { }
}

/// <summary>Thrown when a revision is malformed or otherwise invalid for the datastore.</summary>
public sealed class InvalidRevisionException : DatastoreException
{
    /// <summary>Creates an invalid revision exception.</summary>
    public InvalidRevisionException(string message) : base(message) { }
}

/// <summary>Thrown when registering a relationship counter whose name is already live.</summary>
public sealed class CounterAlreadyRegisteredException : DatastoreException
{
    /// <summary>The counter name that was already registered.</summary>
    public string CounterName { get; }

    /// <summary>Creates the exception for the given counter name.</summary>
    public CounterAlreadyRegisteredException(string name)
        : base($"counter with name `{name}` is already registered") => CounterName = name;
}

/// <summary>Thrown when reading, counting, or unregistering a counter that is not live.</summary>
public sealed class CounterNotRegisteredException : DatastoreException
{
    /// <summary>The counter name that was not registered.</summary>
    public string CounterName { get; }

    /// <summary>Creates the exception for the given counter name.</summary>
    public CounterNotRegisteredException(string name)
        : base($"counter with name `{name}` not found") => CounterName = name;
}
