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

/// <summary>Thrown when a write conflicts with a concurrent write (serialization failure).</summary>
public sealed class SerializationException : DatastoreException
{
    /// <summary>Creates a serialization exception.</summary>
    public SerializationException(string message = "transaction conflicted with a concurrent write") : base(message) { }
}

/// <summary>Thrown when a revision is malformed or otherwise invalid for the datastore.</summary>
public sealed class InvalidRevisionException : DatastoreException
{
    /// <summary>Creates an invalid revision exception.</summary>
    public InvalidRevisionException(string message) : base(message) { }
}
