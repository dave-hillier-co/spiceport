using System.Reflection;
using Orleans;
using Orleans.Runtime;
using Orleans.Serialization.Invocation;
using Spiceport.Grains.Abstractions;

namespace Spiceport.Grains.Tests;

/// <summary>
/// Minimal <see cref="IOutgoingGrainCallContext"/> fake for driving <see cref="CheckDispatchOutgoingCallFilter"/>
/// directly, without a real silo/grain call. Only <see cref="InterfaceMethod"/> and <see cref="Invoke"/> are
/// read by the filter under test; every other member is unused by it and throws if touched.
/// </summary>
internal sealed class FakeOutgoingGrainCallContext : IOutgoingGrainCallContext
{
    private readonly Func<Task> _invoke;

    /// <summary>Creates a fake outgoing call whose body is <paramref name="invoke"/>.</summary>
    /// <param name="invoke">The call body: throw to simulate a faulted grain call, or complete normally.</param>
    /// <param name="interfaceMethod">
    /// The method the filter sees as the target of this call. Defaults to
    /// <see cref="ICheckGrain.DispatchCheck"/> so the filter matches and applies its translation.
    /// </param>
    public FakeOutgoingGrainCallContext(Func<Task> invoke, MethodInfo? interfaceMethod = null)
    {
        ArgumentNullException.ThrowIfNull(invoke);
        _invoke = invoke;
        InterfaceMethod = interfaceMethod ?? typeof(ICheckGrain).GetMethod(nameof(ICheckGrain.DispatchCheck))!;
    }

    public IGrainContext SourceContext => throw new NotSupportedException();
    public IInvokable Request => throw new NotSupportedException();
    public object Grain => throw new NotSupportedException();
    public GrainId? SourceId => null;
    public GrainId TargetId => default;
    public GrainInterfaceType InterfaceType => default;
    public string InterfaceName => InterfaceMethod.DeclaringType?.Name ?? string.Empty;
    public string MethodName => InterfaceMethod.Name;
    public MethodInfo InterfaceMethod { get; }
    public object? Result { get; set; }
    public Response? Response { get; set; }

    public Task Invoke() => _invoke();
}

/// <summary>
/// Minimal <see cref="IIncomingGrainCallContext"/> fake for driving <see cref="CheckDispatchIncomingCallFilter"/>
/// directly. Only <see cref="InterfaceMethod"/> and <see cref="Invoke"/> are read by the filter under test.
/// </summary>
internal sealed class FakeIncomingGrainCallContext : IIncomingGrainCallContext
{
    private readonly Func<Task> _invoke;

    /// <summary>Creates a fake incoming call whose body is <paramref name="invoke"/>.</summary>
    /// <param name="invoke">
    /// The grain-body stand-in: completes normally unless the filter itself rejects the call first.
    /// </param>
    /// <param name="interfaceMethod">
    /// The method the filter sees as the target of this call. Defaults to
    /// <see cref="ICheckGrain.DispatchCheck"/> so the filter matches and applies its boundary guard.
    /// </param>
    public FakeIncomingGrainCallContext(Func<Task>? invoke = null, MethodInfo? interfaceMethod = null)
    {
        _invoke = invoke ?? (() => Task.CompletedTask);
        InterfaceMethod = interfaceMethod ?? typeof(ICheckGrain).GetMethod(nameof(ICheckGrain.DispatchCheck))!;
    }

    public IGrainContext TargetContext => throw new NotSupportedException();
    public MethodInfo ImplementationMethod => InterfaceMethod;
    public IInvokable Request => throw new NotSupportedException();
    public object Grain => throw new NotSupportedException();
    public GrainId? SourceId => null;
    public GrainId TargetId => default;
    public GrainInterfaceType InterfaceType => default;
    public string InterfaceName => InterfaceMethod.DeclaringType?.Name ?? string.Empty;
    public string MethodName => InterfaceMethod.Name;
    public MethodInfo InterfaceMethod { get; }
    public object? Result { get; set; }
    public Response? Response { get; set; }

    public Task Invoke() => _invoke();
}
