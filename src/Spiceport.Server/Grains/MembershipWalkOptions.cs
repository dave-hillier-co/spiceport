namespace Spiceport.Grains;

/// <summary>Toggle for the Leopard membership-walk accelerator. Default ON (opt-out).</summary>
/// <remarks>
/// When <see cref="MemoGrainOptions.Enabled"/> is false the accelerator is never consulted; lookups run
/// the live traversal.
/// </remarks>
public sealed class MembershipWalkOptions : MemoGrainOptions;
