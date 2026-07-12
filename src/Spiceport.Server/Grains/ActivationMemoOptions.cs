namespace Spiceport.Grains;

/// <summary>
/// Toggle and idle-collection tuning for <see cref="CheckGrain"/>'s per-activation reply memo (stage
/// (a) of "Activation-as-cache" — see <c>docs/future-work.md</c> item 1.3). Default ON.
/// </summary>
/// <remarks>
/// When <see cref="MemoGrainOptions.Enabled"/> is false, <see cref="CheckGrain"/> never consults or
/// populates its memo and behaves exactly as it did before this feature existed.
/// </remarks>
public sealed class ActivationMemoOptions : MemoGrainOptions;
