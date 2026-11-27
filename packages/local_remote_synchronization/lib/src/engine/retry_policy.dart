import '../models/sync_error.dart';

/// Strategy for computing retry delays and dead-letter decisions.
///
/// Implementations must safely support concurrent calls within a single
/// Dart isolate. They may be stateful (e.g., for jitter RNG), but must
/// not corrupt shared state when called concurrently from multiple
/// `_pushAndHandle` futures within one sync cycle.
///
/// The engine calls [computeDelay] and [shouldDeadLetter] for each failed
/// operation.
///
/// Both methods validate that [attemptCount] is >= 1. The engine
/// guarantees this by passing `operation.attemptCount + 1` (where
/// `operation.attemptCount` starts at 0).
///
/// **Exception semantics:** Implementations should treat a throw from
/// either method as a programmer error, not a transient condition. The
/// engine does **not** catch policy exceptions per-operation — they
/// propagate to the cycle error boundary, which aborts the cycle with
/// `cycleError` set. Rationale: a broken policy affects every failed
/// operation identically, and the engine cannot determine the correct
/// storage transition without a delay/dead-letter decision. Aborting is
/// consistent with the `markInFlight` failure policy.
abstract interface class RetryPolicy {
  /// Computes the delay before the next retry attempt.
  ///
  /// [attemptCount] is the number of completed push attempts (1 after
  /// the first failure, 2 after the second, etc.). Must be >= 1.
  ///
  /// [error] is the [SyncError] that caused the failure. If
  /// [SyncError.retryAfter] is non-null, implementations **must** honor
  /// it as a minimum delay — local backoff and jitter must not produce
  /// a delay shorter than `retryAfter`. A server-provided `retryAfter`
  /// larger than any local `maxDelay` is preserved.
  ///
  /// If this method throws, the exception propagates to the cycle error
  /// boundary: the cycle aborts with `cycleError`, the operation remains
  /// `inFlight` (claimed, result received, no transition applied), and
  /// it is recovered to `pending` on the next `SyncEngine.start`. No
  /// outcome counter is incremented for it.
  Duration computeDelay(int attemptCount, SyncError error);

  /// Returns `true` if the operation should be moved to `deadLetter`
  /// instead of being scheduled for another retry.
  ///
  /// [attemptCount] is the number of completed push attempts. Must be
  /// >= 1. [error] is the [SyncError] that caused the failure.
  ///
  /// If this method throws, the exception propagates to the cycle error
  /// boundary with the same semantics as [computeDelay].
  bool shouldDeadLetter(int attemptCount, SyncError error);
}
