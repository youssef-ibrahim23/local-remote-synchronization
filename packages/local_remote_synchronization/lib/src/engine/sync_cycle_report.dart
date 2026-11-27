part of 'sync_engine.dart';

/// Immutable summary of a single sync cycle.
///
/// Exactly one report is constructed per cycle, after all storage
/// transitions have settled, and [completedAt] is always populated. The
/// same instance is emitted on `SyncEngine.reports` and returned by
/// `SyncEngine.syncNow()`.
///
/// **Field availability:** counters tied to later phases stay at `0`
/// until their phase lands — `failed`, `deadLettered`, and
/// `storageErrors` from Phase 3.2, `conflicted` from Phase 3.3.
@immutable
final class SyncCycleReport {
  /// Creates a [SyncCycleReport].
  ///
  /// Internal to the sync engine — applications should consume reports
  /// via `SyncEngine.reports` or `SyncEngine.syncNow`.
  const SyncCycleReport._({
    required this.startedAt,
    required this.completedAt,
    required this.fetched,
    required this.succeeded,
    required this.failed,
    required this.conflicted,
    required this.deadLettered,
    required this.storageErrors,
    required this.cycleError,
    required this.skippedOffline,
  });

  /// When the cycle started.
  final DateTime startedAt;

  /// When the cycle completed. Always populated — the report is only
  /// constructed after the cycle has fully finished.
  final DateTime completedAt;

  /// Number of operations returned by `fetchEligibleOperations` across
  /// all batch iterations in the cycle, **before** any filtering.
  /// This counts raw adapter results, not unique operations selected
  /// for pushing.
  final int fetched;

  /// Number of operations successfully pushed and confirmed removed
  /// from the outbox. Incremented **only** when `markSucceeded`
  /// completes without throwing.
  final int succeeded;

  /// Number of operations that failed and were scheduled for retry.
  /// Incremented **only** when `markFailed` completes without throwing.
  final int failed;

  /// Number of operations whose remote result was `SyncConflict`.
  /// This is a subset of [deadLettered].
  final int conflicted;

  /// Number of operations transitioned to `deadLetter`, including
  /// conflicts, non-retryable failures, max-attempt exhaustion, and
  /// protocol violations.
  final int deadLettered;

  /// Number of operation-level storage transition failures: a push
  /// result was received but the corresponding storage transition
  /// threw. Cycle-level storage failures (`fetchEligibleOperations` or
  /// `markInFlight` throwing) are NOT counted here — they set
  /// [cycleError] and abort the cycle instead.
  final int storageErrors;

  /// Cycle-level error that prevented the cycle from completing
  /// normally (e.g., `fetchEligibleOperations` threw, `markInFlight`
  /// failed, a protocol violation, or an operation-level storage
  /// failure in Phase 3.1). Null if the cycle completed without
  /// cycle-level errors.
  final SyncError? cycleError;

  /// Whether the cycle was skipped because the device is offline.
  final bool skippedOffline;

  /// Duration of the sync cycle.
  Duration get duration => completedAt.difference(startedAt);

  /// Whether the cycle completed **without any failure**.
  ///
  /// Returns `false` if [failed], [conflicted], [deadLettered], or
  /// [storageErrors] is greater than zero, or [cycleError] is non-null.
  ///
  /// A `skippedOffline` report returns `true` because no failure
  /// occurred — but this does **NOT** mean any operation was
  /// synchronized. `isSuccess == true` is **not proof that
  /// synchronization occurred**; inspect [succeeded], [fetched], and
  /// [skippedOffline] for that.
  bool get isSuccess =>
      cycleError == null &&
      failed == 0 &&
      conflicted == 0 &&
      deadLettered == 0 &&
      storageErrors == 0;

  @override
  String toString() =>
      'SyncCycleReport(fetched: $fetched, succeeded: $succeeded, '
      'failed: $failed, conflicted: $conflicted, '
      'deadLettered: $deadLettered, storageErrors: $storageErrors, '
      'skippedOffline: $skippedOffline, cycleError: $cycleError, '
      'duration: $duration)';
}
