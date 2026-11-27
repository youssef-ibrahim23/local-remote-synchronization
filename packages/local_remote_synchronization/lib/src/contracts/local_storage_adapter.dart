import '../models/sync_error.dart';
import '../models/sync_operation.dart';
import 'outbox_writer.dart';

/// Durable outbox storage contract and outbox transaction boundary.
///
/// Core remains database-agnostic and does not expose native database
/// transaction handles. Consequently, the generic [runInTransaction] method
/// only guarantees atomicity for outbox operations performed through the
/// provided [OutboxWriter] (and for Zone-based databases such as Drift when
/// domain writes run on the same Zone-bound database).
///
/// When an application's domain writes require an explicit native transaction
/// handle (for example, Sqflite's `Transaction`), the concrete adapter package
/// must provide an adapter-specific typed transaction API that binds both the
/// application's domain writes and the [OutboxWriter] to the exact same
/// underlying native database transaction so they commit or roll back together.
abstract interface class LocalStorageAdapter {
  /// Executes [action] inside an atomic outbox transaction.
  ///
  /// Calls to [OutboxWriter.enqueue] within [action] commit or roll back
  /// together. For adapters that use explicit transaction handles rather than
  /// Zone-scoped transactions, use the adapter's typed transaction API to
  /// include application domain writes in the same native transaction.
  Future<T> runInTransaction<T>(Future<T> Function(OutboxWriter outbox) action);

  /// Resets any `inFlight` operations back to `pending` on startup for crash
  /// recovery, returning the number of recovered operations.
  Future<int> recoverInFlightOperations();

  /// Fetches up to [limit] eligible operations (`pending`, or `failed` with
  /// `nextRetryAt <= now`) ordered by creation time and optional sequence
  /// number ascending.
  ///
  /// When [preservePerEntityOrder] is `true`, an operation is not eligible if
  /// any earlier operation for the same `(entityType, entityId)` has a status
  /// other than `succeeded` — including `pending`, `inFlight`, `failed`, and
  /// `deadLetter`. This preserves per-entity causal order without head-of-line
  /// blocking unrelated entities.
  ///
  /// Note: A `deadLetter` operation continues blocking newer operations for
  /// the same `(entityType, entityId)` until an explicit resolution or discard
  /// mechanism is introduced.
  Future<List<SyncOperation>> fetchEligibleOperations({
    required DateTime now,
    required int limit,
    bool preservePerEntityOrder = true,
  });

  /// Marks the operation identified by [operationId] as `inFlight`.
  Future<void> markInFlight(String operationId);

  /// Marks the operation identified by [operationId] as succeeded and removes
  /// it from the active outbox.
  ///
  /// Must be idempotent: calling this for an already-removed operation must
  /// complete normally without throwing.
  Future<void> markSucceeded(String operationId);

  /// Marks the operation identified by [operationId] as `failed`, persisting
  /// [attemptCount], [nextRetryAt], and [error] durably.
  Future<void> markFailed(
    String operationId, {
    required int attemptCount,
    required DateTime nextRetryAt,
    required SyncError error,
  });

  /// Marks the operation identified by [operationId] as `deadLetter`,
  /// persisting [attemptCount] and [error] durably.
  Future<void> markDeadLetter(
    String operationId, {
    required int attemptCount,
    required SyncError error,
  });

  /// Emits the count of active (non-succeeded) operations in the outbox.
  Stream<int> watchPendingCount();
}
