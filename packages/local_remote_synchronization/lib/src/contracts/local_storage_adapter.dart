import '../models/sync_error.dart';
import '../models/sync_operation.dart';
import 'outbox_writer.dart';

/// Durable outbox storage contract and atomic transaction boundary.
///
/// Implementations must guarantee atomicity between domain entity writes
/// performed within their native transaction context and outbox insertions
/// performed through the provided [OutboxWriter].
///
/// Native database transaction types must not be exposed in this core
/// contract; concrete adapter packages may provide additional typed
/// transaction methods (for example, Sqflite `Transaction` callbacks or
/// Drift Zone-based transactions).
abstract interface class LocalStorageAdapter {
  /// Executes [action] inside an atomic database transaction.
  ///
  /// Domain writes bound to the adapter's active transaction and calls to
  /// [OutboxWriter.enqueue] must commit or roll back together.
  Future<T> runInTransaction<T>(
    Future<T> Function(OutboxWriter outbox) action,
  );

  /// Resets any `inFlight` operations back to `pending` on startup for crash
  /// recovery, returning the number of recovered operations.
  Future<int> recoverInFlightOperations();

  /// Fetches up to [limit] eligible operations (`pending`, or `failed` with
  /// `nextRetryAt <= now`) ordered by creation time and optional sequence
  /// number ascending.
  ///
  /// When [preservePerEntityOrder] is `true`, operations for an
  /// `(entityType, entityId)` pair that already has an earlier non-succeeded
  /// (`inFlight`, `failed`, or `deadLetter`) operation are excluded so
  /// per-entity causal order is preserved without head-of-line blocking
  /// unrelated entities.
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
