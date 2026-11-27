import '../models/local_mutation.dart';
import '../models/sync_operation.dart';

/// Transaction-scoped handle for atomically enqueuing [LocalMutation]s into
/// the durable outbox.
///
/// Implementations are bound to the active database transaction started by
/// `LocalStorageAdapter.runInTransaction` (or an adapter-specific typed
/// transaction method) so that domain entity writes and outbox insertion
/// commit or roll back together.
abstract interface class OutboxWriter {
  /// Enqueues [mutation] into the durable outbox within the active
  /// transaction and returns the persisted [SyncOperation].
  ///
  /// The implementation is responsible for generating a unique, stable
  /// `operationId` and persisting `idempotencyKey` as
  /// `mutation.idempotencyKey ?? operationId`.
  Future<SyncOperation> enqueue(LocalMutation mutation);
}
