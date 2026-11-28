import '../models/sync_operation.dart';
import '../models/sync_result.dart';

/// Emitted on [SyncEngine.conflicts] after a conflict has been durably
/// persisted as `deadLetter`.
///
/// The event is emitted only after [LocalStorageAdapter.markDeadLetter]
/// has completed without throwing. If the storage transition throws, the
/// persisted state is **unknown** (the operation may or may not have been
/// dead-lettered), [SyncCycleReport.storageErrors] is incremented, no
/// event is emitted, and the error is logged.
///
/// [SyncConflictEvent.operation] is a **pre-transition snapshot** of the
/// `SyncOperation` as it was fetched from the outbox. Its `status` field
/// reflects the state at fetch time (e.g., `pending` or `failed`), not
/// `deadLetter`. The adapter does **not** mutate the in-memory
/// `SyncOperation` instance — the engine holds a reference to the same
/// immutable object that was fetched.
///
/// Delivery semantics: the event is added to a broadcast stream. If no
/// listener is attached at the time of emission, the event is lost. The
/// `remoteSnapshot` is not persisted separately — if the process crashes
/// before the application handles the event, the snapshot may be lost.
///
/// The operation remains `deadLetter` in the outbox after notification
/// (assuming `markDeadLetter` succeeded). Later operations for the same
/// entity remain blocked. The Core contract does **not** provide a
/// supported conflict-resolution or discard workflow.
/// [LocalStorageAdapter.markSucceeded] is semantically incorrect as a
/// conflict-resolution API (it means "remote accepted", not "discard
/// conflict") and must not be used as one.
final class SyncConflictEvent {
  /// Creates a [SyncConflictEvent].
  ///
  /// [operation] is the pre-transition snapshot of the operation whose
  /// remote push returned [conflict]. Its `status` reflects the fetch-time
  /// state, not `deadLetter`.
  const SyncConflictEvent({
    required this.operation,
    required this.conflict,
  });

  /// Pre-transition snapshot of the operation that encountered the
  /// conflict. Its `status` reflects the fetch-time state, not
  /// `deadLetter`.
  final SyncOperation operation;

  /// The conflict result from the remote adapter, including
  /// [SyncConflict.remoteSnapshot].
  final SyncConflict conflict;
}
