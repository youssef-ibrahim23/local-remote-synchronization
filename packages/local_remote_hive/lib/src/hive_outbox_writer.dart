import 'package:local_remote_synchronization/local_remote_synchronization.dart';

import 'hive_outbox_snapshot.dart';

/// Hive-backed [OutboxWriter] that enqueues [LocalMutation]s into a snapshot.
///
/// This writer is used inside `HiveLocalStorageAdapter.runInTransaction`.
/// It operates on a mutable copy of the [HiveOutboxSnapshot] and buffers
/// operations in memory. The adapter persists the complete snapshot using
/// one `Box.put` call after the transaction action completes successfully.
///
/// Sequence numbers are allocated from `snapshot.lastSequenceNumber`, which
/// is incremented within the adapter's serialized mutation boundary.
final class HiveOutboxWriter implements OutboxWriter {
  /// Creates a [HiveOutboxWriter] bound to [snapshot].
  ///
  /// [snapshot] must be a mutable working copy — changes to
  /// `lastSequenceNumber` are applied immediately, while operations are
  /// buffered until [flushPending] is called.
  HiveOutboxWriter({
    required HiveOutboxSnapshot snapshot,
    required String Function() operationIdGenerator,
    required DateTime Function() clock,
  })  : _snapshot = snapshot,
        _operationIdGenerator = operationIdGenerator,
        _clock = clock;

  final HiveOutboxSnapshot _snapshot;
  final String Function() _operationIdGenerator;
  final DateTime Function() _clock;

  /// Operations buffered during the transaction.
  final List<SyncOperation> _pendingOperations = <SyncOperation>[];

  /// Whether this writer has pending buffered operations.
  bool get hasPendingWrites => _pendingOperations.isNotEmpty;

  @override
  Future<SyncOperation> enqueue(LocalMutation mutation) async {
    final now = _clock().toUtc();
    final operationId = _operationIdGenerator();

    // Allocate sequence number from the snapshot's counter.
    // The adapter serializes mutations, so this is safe.
    _snapshot.lastSequenceNumber++;
    final sequenceNumber = _snapshot.lastSequenceNumber;

    final operation = SyncOperation.fromMutation(
      mutation,
      operationId: operationId,
      now: now,
    ).copyWith(sequenceNumber: sequenceNumber);

    _pendingOperations.add(operation);

    return operation;
  }

  /// Applies all buffered operations to the snapshot's operation list.
  ///
  /// Called by the adapter after a successful transaction. Does not persist
  /// to Hive — the adapter handles persistence.
  void flushPending() {
    if (_pendingOperations.isEmpty) {
      return;
    }
    _snapshot.operations.addAll(_pendingOperations);
    _pendingOperations.clear();
  }

  /// Discards all buffered operations without applying them.
  ///
  /// Called by the adapter when the transaction action throws. The working
  /// snapshot copy is discarded entirely, so no rollback is needed here.
  void discardPending() {
    _pendingOperations.clear();
  }
}
