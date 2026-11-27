import 'package:hive/hive.dart';
import 'package:local_remote_synchronization/local_remote_synchronization.dart';

import 'hive_outbox_mapper.dart';
import 'hive_outbox_schema.dart';

/// Hive-backed [OutboxWriter] that enqueues [LocalMutation]s into a Hive box.
///
/// When `buffered` is `true`, `enqueue` stores operations in memory and they
/// are flushed to the box by `flushPending`. This provides atomicity for
/// outbox operations within `HiveLocalStorageAdapter.runInTransaction`.
///
/// When `buffered` is `false` (the default), `enqueue` writes directly to the
/// box. This mode is used by `adapter.outbox` for external writes.
final class HiveOutboxWriter implements OutboxWriter {
  /// Creates a [HiveOutboxWriter] bound to [box] and [metadataBox].
  HiveOutboxWriter({
    required Box<String> box,
    required Box<int> metadataBox,
    required String Function() operationIdGenerator,
    required DateTime Function() clock,
    bool buffered = false,
  })  : _box = box,
        _metadataBox = metadataBox,
        _operationIdGenerator = operationIdGenerator,
        _clock = clock,
        _buffered = buffered;

  final Box<String> _box;
  final Box<int> _metadataBox;
  final String Function() _operationIdGenerator;
  final DateTime Function() _clock;
  final bool _buffered;

  /// Pending operations buffered during a transaction.
  final Map<String, SyncOperation> _pendingOperations =
      <String, SyncOperation>{};

  /// In-memory sequence counter offset for buffered mode.
  int _sequenceOffset = 0;

  /// Base sequence number captured at transaction start (buffered mode only).
  int _baseSequenceNumber = 0;

  /// Whether this writer has pending buffered operations.
  bool get hasPendingWrites => _pendingOperations.isNotEmpty;

  @override
  Future<SyncOperation> enqueue(LocalMutation mutation) async {
    final now = _clock().toUtc();
    final operationId = _operationIdGenerator();
    final initial = SyncOperation.fromMutation(
      mutation,
      operationId: operationId,
      now: now,
    );

    final sequenceNumber = await _nextSequenceNumber();
    final operation = initial.copyWith(sequenceNumber: sequenceNumber);

    if (_buffered) {
      _pendingOperations[operationId] = operation;
    } else {
      await _box.put(operationId, HiveOutboxMapper.toJson(operation));
    }

    return operation;
  }

  /// Flushes all buffered operations to the box and updates the sequence
  /// counter. Only meaningful when `buffered` is `true`.
  Future<void> flushPending() async {
    if (_pendingOperations.isEmpty) {
      return;
    }

    // Write all operations and update the counter atomically-ish.
    final writes = <String, String>{};
    for (final op in _pendingOperations.values) {
      writes[op.operationId] = HiveOutboxMapper.toJson(op);
    }
    await _box.putAll(writes);

    // Persist the final sequence counter.
    final finalSequence = _baseSequenceNumber + _sequenceOffset;
    await _metadataBox.put(
      HiveOutboxSchema.sequenceCounterKey,
      finalSequence,
    );

    _pendingOperations.clear();
    _sequenceOffset = 0;
  }

  /// Discards all buffered operations without writing them.
  void discardPending() {
    _pendingOperations.clear();
    _sequenceOffset = 0;
  }

  /// Returns the next sequence number.
  ///
  /// In buffered mode, uses an in-memory counter offset from the base value
  /// captured at transaction start. In direct mode, reads and increments the
  /// persistent counter immediately.
  Future<int> _nextSequenceNumber() async {
    if (_buffered) {
      if (_sequenceOffset == 0) {
        // Capture base on first enqueue.
        _baseSequenceNumber = _metadataBox.get(
              HiveOutboxSchema.sequenceCounterKey,
              defaultValue: 0,
            ) ??
            0;
      }
      _sequenceOffset++;
      return _baseSequenceNumber + _sequenceOffset;
    } else {
      final current = _metadataBox.get(
            HiveOutboxSchema.sequenceCounterKey,
            defaultValue: 0,
          ) ??
          0;
      final next = current + 1;
      await _metadataBox.put(HiveOutboxSchema.sequenceCounterKey, next);
      return next;
    }
  }
}
