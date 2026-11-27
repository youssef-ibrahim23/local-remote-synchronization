import 'dart:async';

import 'package:hive/hive.dart';
import 'package:local_remote_synchronization/local_remote_synchronization.dart';
import 'package:uuid/uuid.dart';

import 'hive_outbox_mapper.dart';
import 'hive_outbox_schema.dart';
import 'hive_outbox_writer.dart';

/// Hive implementation of [LocalStorageAdapter] for the durable outbox.
///
/// The application owns its Hive boxes and lifecycle, while this adapter owns
/// only its outbox box (defaulting to `sync_outbox`) and a metadata box for
/// internal counters.
///
/// Hive does not provide multi-key transactions. [runInTransaction] buffers
/// outbox writes and flushes them atomically on success, providing atomicity
/// for outbox operations only. Domain writes on other Hive boxes are NOT
/// covered by this transaction.
final class HiveLocalStorageAdapter implements LocalStorageAdapter {
  /// Creates a [HiveLocalStorageAdapter] using existing Hive [box] and
  /// [metadataBox].
  ///
  /// Both boxes must be opened before creating the adapter. The adapter does
  /// not open or close boxes - the application owns their lifecycle.
  HiveLocalStorageAdapter({
    required Box<String> box,
    required Box<int> metadataBox,
    String boxName = HiveOutboxSchema.defaultBoxName,
    String Function()? operationIdGenerator,
    DateTime Function()? clock,
  })  : _box = box,
        _metadataBox = metadataBox,
        _operationIdGenerator = operationIdGenerator ?? _defaultUuidV4,
        _clock = clock ?? _defaultUtcNow {
    // Validate boxName for API consistency, though the actual box is provided
    // by the caller.
    HiveOutboxSchema.validateBoxName(boxName);
  }

  static const Uuid _uuid = Uuid();

  static String _defaultUuidV4() => _uuid.v4();
  static DateTime _defaultUtcNow() => DateTime.now().toUtc();

  final Box<String> _box;
  final Box<int> _metadataBox;
  final String Function() _operationIdGenerator;
  final DateTime Function() _clock;
  final StreamController<int> _pendingCountController =
      StreamController<int>.broadcast();
  int _pendingCountVersion = 0;
  bool _disposed = false;

  /// Zone-unaware [OutboxWriter] for use outside of [runInTransaction].
  ///
  /// Writes are applied immediately to the box. For atomic outbox operations,
  /// use [runInTransaction] or [runWithTransaction] instead.
  OutboxWriter get outbox => HiveOutboxWriter(
        box: _box,
        metadataBox: _metadataBox,
        operationIdGenerator: _operationIdGenerator,
        clock: _clock,
      );

  /// Creates the outbox box and metadata box if they do not already exist.
  ///
  /// This is a no-op for Hive since boxes are opened by the application before
  /// creating the adapter. Included for API consistency with other adapters.
  Future<void> ensureInitialized() async {
    // Hive boxes are opened by the application. This method exists for API
    // consistency and future extensibility.
  }

  /// Executes [action] inside an atomic outbox transaction.
  ///
  /// Outbox writes through the provided [OutboxWriter] are buffered and
  /// flushed atomically on success. If [action] throws, all buffered writes
  /// are discarded.
  ///
  /// Note: Hive does not provide multi-key transactions. This method only
  /// guarantees atomicity for outbox operations, not for domain writes on
  /// other Hive boxes.
  @override
  Future<T> runInTransaction<T>(
    Future<T> Function(OutboxWriter outbox) action,
  ) async {
    final writer = HiveOutboxWriter(
      box: _box,
      metadataBox: _metadataBox,
      operationIdGenerator: _operationIdGenerator,
      clock: _clock,
      buffered: true,
    );

    try {
      final result = await action(writer);
      await writer.flushPending();
      await _emitPendingCount();
      return result;
    } catch (e) {
      writer.discardPending();
      rethrow;
    }
  }

  /// Alias for [runInTransaction] for API consistency with other adapters.
  Future<T> runWithTransaction<T>(
    Future<T> Function(OutboxWriter outbox) action,
  ) =>
      runInTransaction(action);

  /// Queries the current outbox count and notifies active [watchPendingCount]
  /// listeners.
  ///
  /// Call this after writing through [outbox] outside of [runInTransaction]
  /// to notify listeners of the new count.
  Future<void> refreshPendingCount() => _emitPendingCount();

  @override
  Future<int> recoverInFlightOperations() async {
    var recovered = 0;

    // Iterate all operations and update inFlight ones to pending.
    for (final key in _box.keys.toList()) {
      final json = _box.get(key);
      if (json == null) continue;

      try {
        final op = HiveOutboxMapper.fromJson(json);
        if (op.status == SyncOperationStatus.inFlight) {
          final updated = op.copyWith(
            status: SyncOperationStatus.pending,
          );
          await _box.put(key, HiveOutboxMapper.toJson(updated));
          recovered++;
        }
      } on FormatException {
        // Skip corrupted entries.
      }
    }

    await _emitPendingCount();
    return recovered;
  }

  @override
  Future<List<SyncOperation>> fetchEligibleOperations({
    required DateTime now,
    required int limit,
    bool preservePerEntityOrder = true,
  }) async {
    if (limit < 0) {
      throw ArgumentError.value(limit, 'limit', 'Must be >= 0.');
    }
    if (limit == 0) {
      return const <SyncOperation>[];
    }

    final nowMs = now.toUtc().millisecondsSinceEpoch;

    // Load all operations from the box.
    final allOperations = <SyncOperation>[];
    for (final key in _box.keys) {
      final json = _box.get(key);
      if (json == null) continue;

      try {
        allOperations.add(HiveOutboxMapper.fromJson(json));
      } on FormatException {
        // Skip corrupted entries.
      }
    }

    // Filter eligible operations.
    var eligible = allOperations.where((op) {
      // Pending operations are always eligible.
      if (op.status == SyncOperationStatus.pending) {
        return true;
      }

      // Failed operations are eligible if nextRetryAt <= now.
      if (op.status == SyncOperationStatus.failed &&
          op.nextRetryAt != null &&
          op.nextRetryAt!.toUtc().millisecondsSinceEpoch <= nowMs) {
        return true;
      }

      return false;
    }).toList();

    // Apply per-entity FIFO filter: an operation is eligible only if no
    // earlier operation (lower sequenceNumber) for the same entity exists
    // in the box. Since all stored operations are in blocking statuses
    // (pending, inFlight, failed, deadLetter), any earlier operation blocks
    // later ones for the same entity.
    if (preservePerEntityOrder) {
      // Build a set of entity keys that have at least one operation.
      // For each eligible op, check if there's an earlier op for same entity.
      final entityEarliestSeq = <String, int>{};
      for (final op in allOperations) {
        final entityKey = '${op.entityType}:${op.entityId}';
        final seq = op.sequenceNumber ?? 0;
        final existing = entityEarliestSeq[entityKey];
        if (existing == null || seq < existing) {
          entityEarliestSeq[entityKey] = seq;
        }
      }

      eligible = eligible.where((op) {
        final entityKey = '${op.entityType}:${op.entityId}';
        final earliestSeq = entityEarliestSeq[entityKey];
        final opSeq = op.sequenceNumber ?? 0;
        // Eligible only if this op IS the earliest for its entity.
        return opSeq == earliestSeq;
      }).toList();
    }

    // Sort eligible by createdAt then sequenceNumber.
    eligible.sort((a, b) {
      final createdCompare = a.createdAt.compareTo(b.createdAt);
      if (createdCompare != 0) return createdCompare;
      final aSeq = a.sequenceNumber ?? 0;
      final bSeq = b.sequenceNumber ?? 0;
      return aSeq.compareTo(bSeq);
    });

    return eligible.take(limit).toList(growable: false);
  }

  @override
  Future<void> markInFlight(String operationId) async {
    final op = await _getOperation(operationId);
    if (op == null) return;

    final updated = op.copyWith(status: SyncOperationStatus.inFlight);
    await _box.put(operationId, HiveOutboxMapper.toJson(updated));
    await _emitPendingCount();
  }

  @override
  Future<void> markSucceeded(String operationId) async {
    await _box.delete(operationId);
    await _emitPendingCount();
  }

  @override
  Future<void> markFailed(
    String operationId, {
    required int attemptCount,
    required DateTime nextRetryAt,
    required SyncError error,
  }) async {
    final op = await _getOperation(operationId);
    if (op == null) return;

    final updated = op.copyWith(
      status: SyncOperationStatus.failed,
      attemptCount: attemptCount,
      nextRetryAt: nextRetryAt,
      lastError: error,
    );
    await _box.put(operationId, HiveOutboxMapper.toJson(updated));
    await _emitPendingCount();
  }

  @override
  Future<void> markDeadLetter(
    String operationId, {
    required int attemptCount,
    required SyncError error,
  }) async {
    final op = await _getOperation(operationId);
    if (op == null) return;

    final updated = op.copyWith(
      status: SyncOperationStatus.deadLetter,
      attemptCount: attemptCount,
      clearNextRetryAt: true,
      lastError: error,
    );
    await _box.put(operationId, HiveOutboxMapper.toJson(updated));
    await _emitPendingCount();
  }

  @override
  Stream<int> watchPendingCount() {
    late final StreamController<int> controller;
    StreamSubscription<int>? subscription;
    var receivedLiveUpdate = false;

    controller = StreamController<int>(
      onListen: () async {
        if (_disposed) {
          await controller.close();
          return;
        }

        subscription = _pendingCountController.stream.listen(
          (count) {
            receivedLiveUpdate = true;
            if (!controller.isClosed) {
              controller.add(count);
            }
          },
          onError: (Object error, StackTrace stackTrace) {
            if (!controller.isClosed) {
              controller.addError(error, stackTrace);
            }
          },
          onDone: () {
            if (!controller.isClosed) {
              unawaited(controller.close());
            }
          },
        );

        final initialVersion = _pendingCountVersion;
        try {
          final initialCount = await _queryPendingCount();
          if (_disposed) {
            if (!controller.isClosed) {
              await controller.close();
            }
            return;
          }
          if (!controller.isClosed &&
              !receivedLiveUpdate &&
              initialVersion == _pendingCountVersion) {
            controller.add(initialCount);
          }
        } catch (error, stackTrace) {
          if (!controller.isClosed) {
            controller.addError(error, stackTrace);
          }
        }
      },
      onCancel: () async {
        await subscription?.cancel();
        if (!controller.isClosed) {
          await controller.close();
        }
      },
    );

    return controller.stream;
  }

  /// Releases internal stream resources without closing the caller-owned
  /// Hive boxes.
  Future<void> dispose() async {
    if (_disposed) {
      return;
    }
    _disposed = true;
    await _pendingCountController.close();
  }

  /// Retrieves a [SyncOperation] by [operationId], or null if not found.
  Future<SyncOperation?> _getOperation(String operationId) async {
    final json = _box.get(operationId);
    if (json == null) return null;

    try {
      return HiveOutboxMapper.fromJson(json);
    } on FormatException {
      return null;
    }
  }

  /// Counts all operations in the box (all statuses count as "pending" for
  /// the purposes of `watchPendingCount`).
  Future<int> _queryPendingCount() async => _box.length;

  Future<void> _emitPendingCount() async {
    final version = ++_pendingCountVersion;
    if (_disposed || !_pendingCountController.hasListener) {
      return;
    }
    try {
      final count = await _queryPendingCount();
      if (!_disposed &&
          !_pendingCountController.isClosed &&
          version == _pendingCountVersion) {
        _pendingCountController.add(count);
      }
    } on Object {
      // Ignore emissions if the caller already closed the box.
    }
  }
}
