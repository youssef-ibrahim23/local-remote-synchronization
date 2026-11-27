import 'dart:async';

import 'package:hive/hive.dart';
import 'package:local_remote_synchronization/local_remote_synchronization.dart';
import 'package:uuid/uuid.dart';

import 'hive_outbox_schema.dart';
import 'hive_outbox_snapshot.dart';
import 'hive_outbox_writer.dart';

/// Hive implementation of [LocalStorageAdapter] for the durable outbox.
///
/// The application owns its Hive box and lifecycle. The adapter stores all
/// outbox state in a single serialized [HiveOutboxSnapshot] under
/// [HiveOutboxSchema.snapshotKey] within the box.
///
/// ## Persistence model
///
/// All operations and the sequence counter are stored in one JSON snapshot.
/// Each mutation loads the snapshot, applies changes in memory, and writes
/// the complete snapshot back in a single [Box.put] call. This provides
/// atomicity for outbox operations within a single Hive box write.
///
/// **Durability limitation:** A single Hive box write is not an unconditional
/// crash-atomic or ACID guarantee. Hive may buffer writes in memory before
/// flushing to disk. For stronger durability guarantees, consider calling
/// `box.compact()` periodically or using a database with explicit transaction
/// support (Drift, Sqflite).
///
/// ## Concurrency
///
/// All mutations are serialized through an internal async mutex. This
/// prevents race conditions in sequence number allocation and ensures
/// read-modify-write cycles are atomic within the adapter instance.
///
/// **Note:** The mutex is per-adapter-instance. If multiple adapter instances
/// share the same Hive box, they can still race. Use a single adapter
/// instance per box.
final class HiveLocalStorageAdapter implements LocalStorageAdapter {
  /// Creates a [HiveLocalStorageAdapter] using an existing Hive [box].
  ///
  /// The box must be opened before creating the adapter. The adapter does
  /// not open or close boxes — the application owns their lifecycle.
  HiveLocalStorageAdapter({
    required Box<String> box,
    String boxName = HiveOutboxSchema.defaultBoxName,
    String Function()? operationIdGenerator,
    DateTime Function()? clock,
  })  : _box = box,
        _operationIdGenerator = operationIdGenerator ?? _defaultUuidV4,
        _clock = clock ?? _defaultUtcNow {
    HiveOutboxSchema.validateBoxName(boxName);
  }

  static const Uuid _uuid = Uuid();

  static String _defaultUuidV4() => _uuid.v4();
  static DateTime _defaultUtcNow() => DateTime.now().toUtc();

  final Box<String> _box;
  final String Function() _operationIdGenerator;
  final DateTime Function() _clock;

  /// Serializes all mutations to prevent race conditions.
  final _AsyncMutex _mutex = _AsyncMutex();

  final StreamController<int> _pendingCountController =
      StreamController<int>.broadcast();
  int _pendingCountVersion = 0;
  bool _disposed = false;

  /// Zone-unaware [OutboxWriter] for use outside of [runInTransaction].
  ///
  /// Writes are applied immediately to the snapshot. For atomic outbox
  /// operations, use [runInTransaction] or [runWithTransaction] instead.
  OutboxWriter get outbox => _DirectOutboxWriter(this);

  /// Creates the outbox snapshot if it does not already exist.
  ///
  /// This is a no-op for Hive since the box is opened by the application
  /// before creating the adapter. Included for API consistency.
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
  /// guarantees atomicity for outbox operations within a single snapshot
  /// write, not for domain writes on other Hive boxes.
  @override
  Future<T> runInTransaction<T>(
    Future<T> Function(OutboxWriter outbox) action,
  ) =>
      _mutex.run(() async {
        final snapshot = await _loadSnapshot();
        final workingSnapshot = snapshot.copy();

        final writer = HiveOutboxWriter(
          snapshot: workingSnapshot,
          operationIdGenerator: _operationIdGenerator,
          clock: _clock,
        );

        try {
          final result = await action(writer);
          writer.flushPending();
          await _persistSnapshot(workingSnapshot);
          await _emitPendingCount();
          return result;
        } catch (e) {
          writer.discardPending();
          rethrow;
        }
      });

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

    await _mutate((snapshot) async {
      for (var i = 0; i < snapshot.operations.length; i++) {
        final op = snapshot.operations[i];
        if (op.status == SyncOperationStatus.inFlight) {
          snapshot.operations[i] = op.copyWith(
            status: SyncOperationStatus.pending,
          );
          recovered++;
        }
      }
    });

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
    final snapshot = await _loadSnapshot();
    final allOperations = snapshot.operations;

    // Filter eligible operations.
    var eligible = allOperations.where((op) {
      if (op.status == SyncOperationStatus.pending) {
        return true;
      }
      if (op.status == SyncOperationStatus.failed &&
          op.nextRetryAt != null &&
          op.nextRetryAt!.toUtc().millisecondsSinceEpoch <= nowMs) {
        return true;
      }
      return false;
    }).toList();

    // Apply per-entity FIFO filter.
    if (preservePerEntityOrder) {
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
        return opSeq == earliestSeq;
      }).toList();
    }

    // Sort by createdAt then sequenceNumber.
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
    await _mutate((snapshot) async {
      final index = snapshot.operations.indexWhere(
        (op) => op.operationId == operationId,
      );
      if (index == -1) return;
      snapshot.operations[index] = snapshot.operations[index].copyWith(
        status: SyncOperationStatus.inFlight,
      );
    });
    await _emitPendingCount();
  }

  @override
  Future<void> markSucceeded(String operationId) async {
    await _mutate((snapshot) async {
      snapshot.operations.removeWhere((op) => op.operationId == operationId);
    });
    await _emitPendingCount();
  }

  @override
  Future<void> markFailed(
    String operationId, {
    required int attemptCount,
    required DateTime nextRetryAt,
    required SyncError error,
  }) async {
    await _mutate((snapshot) async {
      final index = snapshot.operations.indexWhere(
        (op) => op.operationId == operationId,
      );
      if (index == -1) return;
      snapshot.operations[index] = snapshot.operations[index].copyWith(
        status: SyncOperationStatus.failed,
        attemptCount: attemptCount,
        nextRetryAt: nextRetryAt,
        lastError: error,
      );
    });
    await _emitPendingCount();
  }

  @override
  Future<void> markDeadLetter(
    String operationId, {
    required int attemptCount,
    required SyncError error,
  }) async {
    await _mutate((snapshot) async {
      final index = snapshot.operations.indexWhere(
        (op) => op.operationId == operationId,
      );
      if (index == -1) return;
      snapshot.operations[index] = snapshot.operations[index].copyWith(
        status: SyncOperationStatus.deadLetter,
        attemptCount: attemptCount,
        clearNextRetryAt: true,
        lastError: error,
      );
    });
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
  /// Hive box.
  Future<void> dispose() async {
    if (_disposed) {
      return;
    }
    _disposed = true;
    await _pendingCountController.close();
  }

  /// Loads the snapshot from the box.
  ///
  /// Returns an empty snapshot if no snapshot exists.
  /// Throws [HiveOutboxCorruptionException] or [HiveOutboxVersionException]
  /// if the persisted data is invalid.
  Future<HiveOutboxSnapshot> _loadSnapshot() async {
    final json = _box.get(HiveOutboxSchema.snapshotKey);
    if (json == null) {
      return HiveOutboxSnapshot.empty();
    }
    return HiveOutboxSnapshot.fromJson(json);
  }

  /// Persists the snapshot to the box in a single write.
  Future<void> _persistSnapshot(HiveOutboxSnapshot snapshot) async {
    await _box.put(HiveOutboxSchema.snapshotKey, snapshot.toJson());
  }

  /// Executes [action] with the loaded snapshot, then persists the result.
  ///
  /// This is the serialized mutation boundary. All state changes must go
  /// through this method to ensure consistency.
  Future<T> _mutate<T>(
    Future<T> Function(HiveOutboxSnapshot snapshot) action,
  ) =>
      _mutex.run(() async {
        final snapshot = await _loadSnapshot();
        final result = await action(snapshot);
        await _persistSnapshot(snapshot);
        return result;
      });

  /// Counts all operations in the snapshot.
  Future<int> _queryPendingCount() async {
    final snapshot = await _loadSnapshot();
    return snapshot.operations.length;
  }

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

/// Direct [OutboxWriter] for use outside of
/// `HiveLocalStorageAdapter.runInTransaction`.
///
/// Each [enqueue] call acquires the adapter's mutex, loads the snapshot,
/// adds the operation, and persists the complete snapshot.
final class _DirectOutboxWriter implements OutboxWriter {
  _DirectOutboxWriter(this._adapter);

  final HiveLocalStorageAdapter _adapter;

  @override
  Future<SyncOperation> enqueue(LocalMutation mutation) =>
      _adapter._mutate((snapshot) async {
        final now = _adapter._clock().toUtc();
        final operationId = _adapter._operationIdGenerator();

        snapshot.lastSequenceNumber++;
        final sequenceNumber = snapshot.lastSequenceNumber;

        final operation = SyncOperation.fromMutation(
          mutation,
          operationId: operationId,
          now: now,
        ).copyWith(sequenceNumber: sequenceNumber);

        snapshot.operations.add(operation);
        return operation;
      });
}

/// A simple async mutex for serializing mutations.
///
/// Uses a future chain to ensure only one operation runs at a time.
/// Reentrant calls will deadlock — do not call `_mutate` or
/// `runInTransaction` from within a mutation callback.
final class _AsyncMutex {
  Future<void>? _tail;

  /// Runs [action] after all previously queued actions complete.
  Future<T> run<T>(Future<T> Function() action) {
    final previous = _tail ?? Future<void>.value();
    final completer = Completer<void>();
    _tail = completer.future;

    return previous.then((_) async {
      try {
        return await action();
      } finally {
        completer.complete();
      }
    });
  }
}
