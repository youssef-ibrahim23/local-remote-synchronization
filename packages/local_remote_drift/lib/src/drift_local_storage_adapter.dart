import 'dart:async';
import 'dart:convert';

import 'package:drift/drift.dart';
import 'package:local_remote_synchronization/local_remote_synchronization.dart';
import 'package:uuid/uuid.dart';

import 'drift_outbox_mapper.dart';
import 'drift_outbox_schema.dart';
import 'drift_outbox_writer.dart';

/// Drift implementation of [LocalStorageAdapter] for the durable outbox.
///
/// The application owns its Drift [GeneratedDatabase], domain tables, and
/// database lifecycle, while this adapter owns only its outbox table
/// (defaulting to `sync_outbox`).
///
/// Drift uses Zone-based transactions (`database.transaction(...)`), so any
/// domain writes on [database] and calls to [OutboxWriter.enqueue] inside
/// [runWithTransaction] or [runInTransaction] automatically participate in the
/// exact same native Drift transaction.
final class DriftLocalStorageAdapter implements LocalStorageAdapter {
  /// Creates a [DriftLocalStorageAdapter] using an existing Drift
  /// [database].
  DriftLocalStorageAdapter({
    required GeneratedDatabase database,
    String tableName = DriftOutboxSchema.defaultTableName,
    String Function()? operationIdGenerator,
    DateTime Function()? clock,
  })  : _database = database,
        _tableName = DriftOutboxSchema.validateTableName(tableName),
        _operationIdGenerator = operationIdGenerator ?? _defaultUuidV4,
        _clock = clock ?? _defaultUtcNow;

  static const Uuid _uuid = Uuid();

  static String _defaultUuidV4() => _uuid.v4();
  static DateTime _defaultUtcNow() => DateTime.now().toUtc();

  final GeneratedDatabase _database;
  final String _tableName;
  final String Function() _operationIdGenerator;
  final DateTime Function() _clock;
  final StreamController<int> _pendingCountController =
      StreamController<int>.broadcast();
  int _pendingCountVersion = 0;
  bool _disposed = false;

  late final OutboxWriter _outboxWriter = DriftOutboxWriter(
    database: _database,
    tableName: _tableName,
    operationIdGenerator: _operationIdGenerator,
    clock: _clock,
  );

  /// Zone-aware [OutboxWriter] for use inside an already-open
  /// `database.transaction(() async { ... })` block without creating a nested
  /// transaction.
  ///
  /// Because an external transaction has not yet committed when
  /// [OutboxWriter.enqueue] runs, call [refreshPendingCount] after your
  /// external `database.transaction(...)` commits to notify [watchPendingCount]
  /// listeners.
  OutboxWriter get outbox => _outboxWriter;

  /// Creates the outbox table and indexes if they do not already exist.
  ///
  /// Safe to call repeatedly.
  Future<void> ensureInitialized() =>
      DriftOutboxSchema.createSchema(_database, tableName: _tableName);

  /// Executes [action] inside Drift's Zone-based transaction and refreshes
  /// [watchPendingCount] listeners after the transaction commits.
  ///
  /// Any Drift DAO or table writes executed on the same [GeneratedDatabase]
  /// inside [action] automatically share the same transaction as [outbox].
  Future<T> runWithTransaction<T>(
    Future<T> Function(OutboxWriter outbox) action,
  ) =>
      runInTransaction(action);

  /// Queries the current outbox count and notifies active [watchPendingCount]
  /// listeners.
  ///
  /// Call this after committing an external `database.transaction(...)` that
  /// used [outbox]. Never call this while an external transaction is still open
  /// if you want to avoid querying uncommitted state.
  Future<void> refreshPendingCount() => _emitPendingCount();

  @override
  Future<T> runInTransaction<T>(
    Future<T> Function(OutboxWriter outbox) action,
  ) async {
    final result = await _database.transaction(() => action(_outboxWriter));
    await _emitPendingCount();
    return result;
  }

  @override
  Future<int> recoverInFlightOperations() async {
    final nowMs = _clock().toUtc().millisecondsSinceEpoch;
    final recovered = await _database.customUpdate(
      '''
      UPDATE $_tableName
      SET status = ?, updated_at_ms = ?
      WHERE status = ?
      ''',
      variables: <Variable<Object>>[
        Variable.withString(SyncOperationStatus.pending.name),
        Variable.withInt(nowMs),
        Variable.withString(SyncOperationStatus.inFlight.name),
      ],
    );
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
    final perEntityFilter = preservePerEntityOrder
        ? '''
        AND NOT EXISTS (
          SELECT 1
          FROM $_tableName prev
          WHERE prev.entity_type = o.entity_type
            AND prev.entity_id = o.entity_id
            AND prev.sequence_number < o.sequence_number
            AND prev.status != 'succeeded'
        )
        '''
        : '';

    final rows = await _database.customSelect(
      '''
      SELECT o.*
      FROM $_tableName o
      WHERE (
        o.status = ?
        OR (
          o.status = ?
          AND o.next_retry_at_ms IS NOT NULL
          AND o.next_retry_at_ms <= ?
        )
      )
      $perEntityFilter
      ORDER BY o.created_at_ms ASC, o.sequence_number ASC
      LIMIT ?
      ''',
      variables: <Variable<Object>>[
        Variable.withString(SyncOperationStatus.pending.name),
        Variable.withString(SyncOperationStatus.failed.name),
        Variable.withInt(nowMs),
        Variable.withInt(limit),
      ],
    ).get();

    return rows
        .map((row) => DriftOutboxMapper.fromRow(row.data))
        .toList(growable: false);
  }

  @override
  Future<void> markInFlight(String operationId) async {
    final nowMs = _clock().toUtc().millisecondsSinceEpoch;
    await _database.customUpdate(
      '''
      UPDATE $_tableName
      SET status = ?, updated_at_ms = ?
      WHERE operation_id = ?
      ''',
      variables: <Variable<Object>>[
        Variable.withString(SyncOperationStatus.inFlight.name),
        Variable.withInt(nowMs),
        Variable.withString(operationId),
      ],
    );
    await _emitPendingCount();
  }

  @override
  Future<void> markSucceeded(String operationId) async {
    await _database.customUpdate(
      'DELETE FROM $_tableName WHERE operation_id = ?',
      variables: <Variable<Object>>[Variable.withString(operationId)],
    );
    await _emitPendingCount();
  }

  @override
  Future<void> markFailed(
    String operationId, {
    required int attemptCount,
    required DateTime nextRetryAt,
    required SyncError error,
  }) async {
    final nowMs = _clock().toUtc().millisecondsSinceEpoch;
    await _database.customUpdate(
      '''
      UPDATE $_tableName
      SET status = ?,
          attempt_count = ?,
          next_retry_at_ms = ?,
          last_error_json = ?,
          updated_at_ms = ?
      WHERE operation_id = ?
      ''',
      variables: <Variable<Object>>[
        Variable.withString(SyncOperationStatus.failed.name),
        Variable.withInt(attemptCount),
        Variable.withInt(nextRetryAt.toUtc().millisecondsSinceEpoch),
        Variable.withString(jsonEncode(error.toJson())),
        Variable.withInt(nowMs),
        Variable.withString(operationId),
      ],
    );
    await _emitPendingCount();
  }

  @override
  Future<void> markDeadLetter(
    String operationId, {
    required int attemptCount,
    required SyncError error,
  }) async {
    final nowMs = _clock().toUtc().millisecondsSinceEpoch;
    await _database.customUpdate(
      '''
      UPDATE $_tableName
      SET status = ?,
          attempt_count = ?,
          next_retry_at_ms = NULL,
          last_error_json = ?,
          updated_at_ms = ?
      WHERE operation_id = ?
      ''',
      variables: <Variable<Object>>[
        Variable.withString(SyncOperationStatus.deadLetter.name),
        Variable.withInt(attemptCount),
        Variable.withString(jsonEncode(error.toJson())),
        Variable.withInt(nowMs),
        Variable.withString(operationId),
      ],
    );
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
  /// Drift [GeneratedDatabase].
  Future<void> dispose() async {
    if (_disposed) {
      return;
    }
    _disposed = true;
    await _pendingCountController.close();
  }

  Future<int> _queryPendingCount() async {
    final row = await _database
        .customSelect('SELECT COUNT(*) AS count FROM $_tableName')
        .getSingle();
    return row.read<int>('count');
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
      // Ignore emissions if the caller already closed the database.
    }
  }
}
