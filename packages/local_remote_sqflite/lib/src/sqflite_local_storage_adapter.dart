import 'dart:async';
import 'dart:convert';

import 'package:local_remote_synchronization/local_remote_synchronization.dart';
import 'package:sqflite/sqflite.dart';
import 'package:uuid/uuid.dart';

import 'sqflite_outbox_mapper.dart';
import 'sqflite_outbox_schema.dart';
import 'sqflite_outbox_writer.dart';

/// Sqflite implementation of [LocalStorageAdapter] for the durable outbox.
///
/// The application owns its domain tables and database lifecycle, while this
/// adapter owns only its outbox table (defaulting to `sync_outbox`).
///
/// To guarantee atomicity between application domain writes and outbox
/// enqueues, use [runWithTransaction] (or [outboxFor] inside an existing
/// Sqflite `Transaction`).
final class SqfliteLocalStorageAdapter implements LocalStorageAdapter {
  /// Creates a [SqfliteLocalStorageAdapter] using an existing Sqflite
  /// [database].
  SqfliteLocalStorageAdapter({
    required Database database,
    String tableName = SqfliteOutboxSchema.defaultTableName,
    String Function()? operationIdGenerator,
    DateTime Function()? clock,
  })  : _database = database,
        _tableName = SqfliteOutboxSchema.validateTableName(tableName),
        _operationIdGenerator = operationIdGenerator ?? _defaultUuidV4,
        _clock = clock ?? _defaultUtcNow;

  static const Uuid _uuid = Uuid();

  static String _defaultUuidV4() => _uuid.v4();
  static DateTime _defaultUtcNow() => DateTime.now().toUtc();

  final Database _database;
  final String _tableName;
  final String Function() _operationIdGenerator;
  final DateTime Function() _clock;
  final StreamController<int> _pendingCountController =
      StreamController<int>.broadcast();
  bool _disposed = false;

  /// Creates the outbox table and indexes if they do not already exist.
  ///
  /// Safe to call repeatedly.
  Future<void> ensureInitialized() =>
      SqfliteOutboxSchema.createSchema(_database, tableName: _tableName);

  /// Executes [action] inside a native Sqflite [Transaction], providing both
  /// the native [Transaction] handle and an [OutboxWriter] bound to that exact
  /// same transaction.
  ///
  /// This API is specific to the Sqflite adapter so application domain writes
  /// and outbox enqueues commit or roll back atomically without exposing
  /// Sqflite types in Core.
  Future<T> runWithTransaction<T>(
    Future<T> Function(Transaction txn, OutboxWriter outbox) action,
  ) async {
    final result = await _database.transaction(
      (txn) => action(txn, outboxFor(txn)),
    );
    await _emitPendingCount();
    return result;
  }

  /// Returns an [OutboxWriter] bound to an existing Sqflite [txn].
  OutboxWriter outboxFor(Transaction txn) => SqfliteOutboxWriter(
        executor: txn,
        tableName: _tableName,
        operationIdGenerator: _operationIdGenerator,
        clock: _clock,
      );

  @override
  Future<T> runInTransaction<T>(
    Future<T> Function(OutboxWriter outbox) action,
  ) =>
      runWithTransaction((_, outbox) => action(outbox));

  @override
  Future<int> recoverInFlightOperations() async {
    final nowMs = _clock().toUtc().millisecondsSinceEpoch;
    final recovered = await _database.rawUpdate(
      '''
      UPDATE $_tableName
      SET status = ?, updated_at_ms = ?
      WHERE status = ?
      ''',
      <Object?>[
        SyncOperationStatus.pending.name,
        nowMs,
        SyncOperationStatus.inFlight.name,
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

    final rows = await _database.rawQuery(
      '''
      SELECT o.*
      FROM $_tableName o
      WHERE (
        o.status = ?
        OR (
          o.status = ?
          AND (o.next_retry_at_ms IS NULL OR o.next_retry_at_ms <= ?)
        )
      )
      $perEntityFilter
      ORDER BY o.created_at_ms ASC, o.sequence_number ASC
      LIMIT ?
      ''',
      <Object?>[
        SyncOperationStatus.pending.name,
        SyncOperationStatus.failed.name,
        nowMs,
        limit,
      ],
    );

    return rows.map(SqfliteOutboxMapper.fromRow).toList(growable: false);
  }

  @override
  Future<void> markInFlight(String operationId) async {
    final nowMs = _clock().toUtc().millisecondsSinceEpoch;
    await _database.rawUpdate(
      '''
      UPDATE $_tableName
      SET status = ?, updated_at_ms = ?
      WHERE operation_id = ?
      ''',
      <Object?>[
        SyncOperationStatus.inFlight.name,
        nowMs,
        operationId,
      ],
    );
    await _emitPendingCount();
  }

  @override
  Future<void> markSucceeded(String operationId) async {
    await _database.delete(
      _tableName,
      where: 'operation_id = ?',
      whereArgs: <Object?>[operationId],
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
    await _database.rawUpdate(
      '''
      UPDATE $_tableName
      SET status = ?,
          attempt_count = ?,
          next_retry_at_ms = ?,
          last_error_json = ?,
          updated_at_ms = ?
      WHERE operation_id = ?
      ''',
      <Object?>[
        SyncOperationStatus.failed.name,
        attemptCount,
        nextRetryAt.toUtc().millisecondsSinceEpoch,
        jsonEncode(error.toJson()),
        nowMs,
        operationId,
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
    await _database.rawUpdate(
      '''
      UPDATE $_tableName
      SET status = ?,
          attempt_count = ?,
          next_retry_at_ms = NULL,
          last_error_json = ?,
          updated_at_ms = ?
      WHERE operation_id = ?
      ''',
      <Object?>[
        SyncOperationStatus.deadLetter.name,
        attemptCount,
        jsonEncode(error.toJson()),
        nowMs,
        operationId,
      ],
    );
    await _emitPendingCount();
  }

  @override
  Stream<int> watchPendingCount() {
    late final StreamController<int> controller;
    StreamSubscription<int>? subscription;

    controller = StreamController<int>(
      onListen: () async {
        try {
          final initialCount = await _queryPendingCount();
          if (!controller.isClosed) {
            controller.add(initialCount);
          }
        } catch (error, stackTrace) {
          if (!controller.isClosed) {
            controller.addError(error, stackTrace);
          }
        }
        if (!_disposed && !controller.isClosed) {
          subscription = _pendingCountController.stream.listen(
            controller.add,
            onError: controller.addError,
          );
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
  /// Sqflite [Database].
  Future<void> dispose() async {
    if (_disposed) {
      return;
    }
    _disposed = true;
    await _pendingCountController.close();
  }

  Future<int> _queryPendingCount() async {
    final rows = await _database.rawQuery(
      'SELECT COUNT(*) AS count FROM $_tableName',
    );
    return Sqflite.firstIntValue(rows) ?? 0;
  }

  Future<void> _emitPendingCount() async {
    if (_disposed || !_pendingCountController.hasListener) {
      return;
    }
    try {
      final count = await _queryPendingCount();
      if (!_disposed && !_pendingCountController.isClosed) {
        _pendingCountController.add(count);
      }
    } on DatabaseException {
      // Ignore emissions if the caller already closed the database.
    }
  }
}
