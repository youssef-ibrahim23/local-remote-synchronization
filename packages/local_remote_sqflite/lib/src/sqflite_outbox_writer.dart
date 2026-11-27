import 'package:local_remote_synchronization/local_remote_synchronization.dart';
import 'package:sqflite/sqflite.dart';

import 'sqflite_outbox_mapper.dart';

/// Transaction-scoped [OutboxWriter] backed by a Sqflite [DatabaseExecutor].
final class SqfliteOutboxWriter implements OutboxWriter {
  /// Creates a [SqfliteOutboxWriter] bound to [executor] and [tableName].
  SqfliteOutboxWriter({
    required DatabaseExecutor executor,
    required String tableName,
    required String Function() operationIdGenerator,
    required DateTime Function() clock,
  })  : _executor = executor,
        _tableName = tableName,
        _operationIdGenerator = operationIdGenerator,
        _clock = clock;

  final DatabaseExecutor _executor;
  final String _tableName;
  final String Function() _operationIdGenerator;
  final DateTime Function() _clock;

  @override
  Future<SyncOperation> enqueue(LocalMutation mutation) async {
    final now = _clock().toUtc();
    final operationId = _operationIdGenerator();
    final initial = SyncOperation.fromMutation(
      mutation,
      operationId: operationId,
      now: now,
    );

    final row = SqfliteOutboxMapper.toInsertRow(
      initial,
      occurredAt: mutation.occurredAt,
      updatedAt: now,
    );

    final sequenceNumber = await _executor.insert(
      _tableName,
      row,
      conflictAlgorithm: ConflictAlgorithm.abort,
    );

    return initial.copyWith(sequenceNumber: sequenceNumber);
  }
}
