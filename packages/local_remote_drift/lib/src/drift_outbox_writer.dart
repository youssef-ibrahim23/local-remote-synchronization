import 'package:drift/drift.dart';
import 'package:local_remote_synchronization/local_remote_synchronization.dart';

import 'drift_outbox_mapper.dart';

/// Zone-aware [OutboxWriter] backed by a Drift [GeneratedDatabase].
///
/// When [enqueue] is called inside a Drift
/// `database.transaction(() async { ... })` Zone, Drift automatically binds the
/// insert to that active transaction.
final class DriftOutboxWriter implements OutboxWriter {
  /// Creates a [DriftOutboxWriter] bound to [database] and [tableName].
  DriftOutboxWriter({
    required GeneratedDatabase database,
    required String tableName,
    required String Function() operationIdGenerator,
    required DateTime Function() clock,
  })  : _database = database,
        _tableName = tableName,
        _operationIdGenerator = operationIdGenerator,
        _clock = clock;

  final GeneratedDatabase _database;
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

    final variables = DriftOutboxMapper.toInsertVariables(
      initial,
      updatedAt: now,
    );

    final sequenceNumber = await _database.customInsert(
      '''
      INSERT INTO $_tableName (
        operation_id,
        idempotency_key,
        entity_type,
        entity_id,
        operation_type,
        payload_json,
        metadata_json,
        status,
        attempt_count,
        created_at_ms,
        occurred_at_ms,
        updated_at_ms,
        next_retry_at_ms,
        last_error_json
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      ''',
      variables: variables,
    );

    return initial.copyWith(sequenceNumber: sequenceNumber);
  }
}
