import 'package:local_remote_drift/src/drift_outbox_mapper.dart';
import 'package:local_remote_synchronization/local_remote_synchronization.dart';
import 'package:test/test.dart';

void main() {
  group('DriftOutboxMapper', () {
    test('restores all fields including separate createdAt and occurredAt', () {
      final createdAtMs =
          DateTime.utc(2026, 6, 1, 14, 30).millisecondsSinceEpoch;
      final occurredAtMs =
          DateTime.utc(2026, 5, 1, 9, 15).millisecondsSinceEpoch;
      final nextRetryAtMs = DateTime.utc(2026, 6, 1, 15).millisecondsSinceEpoch;

      final op = DriftOutboxMapper.fromRow(<String, Object?>{
        'sequence_number': 7,
        'operation_id': 'op-1',
        'idempotency_key': 'idem-1',
        'entity_type': 'tasks',
        'entity_id': 't-1',
        'operation_type': 'update',
        'payload_json': '{"title":"Ship","nested":{"tags":["a",1,true,null]}}',
        'metadata_json': '{"baseVersion":3}',
        'status': 'failed',
        'attempt_count': 2,
        'created_at_ms': createdAtMs,
        'occurred_at_ms': occurredAtMs,
        'updated_at_ms': createdAtMs,
        'next_retry_at_ms': nextRetryAtMs,
        'last_error_json':
            '{"code":"network","message":"Offline","isRetryable":true}',
      });

      expect(op.sequenceNumber, 7);
      expect(op.operationId, 'op-1');
      expect(op.idempotencyKey, 'idem-1');
      expect(op.entityType, 'tasks');
      expect(op.entityId, 't-1');
      expect(op.type, SyncOperationType.update);
      expect(op.status, SyncOperationStatus.failed);
      expect(op.attemptCount, 2);
      expect(op.createdAt, DateTime.utc(2026, 6, 1, 14, 30));
      expect(op.createdAt.isUtc, isTrue);
      expect(op.occurredAt, DateTime.utc(2026, 5, 1, 9, 15));
      expect(op.occurredAt!.isUtc, isTrue);
      expect(op.nextRetryAt, DateTime.utc(2026, 6, 1, 15));
      expect(op.nextRetryAt!.isUtc, isTrue);
      expect(op.lastError!.code, SyncErrorCode.network);
      expect(
        () => (op.payload!['nested']! as Map<String, dynamic>)['x'] = 1,
        throwsUnsupportedError,
      );
    });

    test('throws descriptive FormatException on corrupted rows', () {
      expect(
        () => DriftOutboxMapper.fromRow(const <String, Object?>{
          'operation_id': 'bad-op',
          'idempotency_key': 'bad-op',
          'entity_type': 'tasks',
          'entity_id': 't-1',
          'operation_type': 'create',
          'payload_json': '{not json',
          'metadata_json': '{}',
          'status': 'pending',
          'attempt_count': 0,
          'created_at_ms': 1000,
        }),
        throwsA(
          isA<FormatException>().having(
            (e) => e.message,
            'message',
            contains('bad-op'),
          ),
        ),
      );

      expect(
        () => DriftOutboxMapper.fromRow(const <String, Object?>{
          'operation_id': 'bad-status',
          'idempotency_key': 'bad-status',
          'entity_type': 'tasks',
          'entity_id': 't-1',
          'operation_type': 'delete',
          'payload_json': null,
          'metadata_json': '{}',
          'status': 'unknownStatus',
          'attempt_count': 0,
          'created_at_ms': 1000,
        }),
        throwsFormatException,
      );
    });
  });
}
