import 'package:local_remote_synchronization/local_remote_synchronization.dart';
import 'package:test/test.dart';

void main() {
  group('SyncOperation', () {
    test('fromMutation uses operationId as default idempotencyKey', () {
      final mutation = LocalMutation(
        entityType: 'todos',
        entityId: 't-1',
        type: SyncOperationType.create,
        payload: <String, dynamic>{'title': 'Buy milk'},
      );
      final now = DateTime.utc(2026, 6, 1, 10);

      final op = SyncOperation.fromMutation(
        mutation,
        operationId: 'op-uuid-1',
        now: now,
        sequenceNumber: 7,
      );

      expect(op.operationId, 'op-uuid-1');
      expect(op.idempotencyKey, 'op-uuid-1');
      expect(op.entityType, 'todos');
      expect(op.entityId, 't-1');
      expect(op.type, SyncOperationType.create);
      expect(op.status, SyncOperationStatus.pending);
      expect(op.attemptCount, 0);
      expect(op.createdAt, now);
      expect(op.sequenceNumber, 7);
      expect(op.nextRetryAt, isNull);
      expect(op.lastError, isNull);
    });

    test('fromMutation preserves explicit idempotencyKey', () {
      final mutation = LocalMutation(
        entityType: 'todos',
        entityId: 't-1',
        type: SyncOperationType.update,
        payload: <String, dynamic>{'done': true},
        idempotencyKey: 'custom-idem-key',
      );

      final op = SyncOperation.fromMutation(mutation, operationId: 'op-uuid-2');

      expect(op.operationId, 'op-uuid-2');
      expect(op.idempotencyKey, 'custom-idem-key');
    });

    test('validates constructor invariants', () {
      final now = DateTime.utc(2026, 6, 1);
      expect(
        () => SyncOperation(
          operationId: '',
          idempotencyKey: 'idem',
          entityType: 'todos',
          entityId: 't-1',
          type: SyncOperationType.delete,
          createdAt: now,
        ),
        throwsArgumentError,
      );
      expect(
        () => SyncOperation(
          operationId: 'op-1',
          idempotencyKey: ' ',
          entityType: 'todos',
          entityId: 't-1',
          type: SyncOperationType.delete,
          createdAt: now,
        ),
        throwsArgumentError,
      );
      expect(
        () => SyncOperation(
          operationId: 'op-1',
          idempotencyKey: 'idem',
          entityType: 'todos',
          entityId: 't-1',
          type: SyncOperationType.delete,
          createdAt: now,
          attemptCount: -1,
        ),
        throwsArgumentError,
      );
      expect(
        () => SyncOperation(
          operationId: 'op-1',
          idempotencyKey: 'idem',
          entityType: 'todos',
          entityId: 't-1',
          type: SyncOperationType.delete,
          createdAt: now,
          sequenceNumber: -5,
        ),
        throwsArgumentError,
      );
    });

    test(
      'copyWith updates fields, preserves idempotencyKey, and clears nulls',
      () {
        final createdAt = DateTime.utc(2026, 6, 1, 10);
        final retryAt = DateTime.utc(2026, 6, 1, 10, 5);
        final error = SyncError(
          code: SyncErrorCode.timeout,
          message: 'Timed out',
          isRetryable: true,
        );

        final initial = SyncOperation(
          operationId: 'op-1',
          idempotencyKey: 'idem-stable',
          entityType: 'todos',
          entityId: 't-1',
          type: SyncOperationType.update,
          createdAt: createdAt,
          payload: <String, dynamic>{'done': true},
          sequenceNumber: 12,
        );

        final failed = initial.copyWith(
          status: SyncOperationStatus.failed,
          attemptCount: 1,
          nextRetryAt: retryAt,
          lastError: error,
        );

        expect(failed.idempotencyKey, 'idem-stable');
        expect(failed.status, SyncOperationStatus.failed);
        expect(failed.attemptCount, 1);
        expect(failed.nextRetryAt, retryAt);
        expect(failed.lastError, error);
        expect(failed.sequenceNumber, 12);

        // Explicitly clearing nullable fields must set them to null.
        final cleared = failed.copyWith(
          status: SyncOperationStatus.pending,
          clearNextRetryAt: true,
          clearLastError: true,
          clearSequenceNumber: true,
        );

        expect(cleared.status, SyncOperationStatus.pending);
        expect(cleared.nextRetryAt, isNull);
        expect(cleared.lastError, isNull);
        expect(cleared.sequenceNumber, isNull);
      },
    );

    test('JSON round-trip preserves equality and deep immutability', () {
      final op = SyncOperation(
        operationId: 'op-100',
        idempotencyKey: 'idem-100',
        entityType: 'orders',
        entityId: 'ord-9',
        type: SyncOperationType.create,
        createdAt: DateTime.utc(2026, 6, 1, 12),
        payload: <String, dynamic>{
          'items': <Object?>[
            <String, dynamic>{'id': 'sku-1', 'qty': 2},
          ],
        },
        metadata: <String, dynamic>{'source': 'mobile'},
        status: SyncOperationStatus.failed,
        attemptCount: 2,
        sequenceNumber: 42,
        nextRetryAt: DateTime.utc(2026, 6, 1, 12, 5),
        lastError: SyncError(
          code: SyncErrorCode.network,
          message: 'No route to host',
          isRetryable: true,
          cause: Exception('in-memory only'),
        ),
      );

      final json = op.toJson();
      final decoded = SyncOperation.fromJson(json);

      expect(decoded, equals(op));
      expect(decoded.hashCode, equals(op.hashCode));
      expect(decoded.lastError!.cause, isNull);
      expect(
        () => (decoded.payload!['items'] as List<Object?>).add('x'),
        throwsUnsupportedError,
      );
    });

    test('fromJson throws FormatException on invalid input', () {
      expect(
        () => SyncOperation.fromJson(<String, dynamic>{'operationId': 'op-1'}),
        throwsFormatException,
      );
      expect(
        () => SyncOperation.fromJson(<String, dynamic>{
          'operationId': 'op-1',
          'idempotencyKey': 'idem-1',
          'entityType': 'orders',
          'entityId': 'ord-1',
          'type': 'invalidType',
          'status': 'pending',
          'attemptCount': 0,
          'createdAt': '2026-06-01T00:00:00.000Z',
        }),
        throwsFormatException,
      );
      expect(
        () => SyncOperation.fromJson(<String, dynamic>{
          'operationId': 'op-1',
          'idempotencyKey': 'idem-1',
          'entityType': 'orders',
          'entityId': 'ord-1',
          'type': 'delete',
          'status': 'pending',
          'attemptCount': 0,
          'createdAt': '2026-06-01T00:00:00.000Z',
          'occurredAt': 12345,
        }),
        throwsFormatException,
      );
    });

    test('fromMutation keeps occurredAt separate from createdAt in UTC', () {
      final occurredLocal = DateTime(2026, 5, 1, 9, 15);
      final enqueuedNow = DateTime(2026, 6, 1, 14, 30);
      final mutation = LocalMutation(
        entityType: 'orders',
        entityId: 'ord-1',
        type: SyncOperationType.create,
        payload: <String, dynamic>{'total': 10},
        occurredAt: occurredLocal,
      );

      final op = SyncOperation.fromMutation(
        mutation,
        operationId: 'op-sep-1',
        now: enqueuedNow,
      );

      expect(op.createdAt, enqueuedNow.toUtc());
      expect(op.createdAt.isUtc, isTrue);
      expect(op.occurredAt, occurredLocal.toUtc());
      expect(op.occurredAt!.isUtc, isTrue);
      expect(op.createdAt, isNot(equals(op.occurredAt)));

      final opDefaultNow = SyncOperation.fromMutation(
        mutation,
        operationId: 'op-sep-2',
      );
      expect(opDefaultNow.occurredAt, occurredLocal.toUtc());
      expect(opDefaultNow.createdAt.isUtc, isTrue);
      expect(opDefaultNow.createdAt.isAfter(occurredLocal.toUtc()), isTrue);
    });

    test('JSON round-trip preserves both createdAt and occurredAt', () {
      final op = SyncOperation(
        operationId: 'op-ts-1',
        idempotencyKey: 'idem-ts-1',
        entityType: 'orders',
        entityId: 'ord-1',
        type: SyncOperationType.delete,
        createdAt: DateTime.utc(2026, 6, 1, 12),
        occurredAt: DateTime(2026, 5, 31, 23, 45),
      );

      final json = op.toJson();
      expect(json['createdAt'], '2026-06-01T12:00:00.000Z');
      expect(json['occurredAt'], op.occurredAt!.toIso8601String());

      final decoded = SyncOperation.fromJson(json);
      expect(decoded, equals(op));
      expect(decoded.hashCode, equals(op.hashCode));
      expect(decoded.occurredAt, op.occurredAt);
    });

    test('fromJson remains backward compatible when occurredAt is omitted', () {
      final decoded = SyncOperation.fromJson(<String, dynamic>{
        'operationId': 'op-legacy',
        'idempotencyKey': 'idem-legacy',
        'entityType': 'orders',
        'entityId': 'ord-1',
        'type': 'delete',
        'status': 'pending',
        'attemptCount': 0,
        'createdAt': '2026-06-01T12:00:00.000Z',
      });

      expect(decoded.occurredAt, isNull);
      expect(decoded.createdAt, DateTime.utc(2026, 6, 1, 12));
    });
  });
}
