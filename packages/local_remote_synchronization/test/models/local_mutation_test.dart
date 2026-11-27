import 'package:local_remote_synchronization/local_remote_synchronization.dart';
import 'package:test/test.dart';

void main() {
  group('LocalMutation', () {
    test(
      'constructs valid create/update/delete mutations and normalizes UTC',
      () {
        final localTime = DateTime(2026, 5, 1, 12, 30);
        final createMutation = LocalMutation(
          entityType: 'orders',
          entityId: 'ord-1',
          type: SyncOperationType.create,
          payload: <String, dynamic>{'total': 42.5},
          idempotencyKey: 'idem-1',
          occurredAt: localTime,
          metadata: const <String, dynamic>{'baseVersion': 1},
        );

        expect(createMutation.entityType, 'orders');
        expect(createMutation.entityId, 'ord-1');
        expect(createMutation.type, SyncOperationType.create);
        expect(createMutation.payload, <String, dynamic>{'total': 42.5});
        expect(createMutation.idempotencyKey, 'idem-1');
        expect(createMutation.occurredAt, localTime.toUtc());
        expect(createMutation.occurredAt!.isUtc, isTrue);
        expect(createMutation.metadata, const <String, dynamic>{
          'baseVersion': 1,
        });

        final deleteMutation = LocalMutation(
          entityType: 'orders',
          entityId: 'ord-1',
          type: SyncOperationType.delete,
        );
        expect(deleteMutation.payload, isNull);
      },
    );

    test('validates required and non-empty fields', () {
      expect(
        () => LocalMutation(
          entityType: '  ',
          entityId: 'ord-1',
          type: SyncOperationType.delete,
        ),
        throwsArgumentError,
      );
      expect(
        () => LocalMutation(
          entityType: 'orders',
          entityId: '',
          type: SyncOperationType.delete,
        ),
        throwsArgumentError,
      );
      expect(
        () => LocalMutation(
          entityType: 'orders',
          entityId: 'ord-1',
          type: SyncOperationType.create,
        ),
        throwsArgumentError,
      );
      expect(
        () => LocalMutation(
          entityType: 'orders',
          entityId: 'ord-1',
          type: SyncOperationType.update,
        ),
        throwsArgumentError,
      );
      expect(
        () => LocalMutation(
          entityType: 'orders',
          entityId: 'ord-1',
          type: SyncOperationType.delete,
          idempotencyKey: '   ',
        ),
        throwsArgumentError,
      );
    });

    test('enforces deep immutability on payload and metadata', () {
      final nestedList = <dynamic>[
        1,
        <String, dynamic>{'sku': 'A'},
      ];
      final nestedMap = <String, dynamic>{'city': 'Cairo'};
      final rawPayload = <String, dynamic>{
        'address': nestedMap,
        'items': nestedList,
      };
      final rawMetadata = <String, dynamic>{
        'tags': <String>['urgent'],
      };

      final mutation = LocalMutation(
        entityType: 'orders',
        entityId: 'ord-1',
        type: SyncOperationType.create,
        payload: rawPayload,
        metadata: rawMetadata,
      );

      // Mutating original inputs must not affect the model.
      rawPayload['extra'] = true;
      nestedMap['city'] = 'Alexandria';
      nestedList.add(99);
      (rawMetadata['tags'] as List<String>).add('vip');

      expect(mutation.payload!.containsKey('extra'), isFalse);
      expect(
        (mutation.payload!['address'] as Map<String, dynamic>)['city'],
        'Cairo',
      );
      expect((mutation.payload!['items'] as List<Object?>).length, 2);
      expect(mutation.metadata['tags'], <String>['urgent']);

      // Direct mutation attempts on stored collections must throw.
      expect(() => mutation.payload!['x'] = 1, throwsUnsupportedError);
      expect(
        () => (mutation.payload!['address'] as Map<String, dynamic>)['city'] =
            'Giza',
        throwsUnsupportedError,
      );
      expect(
        () => (mutation.payload!['items'] as List<Object?>).add(2),
        throwsUnsupportedError,
      );
      expect(
        () =>
            ((mutation.payload!['items'] as List<Object?>)[1]
                    as Map<String, dynamic>)['sku'] =
                'B',
        throwsUnsupportedError,
      );
      expect(
        () => (mutation.metadata['tags'] as List<Object?>).add('other'),
        throwsUnsupportedError,
      );
    });

    test('rejects unsupported JSON value types', () {
      expect(
        () => LocalMutation(
          entityType: 'orders',
          entityId: 'ord-1',
          type: SyncOperationType.create,
          payload: <String, dynamic>{'createdAt': DateTime.now()},
        ),
        throwsArgumentError,
      );
      expect(
        () => LocalMutation(
          entityType: 'orders',
          entityId: 'ord-1',
          type: SyncOperationType.create,
          payload: <String, dynamic>{
            'nested': <Object?>[Object()],
          },
        ),
        throwsArgumentError,
      );
      expect(
        () => LocalMutation(
          entityType: 'orders',
          entityId: 'ord-1',
          type: SyncOperationType.create,
          payload: <String, dynamic>{
            'badMap': <int, String>{1: 'one'},
          },
        ),
        throwsArgumentError,
      );
    });

    test('supports deep equality and consistent hashCode', () {
      final time = DateTime.utc(2026, 1, 1);
      final a = LocalMutation(
        entityType: 'orders',
        entityId: 'ord-1',
        type: SyncOperationType.update,
        payload: <String, dynamic>{
          'nested': <String, dynamic>{
            'list': <int>[1, 2, 3],
          },
        },
        occurredAt: time,
        metadata: <String, dynamic>{'v': 2},
      );
      final b = LocalMutation(
        entityType: 'orders',
        entityId: 'ord-1',
        type: SyncOperationType.update,
        payload: <String, dynamic>{
          'nested': <String, dynamic>{
            'list': <int>[1, 2, 3],
          },
        },
        occurredAt: time,
        metadata: <String, dynamic>{'v': 2},
      );
      final c = LocalMutation(
        entityType: 'orders',
        entityId: 'ord-1',
        type: SyncOperationType.update,
        payload: <String, dynamic>{
          'nested': <String, dynamic>{
            'list': <int>[1, 2, 4],
          },
        },
        occurredAt: time,
        metadata: <String, dynamic>{'v': 2},
      );

      expect(a, equals(b));
      expect(a.hashCode, equals(b.hashCode));
      expect(a, isNot(equals(c)));
    });

    test(
      'rejects non-finite doubles in payload, metadata, and nested values',
      () {
        for (final invalid in <double>[
          double.nan,
          double.infinity,
          double.negativeInfinity,
        ]) {
          expect(
            () => LocalMutation(
              entityType: 'orders',
              entityId: 'ord-1',
              type: SyncOperationType.create,
              payload: <String, dynamic>{'total': invalid},
            ),
            throwsArgumentError,
          );
          expect(
            () => LocalMutation(
              entityType: 'orders',
              entityId: 'ord-1',
              type: SyncOperationType.create,
              payload: <String, dynamic>{
                'nested': <String, dynamic>{
                  'list': <Object?>[1, invalid],
                },
              },
            ),
            throwsArgumentError,
          );
          expect(
            () => LocalMutation(
              entityType: 'orders',
              entityId: 'ord-1',
              type: SyncOperationType.delete,
              metadata: <String, dynamic>{'score': invalid},
            ),
            throwsArgumentError,
          );
        }

        final valid = LocalMutation(
          entityType: 'orders',
          entityId: 'ord-1',
          type: SyncOperationType.create,
          payload: <String, dynamic>{'intVal': 10, 'doubleVal': -42.5},
        );
        expect(valid.payload!['intVal'], 10);
        expect(valid.payload!['doubleVal'], -42.5);
      },
    );
  });
}
