import 'package:local_remote_synchronization/local_remote_synchronization.dart';
import 'package:test/test.dart';

void main() {
  group('SyncResult', () {
    test('validates non-empty operationId', () {
      expect(() => SyncSuccess(operationId: '  '), throwsArgumentError);
    });

    test('SyncSuccess enforces deep immutability and equality', () {
      final raw = <String, dynamic>{
        'server': <String, dynamic>{
          'tags': <String>['a'],
        },
      };
      final ts = DateTime(2026, 6, 1, 14);
      final a = SyncSuccess(
        operationId: 'op-1',
        remoteVersion: 'v2',
        serverTimestamp: ts,
        syncedData: raw,
      );
      final b = SyncSuccess(
        operationId: 'op-1',
        remoteVersion: 'v2',
        serverTimestamp: ts,
        syncedData: <String, dynamic>{
          'server': <String, dynamic>{
            'tags': <String>['a'],
          },
        },
      );

      raw['mutated'] = true;
      expect(a.syncedData!.containsKey('mutated'), isFalse);
      expect(a.serverTimestamp!.isUtc, isTrue);
      expect(a, equals(b));
      expect(a.hashCode, equals(b.hashCode));
      expect(
        () => (a.syncedData!['server'] as Map<String, dynamic>)['x'] = 1,
        throwsUnsupportedError,
      );
    });

    test('SyncConflict enforces deep immutability and equality', () {
      final snapshot = <String, dynamic>{
        'title': 'Server Title',
        'labels': <String>['s1'],
      };
      final conflict = SyncConflict(
        operationId: 'op-2',
        remoteSnapshot: snapshot,
        remoteVersion: 'etag-9',
      );
      snapshot['title'] = 'Mutated';

      expect(conflict.remoteSnapshot['title'], 'Server Title');
      expect(
        () => (conflict.remoteSnapshot['labels'] as List<Object?>).add('s2'),
        throwsUnsupportedError,
      );

      final other = SyncConflict(
        operationId: 'op-2',
        remoteSnapshot: <String, dynamic>{
          'title': 'Server Title',
          'labels': <String>['s1'],
        },
        remoteVersion: 'etag-9',
      );
      expect(conflict, equals(other));
      expect(conflict.hashCode, equals(other.hashCode));
    });

    test('SyncFailure equality and exhaustive sealed switching', () {
      final failure = SyncFailure(
        operationId: 'op-3',
        error: SyncError(
          code: SyncErrorCode.validation,
          message: 'Invalid field',
          isRetryable: false,
          statusCode: 400,
        ),
      );

      final label = switch (failure as SyncResult) {
        SyncSuccess() => 'success',
        SyncConflict() => 'conflict',
        SyncFailure(:final error) => 'failure:${error.code.name}',
      };
      expect(label, 'failure:validation');
    });
  });
}
