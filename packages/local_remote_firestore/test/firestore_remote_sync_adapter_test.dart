import 'package:cloud_firestore/cloud_firestore.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:local_remote_firestore/local_remote_firestore.dart';
import 'package:local_remote_synchronization/local_remote_synchronization.dart';

import 'support/firestore_test_harness.dart';

void main() {
  late FirestoreTestHarness harness;

  setUp(() {
    harness = FirestoreTestHarness.create();
  });

  DocumentReference<Map<String, dynamic>> noteDoc([String id = 'entity-1']) =>
      harness.firestore.collection('notes').doc(id);

  Future<Map<String, dynamic>?> noteData([String id = 'entity-1']) async =>
      (await noteDoc(id).get()).data();

  group('configuration', () {
    test('rejects an empty entity mapping', () {
      expect(
        () => FirestoreRemoteSyncAdapter(
          firestore: harness.firestore,
          entityMapping: const {},
        ),
        throwsArgumentError,
      );
    });

    test('rejects a blank entityType key', () {
      expect(
        () => FirestoreRemoteSyncAdapter(
          firestore: harness.firestore,
          entityMapping: {
            '  ': FirestoreEntityConfig(collectionPath: 'notes'),
          },
        ),
        throwsArgumentError,
      );
    });

    test('rejects invalid idempotencyCollectionPath', () {
      expect(
        () => FirestoreRemoteSyncAdapter(
          firestore: harness.firestore,
          entityMapping: {
            'note': FirestoreEntityConfig(collectionPath: 'notes'),
          },
          idempotencyCollectionPath: 'a/b',
        ),
        throwsArgumentError,
      );
    });

    test('rejects blank expectedVersionKey', () {
      expect(
        () => FirestoreRemoteSyncAdapter(
          firestore: harness.firestore,
          entityMapping: {
            'note': FirestoreEntityConfig(collectionPath: 'notes'),
          },
          expectedVersionKey: '  ',
        ),
        throwsArgumentError,
      );
    });

    test('mapping is snapshotted at construction', () async {
      final mapping = <String, FirestoreEntityConfig>{
        'note': FirestoreEntityConfig(collectionPath: 'notes'),
      };
      final adapter = FirestoreRemoteSyncAdapter(
        firestore: harness.firestore,
        entityMapping: mapping,
      );
      mapping['todo'] = FirestoreEntityConfig(collectionPath: 'todos');
      final result = await adapter.push(
        makeOperation(entityType: 'todo', payload: {}),
      );
      expect(result, isA<SyncFailure>());
      expect((result as SyncFailure).error.code, SyncErrorCode.validation);
    });

    test('unknown entityType returns non-retryable validation failure',
        () async {
      final result = await harness.adapter.push(
        makeOperation(entityType: 'unknown', payload: {'title': 'x'}),
      );
      expect(result, isA<SyncFailure>());
      final failure = result as SyncFailure;
      expect(failure.error.code, SyncErrorCode.validation);
      expect(failure.error.isRetryable, isFalse);
      expect(failure.operationId, 'op-1');
    });
  });

  group('FirestoreEntityConfig validation', () {
    test('rejects blank collectionPath', () {
      expect(
        () => FirestoreEntityConfig(collectionPath: '  '),
        throwsArgumentError,
      );
    });

    test('rejects even-length collection paths', () {
      expect(
        () => FirestoreEntityConfig(collectionPath: 'users/user_1'),
        throwsArgumentError,
      );
    });

    test('rejects empty and dot segments', () {
      expect(
        () => FirestoreEntityConfig(collectionPath: 'a//b'),
        throwsArgumentError,
      );
      expect(
        () => FirestoreEntityConfig(collectionPath: 'a/./b'),
        throwsArgumentError,
      );
      expect(
        () => FirestoreEntityConfig(collectionPath: 'a/../b'),
        throwsArgumentError,
      );
    });

    test('accepts nested collection paths and normalizes slashes', () {
      final config = FirestoreEntityConfig(collectionPath: '/users/u1/todos/');
      expect(config.collectionPath, 'users/u1/todos');
    });

    test('rejects blank versionField', () {
      expect(
        () => FirestoreEntityConfig(
          collectionPath: 'notes',
          versionField: ' ',
        ),
        throwsArgumentError,
      );
    });
  });

  group('create', () {
    test('creates the document with entityId, payload, and version 1',
        () async {
      final result = await harness.adapter.push(
        makeOperation(payload: {'title': 'Hello'}),
      );

      expect(result, isA<SyncSuccess>());
      final success = result as SyncSuccess;
      expect(success.operationId, 'op-1');
      expect(success.remoteVersion, '1');
      expect(success.syncedData?['title'], 'Hello');

      final data = await noteData();
      expect(data?['title'], 'Hello');
      expect(data?['sync_version'], 1);
    });

    test('does not overwrite an existing document', () async {
      await noteDoc().set({'title': 'Existing', 'sync_version': 5});

      final result = await harness.adapter.push(
        makeOperation(
          idempotencyKey: 'create-key',
          payload: {'title': 'New'},
        ),
      );

      expect(result, isA<SyncConflict>());
      final conflict = result as SyncConflict;
      expect(conflict.remoteSnapshot['title'], 'Existing');
      expect(conflict.remoteVersion, '5');
      expect((await noteData())?['title'], 'Existing');
    });

    test('unversioned config does not inject a version field', () async {
      final result = await harness.adapter.push(
        makeOperation(
          entityType: 'todo',
          entityId: 't-1',
          payload: {'title': 'x'},
        ),
      );

      expect(result, isA<SyncSuccess>());
      expect((result as SyncSuccess).remoteVersion, isNull);
      final data = await harness.firestore
          .collection('todos')
          .doc('t-1')
          .get()
          .then((snap) => snap.data());
      expect(data, {'title': 'x'});
    });

    test('nested collection path resolves correctly', () async {
      final h = FirestoreTestHarness.create(
        entityMapping: {
          'note': FirestoreEntityConfig(
            collectionPath: 'users/u1/notes',
          ),
        },
      );

      await h.adapter.push(makeOperation(payload: {'title': 'nested'}));

      final data = await h.firestore
          .collection('users/u1/notes')
          .doc('entity-1')
          .get()
          .then((snap) => snap.data());
      expect(data?['title'], 'nested');
    });
  });

  group('update', () {
    Future<void> seed({
      String title = 'Old',
      int version = 3,
      Map<String, dynamic>? extra,
    }) =>
        noteDoc().set({
          'title': title,
          'sync_version': version,
          'unrelated': 'keep-me',
          ...?extra,
        });

    test('merges payload fields and preserves unrelated fields', () async {
      await seed();

      final result = await harness.adapter.push(
        makeOperation(
          type: SyncOperationType.update,
          payload: {'title': 'New'},
          metadata: {'baseVersion': 3},
        ),
      );

      expect(result, isA<SyncSuccess>());
      final success = result as SyncSuccess;
      expect(success.remoteVersion, '4');

      final data = await noteData();
      expect(data?['title'], 'New');
      expect(data?['unrelated'], 'keep-me');
      expect(data?['sync_version'], 4);
    });

    test('increments version even without baseVersion', () async {
      await seed(version: 7);

      final result = await harness.adapter.push(
        makeOperation(
          type: SyncOperationType.update,
          payload: {'title': 'New'},
        ),
      );

      expect(result, isA<SyncSuccess>());
      expect((result as SyncSuccess).remoteVersion, '8');
      expect((await noteData())?['sync_version'], 8);
    });

    test('missing document returns notFound', () async {
      final result = await harness.adapter.push(
        makeOperation(
          type: SyncOperationType.update,
          payload: {'title': 'x'},
        ),
      );

      expect(result, isA<SyncFailure>());
      final failure = result as SyncFailure;
      expect(failure.error.code, SyncErrorCode.notFound);
      expect(failure.error.isRetryable, isFalse);
    });

    test('payload containing versionField is rejected', () async {
      await seed();

      final result = await harness.adapter.push(
        makeOperation(
          type: SyncOperationType.update,
          payload: {'sync_version': 99, 'title': 'x'},
        ),
      );

      expect(result, isA<SyncFailure>());
      expect((result as SyncFailure).error.code, SyncErrorCode.validation);
      expect((await noteData())?['sync_version'], 3);
      expect((await noteData())?['title'], 'Old');
    });

    test('unversioned config ignores version metadata', () async {
      await harness.firestore
          .collection('todos')
          .doc('t-1')
          .set({'title': 'Old'});

      final result = await harness.adapter.push(
        makeOperation(
          entityType: 'todo',
          entityId: 't-1',
          type: SyncOperationType.update,
          payload: {'title': 'New'},
          metadata: {'baseVersion': 99},
        ),
      );

      expect(result, isA<SyncSuccess>());
      expect((result as SyncSuccess).remoteVersion, isNull);
      final data = await harness.firestore
          .collection('todos')
          .doc('t-1')
          .get()
          .then((snap) => snap.data());
      expect(data?['title'], 'New');
    });
  });

  group('optimistic concurrency', () {
    Future<void> seedVersioned({int version = 3}) => harness.firestore
        .collection('notes')
        .doc('entity-1')
        .set({'title': 'Old', 'sync_version': version});

    test('matching baseVersion succeeds', () async {
      await seedVersioned();
      final result = await harness.adapter.push(
        makeOperation(
          type: SyncOperationType.update,
          payload: {'title': 'New'},
          metadata: {'baseVersion': 3},
        ),
      );
      expect(result, isA<SyncSuccess>());
      expect((await noteData())?['sync_version'], 4);
    });

    test('stale baseVersion returns SyncConflict without mutation', () async {
      await seedVersioned(version: 5);
      final result = await harness.adapter.push(
        makeOperation(
          type: SyncOperationType.update,
          payload: {'title': 'Stale'},
          metadata: {'baseVersion': 3},
        ),
      );

      expect(result, isA<SyncConflict>());
      final conflict = result as SyncConflict;
      expect(conflict.remoteSnapshot['title'], 'Old');
      expect(conflict.remoteVersion, '5');
      expect((await noteData())?['title'], 'Old');
      expect((await noteData())?['sync_version'], 5);
    });

    test('non-integer stored version cannot be incremented', () async {
      await harness.firestore
          .collection('notes')
          .doc('entity-1')
          .set({'title': 'Old', 'sync_version': 'three'});

      final result = await harness.adapter.push(
        makeOperation(
          type: SyncOperationType.update,
          payload: {'title': 'New'},
        ),
      );
      expect(result, isA<SyncConflict>());
    });

    test('string baseVersion is rejected before Firestore access', () async {
      await seedVersioned();
      final result = await harness.adapter.push(
        makeOperation(
          type: SyncOperationType.update,
          payload: {'title': 'x'},
          metadata: {'baseVersion': '3'},
        ),
      );
      expect(result, isA<SyncFailure>());
      expect((result as SyncFailure).error.code, SyncErrorCode.validation);
      expect((await noteData())?['title'], 'Old');
    });

    test('double baseVersion is rejected', () async {
      await seedVersioned();
      final result = await harness.adapter.push(
        makeOperation(
          type: SyncOperationType.update,
          payload: {'title': 'x'},
          metadata: {'baseVersion': 3.0},
        ),
      );
      expect(result, isA<SyncFailure>());
      expect((result as SyncFailure).error.code, SyncErrorCode.validation);
    });

    test('custom expectedVersionKey is honored', () async {
      final h = FirestoreTestHarness.create(expectedVersionKey: 'etag');
      await h.firestore
          .collection('notes')
          .doc('entity-1')
          .set({'title': 'Old', 'sync_version': 2});

      final result = await h.adapter.push(
        makeOperation(
          type: SyncOperationType.update,
          payload: {'title': 'New'},
          metadata: {'etag': 2},
        ),
      );
      expect(result, isA<SyncSuccess>());
      expect((result as SyncSuccess).remoteVersion, '3');
    });
  });

  group('delete', () {
    test('deletes the document', () async {
      await harness.firestore
          .collection('notes')
          .doc('entity-1')
          .set({'title': 'Bye', 'sync_version': 2});

      final result = await harness.adapter.push(
        makeOperation(type: SyncOperationType.delete),
      );

      expect(result, isA<SyncSuccess>());
      expect(await noteData(), isNull);
    });

    test('absent document is an idempotent success', () async {
      final result = await harness.adapter.push(
        makeOperation(type: SyncOperationType.delete),
      );
      expect(result, isA<SyncSuccess>());
    });

    test('versioned delete enforces expected version', () async {
      await harness.firestore
          .collection('notes')
          .doc('entity-1')
          .set({'title': 'Bye', 'sync_version': 4});

      final stale = await harness.adapter.push(
        makeOperation(
          type: SyncOperationType.delete,
          metadata: {'baseVersion': 2},
        ),
      );
      expect(stale, isA<SyncConflict>());
      expect(await noteData(), isNotNull);

      final fresh = await harness.adapter.push(
        makeOperation(
          type: SyncOperationType.delete,
          idempotencyKey: 'delete-key-2',
          metadata: {'baseVersion': 4},
        ),
      );
      expect(fresh, isA<SyncSuccess>());
      expect(await noteData(), isNull);
    });
  });

  group('idempotency ledger', () {
    test('retrying a create replays success without duplicating', () async {
      final op = makeOperation(
        operationId: 'op-create',
        idempotencyKey: 'key-create',
        payload: {'title': 'Once'},
      );

      final first = await harness.adapter.push(op);
      expect(first, isA<SyncSuccess>());

      // Simulate a remote-side change after the first push; the retry
      // must still replay the recorded result, not re-apply.
      await noteDoc().update({'title': 'Tampered'});
      final second = await harness.adapter.push(op);

      expect(second, isA<SyncSuccess>());
      expect((second as SyncSuccess).syncedData?['title'], 'Once');
      expect((await noteData())?['title'], 'Tampered');
    });

    test('retrying an update does not apply it twice', () async {
      await harness.firestore
          .collection('notes')
          .doc('entity-1')
          .set({'title': 'Old', 'sync_version': 1});
      final op = makeOperation(
        operationId: 'op-update',
        idempotencyKey: 'key-update',
        type: SyncOperationType.update,
        payload: {'title': 'New'},
      );

      await harness.adapter.push(op);
      final retry = await harness.adapter.push(op);

      expect(retry, isA<SyncSuccess>());
      expect((retry as SyncSuccess).remoteVersion, '2');
      expect((await noteData())?['sync_version'], 2);
    });

    test('retrying a delete does not remove a recreated document', () async {
      final op = makeOperation(
        operationId: 'op-delete',
        idempotencyKey: 'key-delete',
        type: SyncOperationType.delete,
      );

      await harness.firestore
          .collection('notes')
          .doc('entity-1')
          .set({'title': 'V1', 'sync_version': 1});
      expect(await harness.adapter.push(op), isA<SyncSuccess>());
      expect(await noteData(), isNull);

      // Entity ID reused: a new document exists under the same ID.
      await harness.firestore
          .collection('notes')
          .doc('entity-1')
          .set({'title': 'V2', 'sync_version': 1});

      final retry = await harness.adapter.push(op);
      expect(retry, isA<SyncSuccess>());
      // The recreated document must survive the stale delete retry.
      expect((await noteData())?['title'], 'V2');
    });

    test('same key with different operationId is rejected', () async {
      await harness.adapter.push(
        makeOperation(
          operationId: 'op-a',
          idempotencyKey: 'shared-key',
          payload: {'title': 'x'},
        ),
      );

      final result = await harness.adapter.push(
        makeOperation(
          operationId: 'op-b',
          idempotencyKey: 'shared-key',
          payload: {'title': 'x'},
        ),
      );

      expect(result, isA<SyncFailure>());
      expect((result as SyncFailure).error.code, SyncErrorCode.validation);
    });

    test('same key with different payload is rejected', () async {
      await harness.adapter.push(
        makeOperation(
          idempotencyKey: 'shared-key-2',
          payload: {'title': 'x'},
        ),
      );

      final result = await harness.adapter.push(
        makeOperation(
          idempotencyKey: 'shared-key-2',
          payload: {'title': 'different'},
        ),
      );

      expect(result, isA<SyncFailure>());
      expect((result as SyncFailure).error.code, SyncErrorCode.validation);
    });

    test('same key with different operation type is rejected', () async {
      await harness.adapter.push(
        makeOperation(
          idempotencyKey: 'shared-key-3',
          payload: {'title': 'x'},
        ),
      );

      final result = await harness.adapter.push(
        makeOperation(
          idempotencyKey: 'shared-key-3',
          type: SyncOperationType.delete,
        ),
      );

      expect(result, isA<SyncFailure>());
      expect((result as SyncFailure).error.code, SyncErrorCode.validation);
    });

    test('ledger and entity writes are atomic', () async {
      final result = await harness.adapter.push(
        makeOperation(
          idempotencyKey: 'atomic-key',
          payload: {'title': 'Atomic'},
        ),
      );
      expect(result, isA<SyncSuccess>());

      // Exactly one ledger document exists for the operation.
      final ledger =
          await harness.firestore.collection('_sync_test_operations').get();
      expect(ledger.docs, hasLength(1));
      final entry = ledger.docs.single.data();
      expect(entry['idempotencyKey'], 'atomic-key');
      expect(entry['operationId'], 'op-1');
      expect(entry['entityType'], 'note');
      expect(entry['entityId'], 'entity-1');
      expect(entry['operationType'], 'create');
      expect(entry['status'], 'applied');
      expect(entry['remoteVersion'], '1');
    });
  });

  group('input validation', () {
    test('entityId containing / is rejected without Firestore access',
        () async {
      final result = await harness.adapter.push(
        makeOperation(entityId: 'a/b', payload: {}),
      );
      expect(result, isA<SyncFailure>());
      expect((result as SyncFailure).error.code, SyncErrorCode.validation);
    });

    test('dot entityIds are rejected', () async {
      for (final id in ['.', '..']) {
        final result = await harness.adapter.push(
          makeOperation(entityId: id, payload: {}),
        );
        expect(result, isA<SyncFailure>());
        expect((result as SyncFailure).error.code, SyncErrorCode.validation);
      }
    });

    test('reserved __id__ entityIds are rejected', () async {
      final result = await harness.adapter.push(
        makeOperation(entityId: '__reserved__', payload: {}),
      );
      expect(result, isA<SyncFailure>());
      expect((result as SyncFailure).error.code, SyncErrorCode.validation);
    });
  });

  group('contract', () {
    test('every result preserves the operation id', () async {
      await harness.firestore
          .collection('notes')
          .doc('entity-1')
          .set({'title': 'Old', 'sync_version': 1});

      final results = await Future.wait([
        harness.adapter.push(
          makeOperation(
            idempotencyKey: 'k1',
            payload: {'title': 'a'},
          ),
        ),
        harness.adapter.push(
          makeOperation(
            operationId: 'op-2',
            idempotencyKey: 'k2',
            entityId: 'missing',
            type: SyncOperationType.update,
            payload: {'title': 'b'},
          ),
        ),
        harness.adapter.push(
          makeOperation(
            operationId: 'op-3',
            idempotencyKey: 'k3',
            entityType: 'unknown',
            payload: {},
          ),
        ),
      ]);

      expect(results[0].operationId, 'op-1');
      expect(results[1].operationId, 'op-2');
      expect(results[2].operationId, 'op-3');
    });
  });
}
