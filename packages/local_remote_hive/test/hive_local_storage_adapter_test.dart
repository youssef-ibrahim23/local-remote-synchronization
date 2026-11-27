import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:hive/hive.dart';
import 'package:local_remote_hive/local_remote_hive.dart';
import 'package:local_remote_synchronization/local_remote_synchronization.dart';
import 'package:test/test.dart';

void main() {
  late Directory tempDir;
  late Box<String> box;

  Future<void> openBox() async {
    Hive.init(tempDir.path);
    box = await Hive.openBox<String>('sync_outbox');
  }

  setUp(() async {
    tempDir = await Directory.systemTemp.createTemp('hive_outbox_test_');
    await openBox();
  });

  tearDown(() async {
    await box.close();
    await Hive.deleteFromDisk();
    if (tempDir.existsSync()) {
      await tempDir.delete(recursive: true);
    }
  });

  group('HiveOutboxSchema', () {
    test('rejects invalid boxName identifiers', () {
      expect(
        () => HiveLocalStorageAdapter(
          box: box,
          boxName: 'Invalid Name!',
        ),
        throwsArgumentError,
      );
      expect(
        () => HiveLocalStorageAdapter(
          box: box,
          boxName: '123_invalid',
        ),
        throwsArgumentError,
      );
      expect(
        () => HiveLocalStorageAdapter(
          box: box,
          boxName: 'valid_box_name',
        ),
        returnsNormally,
      );
    });

    test('ensureInitialized is idempotent', () async {
      final adapter = HiveLocalStorageAdapter(box: box);
      addTearDown(adapter.dispose);

      await adapter.ensureInitialized();
      await adapter.ensureInitialized();

      final ops = await adapter.fetchEligibleOperations(
        now: DateTime.now().toUtc(),
        limit: 10,
      );
      expect(ops, isEmpty);
    });
  });

  group('Snapshot Persistence', () {
    test('persists and restores all fields', () async {
      final fixedNow = DateTime.utc(2026, 6, 1, 10);
      final occurredLocal = DateTime(2026, 6, 1, 9, 30);

      var adapter = HiveLocalStorageAdapter(
        box: box,
        operationIdGenerator: () => 'op-persist-1',
        clock: () => fixedNow,
      );

      final enqueued = await adapter.runInTransaction(
        (outbox) => outbox.enqueue(
          LocalMutation(
            entityType: 'tasks',
            entityId: 'task-1',
            type: SyncOperationType.create,
            payload: const <String, dynamic>{
              'title': 'Write tests',
              'nested': <String, dynamic>{
                'tags': <Object?>['urgent', 1, true, null, 2.5],
              },
            },
            idempotencyKey: 'custom-idem-1',
            occurredAt: occurredLocal,
            metadata: const <String, dynamic>{'baseVersion': 1},
          ),
        ),
      );

      await adapter.dispose();
      await box.close();

      // Reopen box and verify persistence.
      await openBox();
      adapter = HiveLocalStorageAdapter(box: box);
      addTearDown(adapter.dispose);

      final fetched = await adapter.fetchEligibleOperations(
        now: fixedNow,
        limit: 10,
      );
      expect(fetched, hasLength(1));
      expect(fetched.single, equals(enqueued));
      expect(fetched.single.operationId, 'op-persist-1');
      expect(fetched.single.idempotencyKey, 'custom-idem-1');
      expect(fetched.single.createdAt, fixedNow);
      expect(fetched.single.createdAt.isUtc, isTrue);
      expect(fetched.single.occurredAt, occurredLocal.toUtc());
      expect(fetched.single.occurredAt!.isUtc, isTrue);
      expect(
        fetched.single.createdAt,
        isNot(equals(fetched.single.occurredAt)),
      );
      expect(fetched.single.sequenceNumber, isNotNull);
    });

    test('preserves nullable fields for delete operations', () async {
      final adapter = HiveLocalStorageAdapter(
        box: box,
        operationIdGenerator: () => 'op-del-1',
        clock: () => DateTime.utc(2026, 6, 1, 12),
      );
      addTearDown(adapter.dispose);

      final op = await adapter.runInTransaction(
        (outbox) => outbox.enqueue(
          LocalMutation(
            entityType: 'tasks',
            entityId: 'task-99',
            type: SyncOperationType.delete,
          ),
        ),
      );

      expect(op.payload, isNull);
      expect(op.idempotencyKey, 'op-del-1');

      final fetched = await adapter.fetchEligibleOperations(
        now: DateTime.utc(2026, 6, 1, 12),
        limit: 10,
      );
      expect(fetched.single.payload, isNull);
      expect(fetched.single.nextRetryAt, isNull);
      expect(fetched.single.lastError, isNull);
    });

    test('snapshot contains schemaVersion and lastSequenceNumber', () async {
      var counter = 0;
      final adapter = HiveLocalStorageAdapter(
        box: box,
        operationIdGenerator: () => 'op-seq-${++counter}',
      );
      addTearDown(adapter.dispose);

      await adapter.runInTransaction(
        (outbox) async {
          await outbox.enqueue(
            LocalMutation(
              entityType: 'tasks',
              entityId: 't-1',
              type: SyncOperationType.create,
              payload: const <String, dynamic>{'id': 't-1'},
            ),
          );
          await outbox.enqueue(
            LocalMutation(
              entityType: 'tasks',
              entityId: 't-2',
              type: SyncOperationType.create,
              payload: const <String, dynamic>{'id': 't-2'},
            ),
          );
        },
      );

      final json = box.get(HiveOutboxSchema.snapshotKey);
      expect(json, isNotNull);
      final decoded = jsonDecode(json!) as Map<String, dynamic>;
      expect(decoded['schemaVersion'], HiveOutboxSchema.currentVersion);
      expect(decoded['lastSequenceNumber'], 2);
      expect(decoded['operations'], hasLength(2));
    });
  });

  group('Snapshot Versioning', () {
    test('loads current version snapshot', () async {
      // Write a valid v2 snapshot directly.
      final snapshot = HiveOutboxSnapshot(
        schemaVersion: HiveOutboxSchema.currentVersion,
        lastSequenceNumber: 5,
        operations: <SyncOperation>[],
      );
      await box.put(HiveOutboxSchema.snapshotKey, snapshot.toJson());

      final adapter = HiveLocalStorageAdapter(box: box);
      addTearDown(adapter.dispose);

      final ops = await adapter.fetchEligibleOperations(
        now: DateTime.now().toUtc(),
        limit: 10,
      );
      expect(ops, isEmpty);
    });

    test('migrates v1 snapshot to v2', () async {
      // Write a v1 snapshot without lastSequenceNumber.
      final v1Json = jsonEncode(<String, dynamic>{
        'schemaVersion': 1,
        'operations': <Map<String, dynamic>>[],
      });
      await box.put(HiveOutboxSchema.snapshotKey, v1Json);

      final adapter = HiveLocalStorageAdapter(box: box);
      addTearDown(adapter.dispose);

      // Should not throw — migration happens automatically.
      final ops = await adapter.fetchEligibleOperations(
        now: DateTime.now().toUtc(),
        limit: 10,
      );
      expect(ops, isEmpty);

      // Note: The snapshot is only persisted on mutation, not on read.
      // The migration happens in-memory during load.
    });

    test('rejects unsupported future version', () async {
      final futureJson = jsonEncode(<String, dynamic>{
        'schemaVersion': HiveOutboxSchema.currentVersion + 1,
        'lastSequenceNumber': 0,
        'operations': <Map<String, dynamic>>[],
      });
      await box.put(HiveOutboxSchema.snapshotKey, futureJson);

      final adapter = HiveLocalStorageAdapter(box: box);
      addTearDown(adapter.dispose);

      await expectLater(
        () => adapter.fetchEligibleOperations(
          now: DateTime.now().toUtc(),
          limit: 10,
        ),
        throwsA(isA<HiveOutboxVersionException>()),
      );
    });

    test('rejects malformed version field', () async {
      final badJson = jsonEncode(<String, dynamic>{
        'schemaVersion': 'not_an_int',
        'lastSequenceNumber': 0,
        'operations': <Map<String, dynamic>>[],
      });
      await box.put(HiveOutboxSchema.snapshotKey, badJson);

      final adapter = HiveLocalStorageAdapter(box: box);
      addTearDown(adapter.dispose);

      await expectLater(
        () => adapter.fetchEligibleOperations(
          now: DateTime.now().toUtc(),
          limit: 10,
        ),
        throwsA(isA<HiveOutboxCorruptionException>()),
      );
    });
  });

  group('Corruption Handling', () {
    test('throws on malformed snapshot JSON', () async {
      await box.put(HiveOutboxSchema.snapshotKey, '{not valid json');

      final adapter = HiveLocalStorageAdapter(box: box);
      addTearDown(adapter.dispose);

      await expectLater(
        () => adapter.fetchEligibleOperations(
          now: DateTime.now().toUtc(),
          limit: 10,
        ),
        throwsA(isA<HiveOutboxCorruptionException>()),
      );
    });

    test('throws on snapshot that is not a JSON object', () async {
      await box.put(HiveOutboxSchema.snapshotKey, '[1, 2, 3]');

      final adapter = HiveLocalStorageAdapter(box: box);
      addTearDown(adapter.dispose);

      await expectLater(
        () => adapter.fetchEligibleOperations(
          now: DateTime.now().toUtc(),
          limit: 10,
        ),
        throwsA(isA<HiveOutboxCorruptionException>()),
      );
    });

    test('throws on missing operations field', () async {
      final badJson = jsonEncode(<String, dynamic>{
        'schemaVersion': HiveOutboxSchema.currentVersion,
        'lastSequenceNumber': 0,
      });
      await box.put(HiveOutboxSchema.snapshotKey, badJson);

      final adapter = HiveLocalStorageAdapter(box: box);
      addTearDown(adapter.dispose);

      await expectLater(
        () => adapter.fetchEligibleOperations(
          now: DateTime.now().toUtc(),
          limit: 10,
        ),
        throwsA(isA<HiveOutboxCorruptionException>()),
      );
    });

    test('throws on invalid operation in snapshot', () async {
      final badJson = jsonEncode(<String, dynamic>{
        'schemaVersion': HiveOutboxSchema.currentVersion,
        'lastSequenceNumber': 1,
        'operations': <Map<String, dynamic>>[
          {'invalid': 'operation'},
        ],
      });
      await box.put(HiveOutboxSchema.snapshotKey, badJson);

      final adapter = HiveLocalStorageAdapter(box: box);
      addTearDown(adapter.dispose);

      await expectLater(
        () => adapter.fetchEligibleOperations(
          now: DateTime.now().toUtc(),
          limit: 10,
        ),
        throwsA(isA<HiveOutboxCorruptionException>()),
      );
    });

    test('preserves corrupted data on disk', () async {
      const corruptedData = '{corrupted snapshot data';
      await box.put(HiveOutboxSchema.snapshotKey, corruptedData);

      final adapter = HiveLocalStorageAdapter(box: box);
      addTearDown(adapter.dispose);

      // Attempt to read — should throw.
      await expectLater(
        () => adapter.fetchEligibleOperations(
          now: DateTime.now().toUtc(),
          limit: 10,
        ),
        throwsA(isA<HiveOutboxCorruptionException>()),
      );

      // Verify the corrupted data is still on disk (not deleted).
      expect(box.get(HiveOutboxSchema.snapshotKey), corruptedData);
    });
  });

  group('Per-Entity FIFO', () {
    test('blocks later operations for same entity across all blocking statuses',
        () async {
      final t0 = DateTime.utc(2026, 6, 1, 10);
      var counter = 0;
      final adapter = HiveLocalStorageAdapter(
        box: box,
        operationIdGenerator: () => 'op-${++counter}',
        clock: () => t0.add(Duration(seconds: counter)),
      );
      addTearDown(adapter.dispose);

      // Entity A: op-1, op-2. Entity B: op-3.
      final op1 = await adapter.runInTransaction(
        (outbox) => outbox.enqueue(
          LocalMutation(
            entityType: 'tasks',
            entityId: 'A',
            type: SyncOperationType.create,
            payload: const <String, dynamic>{'v': 1},
          ),
        ),
      );
      final op2 = await adapter.runInTransaction(
        (outbox) => outbox.enqueue(
          LocalMutation(
            entityType: 'tasks',
            entityId: 'A',
            type: SyncOperationType.update,
            payload: const <String, dynamic>{'v': 2},
          ),
        ),
      );
      final op3 = await adapter.runInTransaction(
        (outbox) => outbox.enqueue(
          LocalMutation(
            entityType: 'tasks',
            entityId: 'B',
            type: SyncOperationType.create,
            payload: const <String, dynamic>{'v': 1},
          ),
        ),
      );

      // 1. Earlier pending blocks op2; op1 and op3 are eligible.
      var eligible = await adapter.fetchEligibleOperations(
        now: t0.add(const Duration(minutes: 1)),
        limit: 10,
      );
      expect(
        eligible.map((o) => o.operationId),
        <String>[op1.operationId, op3.operationId],
      );

      // 2. Earlier inFlight blocks op2; op3 remains independent.
      await adapter.markInFlight(op1.operationId);
      eligible = await adapter.fetchEligibleOperations(
        now: t0.add(const Duration(minutes: 1)),
        limit: 10,
      );
      expect(eligible.map((o) => o.operationId), <String>[op3.operationId]);

      // 3. Earlier failed (backoff in future) blocks op2; op3 remains eligible.
      await adapter.markFailed(
        op1.operationId,
        attemptCount: 1,
        nextRetryAt: t0.add(const Duration(minutes: 10)),
        error: SyncError(
          code: SyncErrorCode.network,
          message: 'Offline',
          isRetryable: true,
        ),
      );
      eligible = await adapter.fetchEligibleOperations(
        now: t0.add(const Duration(minutes: 1)),
        limit: 10,
      );
      expect(eligible.map((o) => o.operationId), <String>[op3.operationId]);

      // 4. Earlier deadLetter blocks op2; op3 remains eligible.
      await adapter.markDeadLetter(
        op1.operationId,
        attemptCount: 3,
        error: SyncError(
          code: SyncErrorCode.validation,
          message: 'Bad payload',
          isRetryable: false,
        ),
      );
      eligible = await adapter.fetchEligibleOperations(
        now: t0.add(const Duration(minutes: 20)),
        limit: 10,
      );
      expect(eligible.map((o) => o.operationId), <String>[op3.operationId]);

      // 5. Once op1 succeeds (removed), op2 becomes eligible.
      await adapter.markSucceeded(op1.operationId);
      eligible = await adapter.fetchEligibleOperations(
        now: t0.add(const Duration(minutes: 20)),
        limit: 10,
      );
      expect(
        eligible.map((o) => o.operationId),
        <String>[op2.operationId, op3.operationId],
      );
    });
  });

  group('State transitions & Crash recovery', () {
    test('recovers inFlight operations after restart and preserves retry state',
        () async {
      final t0 = DateTime.utc(2026, 6, 1, 10);
      var adapter = HiveLocalStorageAdapter(
        box: box,
        operationIdGenerator: () => 'op-recover-1',
        clock: () => t0,
      );

      final op = await adapter.runInTransaction(
        (outbox) => outbox.enqueue(
          LocalMutation(
            entityType: 'tasks',
            entityId: 't-1',
            type: SyncOperationType.create,
            payload: const <String, dynamic>{'title': 'Task'},
          ),
        ),
      );
      await adapter.markInFlight(op.operationId);
      await adapter.dispose();
      await box.close();

      // Simulate restart.
      await openBox();
      adapter = HiveLocalStorageAdapter(
        box: box,
        clock: () => t0.add(const Duration(seconds: 5)),
      );
      addTearDown(adapter.dispose);

      final recoveredCount = await adapter.recoverInFlightOperations();
      expect(recoveredCount, 1);

      final eligible = await adapter.fetchEligibleOperations(
        now: t0.add(const Duration(seconds: 5)),
        limit: 10,
      );
      expect(eligible.single.status, SyncOperationStatus.pending);
      expect(eligible.single.idempotencyKey, 'op-recover-1');

      // Idempotent markSucceeded.
      await adapter.markSucceeded(op.operationId);
      await adapter.markSucceeded(op.operationId);

      final afterSucceed = await adapter.fetchEligibleOperations(
        now: t0.add(const Duration(seconds: 10)),
        limit: 10,
      );
      expect(afterSucceed, isEmpty);
    });

    test('watchPendingCount emits initial count and reacts to mutations',
        () async {
      var counter = 0;
      final adapter = HiveLocalStorageAdapter(
        box: box,
        operationIdGenerator: () => 'op-watch-${++counter}',
      );
      addTearDown(adapter.dispose);

      final allEventsFuture = adapter.watchPendingCount().toList();
      final queue = StreamIterator<int>(adapter.watchPendingCount());
      addTearDown(queue.cancel);

      expect(await queue.moveNext(), isTrue);
      expect(queue.current, 0);

      final op = await adapter.runInTransaction(
        (outbox) => outbox.enqueue(
          LocalMutation(
            entityType: 'tasks',
            entityId: 't-1',
            type: SyncOperationType.delete,
          ),
        ),
      );
      expect(await queue.moveNext(), isTrue);
      expect(queue.current, 1);

      await adapter.markSucceeded(op.operationId);
      expect(await queue.moveNext(), isTrue);
      expect(queue.current, 0);

      await adapter.dispose();
      expect(await queue.moveNext(), isFalse);
      expect(await allEventsFuture, <int>[0, 1, 0]);
    });

    test(
        'watchPendingCount does not lose updates during initial subscription '
        'setup and supports multiple listeners', () async {
      var counter = 0;
      final adapter = HiveLocalStorageAdapter(
        box: box,
        operationIdGenerator: () => 'op-race-${++counter}',
      );
      addTearDown(adapter.dispose);

      // Start listening and immediately enqueue without awaiting initial count
      // so the mutation races with initial _queryPendingCount().
      final eventsFuture1 = adapter.watchPendingCount().toList();
      final eventsFuture2 = adapter.watchPendingCount().toList();

      await adapter.runInTransaction(
        (outbox) => outbox.enqueue(
          LocalMutation(
            entityType: 'tasks',
            entityId: 't-race',
            type: SyncOperationType.delete,
          ),
        ),
      );

      // Closing the adapter completes both streams after all events flush.
      await adapter.dispose();
      final listener1 = await eventsFuture1;
      final listener2 = await eventsFuture2;

      // Only [0, 1] or [1] are valid; [1, 0] or [0, 1, 0] must fail.
      expect(
        listener1,
        anyOf(equals(<int>[0, 1]), equals(<int>[1])),
      );
      expect(
        listener2,
        anyOf(equals(<int>[0, 1]), equals(<int>[1])),
      );
    });

    test('dispose completes all active watchPendingCount streams', () async {
      final adapter = HiveLocalStorageAdapter(box: box);
      addTearDown(adapter.dispose);

      final q1 = StreamIterator<int>(adapter.watchPendingCount());
      final q2 = StreamIterator<int>(adapter.watchPendingCount());
      addTearDown(q1.cancel);
      addTearDown(q2.cancel);

      expect(await q1.moveNext(), isTrue);
      expect(q1.current, 0);
      expect(await q2.moveNext(), isTrue);
      expect(q2.current, 0);

      await adapter.dispose();

      expect(await q1.moveNext(), isFalse);
      expect(await q2.moveNext(), isFalse);
    });

    test('outbox writes outside transaction notify via refreshPendingCount',
        () async {
      var counter = 0;
      final adapter = HiveLocalStorageAdapter(
        box: box,
        operationIdGenerator: () => 'op-ext-${++counter}',
      );
      addTearDown(adapter.dispose);

      final allEventsFuture = adapter.watchPendingCount().toList();
      final queue = StreamIterator<int>(adapter.watchPendingCount());
      addTearDown(queue.cancel);

      expect(await queue.moveNext(), isTrue);
      expect(queue.current, 0);

      // Write through adapter.outbox (direct write, no transaction).
      await adapter.outbox.enqueue(
        LocalMutation(
          entityType: 'tasks',
          entityId: 't-ext',
          type: SyncOperationType.create,
          payload: const <String, dynamic>{'id': 't-ext'},
        ),
      );

      // refreshPendingCount notifies listeners.
      await adapter.refreshPendingCount();
      expect(await queue.moveNext(), isTrue);
      expect(queue.current, 1);

      await adapter.dispose();
      expect(await queue.moveNext(), isFalse);
      expect(await allEventsFuture, <int>[0, 1]);
    });

    test('failed retry eligibility requires nextRetryAt <= now and non-null',
        () async {
      final t0 = DateTime.utc(2026, 6, 1, 10);
      var counter = 0;
      final adapter = HiveLocalStorageAdapter(
        box: box,
        operationIdGenerator: () => 'op-retry-${++counter}',
        clock: () => t0,
      );
      addTearDown(adapter.dispose);

      final futureOp = await adapter.runInTransaction(
        (outbox) => outbox.enqueue(
          LocalMutation(
            entityType: 'tasks',
            entityId: 'future',
            type: SyncOperationType.delete,
          ),
        ),
      );
      final pastOp = await adapter.runInTransaction(
        (outbox) => outbox.enqueue(
          LocalMutation(
            entityType: 'tasks',
            entityId: 'past',
            type: SyncOperationType.delete,
          ),
        ),
      );
      final nullRetryOp = await adapter.runInTransaction(
        (outbox) => outbox.enqueue(
          LocalMutation(
            entityType: 'tasks',
            entityId: 'null-retry',
            type: SyncOperationType.delete,
          ),
        ),
      );

      final err = SyncError(
        code: SyncErrorCode.network,
        message: 'Timeout',
        isRetryable: true,
      );

      await adapter.markFailed(
        futureOp.operationId,
        attemptCount: 1,
        nextRetryAt: t0.add(const Duration(minutes: 5)),
        error: err,
      );
      await adapter.markFailed(
        pastOp.operationId,
        attemptCount: 1,
        nextRetryAt: t0.subtract(const Duration(seconds: 1)),
        error: err,
      );

      // Force failed status with NULL next_retry_at by directly modifying
      // the snapshot.
      final snapshotJson = box.get(HiveOutboxSchema.snapshotKey);
      if (snapshotJson != null) {
        final snapshot = HiveOutboxSnapshot.fromJson(snapshotJson);
        for (var i = 0; i < snapshot.operations.length; i++) {
          if (snapshot.operations[i].operationId == nullRetryOp.operationId) {
            snapshot.operations[i] = snapshot.operations[i].copyWith(
              status: SyncOperationStatus.failed,
              clearNextRetryAt: true,
            );
          }
        }
        await box.put(HiveOutboxSchema.snapshotKey, snapshot.toJson());
      }

      final eligible = await adapter.fetchEligibleOperations(
        now: t0,
        limit: 10,
      );
      expect(
        eligible.map((o) => o.operationId),
        <String>[pastOp.operationId],
      );
    });
  });

  group('Atomicity', () {
    test('multiple outbox enqueues commit together in runInTransaction',
        () async {
      var counter = 0;
      final adapter = HiveLocalStorageAdapter(
        box: box,
        operationIdGenerator: () => 'op-atom-${++counter}',
      );
      addTearDown(adapter.dispose);

      await adapter.runInTransaction((outbox) async {
        await outbox.enqueue(
          LocalMutation(
            entityType: 'tasks',
            entityId: 't-1',
            type: SyncOperationType.create,
            payload: const <String, dynamic>{'id': 't-1'},
          ),
        );
        await outbox.enqueue(
          LocalMutation(
            entityType: 'tasks',
            entityId: 't-2',
            type: SyncOperationType.create,
            payload: const <String, dynamic>{'id': 't-2'},
          ),
        );
      });

      final ops = await adapter.fetchEligibleOperations(
        now: DateTime.now().toUtc().add(const Duration(minutes: 1)),
        limit: 10,
      );
      expect(ops, hasLength(2));
    });

    test('outbox failure rolls back all buffered writes', () async {
      var counter = 0;
      final adapter = HiveLocalStorageAdapter(
        box: box,
        operationIdGenerator: () => 'op-rollback-${++counter}',
      );
      addTearDown(adapter.dispose);

      await expectLater(
        () => adapter.runInTransaction((outbox) async {
          await outbox.enqueue(
            LocalMutation(
              entityType: 'tasks',
              entityId: 't-1',
              type: SyncOperationType.create,
              payload: const <String, dynamic>{'id': 't-1'},
            ),
          );
          throw StateError('transaction aborted');
        }),
        throwsStateError,
      );

      final ops = await adapter.fetchEligibleOperations(
        now: DateTime.now().toUtc().add(const Duration(minutes: 1)),
        limit: 10,
      );
      expect(ops, isEmpty);
    });
  });

  group('Concurrency', () {
    test('concurrent direct enqueues produce unique sequence numbers',
        () async {
      var counter = 0;
      final adapter = HiveLocalStorageAdapter(
        box: box,
        operationIdGenerator: () => 'op-concurrent-${++counter}',
      );
      addTearDown(adapter.dispose);

      // Launch multiple concurrent enqueues.
      final futures = List.generate(
        10,
        (i) => adapter.outbox.enqueue(
          LocalMutation(
            entityType: 'tasks',
            entityId: 't-$i',
            type: SyncOperationType.create,
            payload: <String, dynamic>{'index': i},
          ),
        ),
      );

      final results = await Future.wait(futures);

      // All operations should have unique sequence numbers.
      final seqNumbers = results.map((op) => op.sequenceNumber).toSet();
      expect(seqNumbers, hasLength(10));

      // Sequence numbers should be 1-10 (or some contiguous range).
      final sortedSeqs = seqNumbers.toList()..sort();
      expect(sortedSeqs.first, 1);
      expect(sortedSeqs.last, 10);

      // All operations should be in the snapshot.
      final ops = await adapter.fetchEligibleOperations(
        now: DateTime.now().toUtc().add(const Duration(minutes: 1)),
        limit: 20,
      );
      expect(ops, hasLength(10));
    });

    test('concurrent transactions do not lose operations', () async {
      var counter = 0;
      final adapter = HiveLocalStorageAdapter(
        box: box,
        operationIdGenerator: () => 'op-txn-${++counter}',
      );
      addTearDown(adapter.dispose);

      // Launch multiple concurrent transactions, each enqueuing 2 operations.
      final futures = List.generate(
        5,
        (i) => adapter.runInTransaction((outbox) async {
          await outbox.enqueue(
            LocalMutation(
              entityType: 'tasks',
              entityId: 't-$i-a',
              type: SyncOperationType.create,
              payload: const <String, dynamic>{'id': 'a'},
            ),
          );
          await outbox.enqueue(
            LocalMutation(
              entityType: 'tasks',
              entityId: 't-$i-b',
              type: SyncOperationType.create,
              payload: const <String, dynamic>{'id': 'b'},
            ),
          );
          return i;
        }),
      );

      await Future.wait(futures);

      // All 10 operations should be present.
      final ops = await adapter.fetchEligibleOperations(
        now: DateTime.now().toUtc().add(const Duration(minutes: 1)),
        limit: 20,
      );
      expect(ops, hasLength(10));

      // Sequence numbers should be unique.
      final seqNumbers = ops.map((op) => op.sequenceNumber).toSet();
      expect(seqNumbers, hasLength(10));
    });

    test('concurrent state transitions are serialized', () async {
      var counter = 0;
      final adapter = HiveLocalStorageAdapter(
        box: box,
        operationIdGenerator: () => 'op-state-${++counter}',
      );
      addTearDown(adapter.dispose);

      // Enqueue an operation.
      final op = await adapter.runInTransaction(
        (outbox) => outbox.enqueue(
          LocalMutation(
            entityType: 'tasks',
            entityId: 't-state',
            type: SyncOperationType.create,
            payload: const <String, dynamic>{'id': 't-state'},
          ),
        ),
      );

      // Concurrently mark in different states.
      await Future.wait([
        adapter.markInFlight(op.operationId),
        adapter.markFailed(
          op.operationId,
          attemptCount: 1,
          nextRetryAt: DateTime.now().toUtc().add(const Duration(minutes: 1)),
          error: SyncError(
            code: SyncErrorCode.network,
            message: 'Test',
            isRetryable: true,
          ),
        ),
        adapter.markDeadLetter(
          op.operationId,
          attemptCount: 3,
          error: SyncError(
            code: SyncErrorCode.validation,
            message: 'Test',
            isRetryable: false,
          ),
        ),
      ]);

      // The final state depends on which mutation ran last.
      // All mutations should have completed without error.
      // The operation should still exist in the snapshot (not succeeded).
      final snapshotJson = box.get(HiveOutboxSchema.snapshotKey);
      expect(snapshotJson, isNotNull);
      final snapshot = HiveOutboxSnapshot.fromJson(snapshotJson!);
      expect(snapshot.operations, hasLength(1));
      expect(snapshot.operations.single.operationId, op.operationId);
      // Status should be one of the concurrent states.
      expect(
        snapshot.operations.single.status,
        anyOf(
          SyncOperationStatus.inFlight,
          SyncOperationStatus.failed,
          SyncOperationStatus.deadLetter,
        ),
      );
    });

    test('FIFO ordering preserved after concurrent writes', () async {
      var counter = 0;
      final adapter = HiveLocalStorageAdapter(
        box: box,
        operationIdGenerator: () => 'op-fifo-${++counter}',
        clock: () =>
            DateTime.utc(2026, 6, 1, 10).add(Duration(seconds: counter)),
      );
      addTearDown(adapter.dispose);

      // Concurrently enqueue operations for the same entity.
      final futures = List.generate(
        5,
        (i) => adapter.outbox.enqueue(
          LocalMutation(
            entityType: 'tasks',
            entityId: 't-fifo',
            type: SyncOperationType.update,
            payload: <String, dynamic>{'seq': i},
          ),
        ),
      );

      final results = await Future.wait(futures);

      // Only the first operation should be eligible (FIFO).
      final eligible = await adapter.fetchEligibleOperations(
        now: DateTime.utc(2026, 6, 1, 11),
        limit: 10,
      );
      expect(eligible, hasLength(1));
      expect(eligible.single.sequenceNumber, results.first.sequenceNumber);
    });
  });

  group('Edge cases', () {
    test('markSucceeded is idempotent for non-existent operation', () async {
      final adapter = HiveLocalStorageAdapter(box: box);
      addTearDown(adapter.dispose);

      // Should not throw.
      await adapter.markSucceeded('non-existent');
      await adapter.markSucceeded('non-existent');
    });

    test('markInFlight on non-existent operation is a no-op', () async {
      final adapter = HiveLocalStorageAdapter(box: box);
      addTearDown(adapter.dispose);

      // Should not throw.
      await adapter.markInFlight('non-existent');
    });

    test('markFailed on non-existent operation is a no-op', () async {
      final adapter = HiveLocalStorageAdapter(box: box);
      addTearDown(adapter.dispose);

      // Should not throw.
      await adapter.markFailed(
        'non-existent',
        attemptCount: 1,
        nextRetryAt: DateTime.now().toUtc(),
        error: SyncError(
          code: SyncErrorCode.network,
          message: 'Test',
          isRetryable: true,
        ),
      );
    });

    test('markDeadLetter on non-existent operation is a no-op', () async {
      final adapter = HiveLocalStorageAdapter(box: box);
      addTearDown(adapter.dispose);

      // Should not throw.
      await adapter.markDeadLetter(
        'non-existent',
        attemptCount: 1,
        error: SyncError(
          code: SyncErrorCode.validation,
          message: 'Test',
          isRetryable: false,
        ),
      );
    });
  });
}
