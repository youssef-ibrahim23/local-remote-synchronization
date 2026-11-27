import 'dart:async';
import 'dart:io';

import 'package:hive/hive.dart';
import 'package:local_remote_hive/local_remote_hive.dart';
import 'package:local_remote_synchronization/local_remote_synchronization.dart';
import 'package:test/test.dart';

void main() {
  late Directory tempDir;
  late Box<String> box;
  late Box<int> metadataBox;

  Future<void> openBoxes() async {
    Hive.init(tempDir.path);
    box = await Hive.openBox<String>('sync_outbox');
    metadataBox = await Hive.openBox<int>('sync_outbox_meta');
  }

  setUp(() async {
    tempDir = await Directory.systemTemp.createTemp('hive_outbox_test_');
    await openBoxes();
  });

  tearDown(() async {
    await box.close();
    await metadataBox.close();
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
          metadataBox: metadataBox,
          boxName: 'Invalid Name!',
        ),
        throwsArgumentError,
      );
      expect(
        () => HiveLocalStorageAdapter(
          box: box,
          metadataBox: metadataBox,
          boxName: '123_invalid',
        ),
        throwsArgumentError,
      );
      expect(
        () => HiveLocalStorageAdapter(
          box: box,
          metadataBox: metadataBox,
          boxName: 'valid_box_name',
        ),
        returnsNormally,
      );
    });

    test('ensureInitialized is idempotent', () async {
      final adapter = HiveLocalStorageAdapter(
        box: box,
        metadataBox: metadataBox,
      );
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

  group('Persistence & Serialization', () {
    test('persists and restores all fields', () async {
      final fixedNow = DateTime.utc(2026, 6, 1, 10);
      final occurredLocal = DateTime(2026, 6, 1, 9, 30);

      var adapter = HiveLocalStorageAdapter(
        box: box,
        metadataBox: metadataBox,
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
      await metadataBox.close();

      // Reopen boxes and verify persistence.
      await openBoxes();
      adapter = HiveLocalStorageAdapter(
        box: box,
        metadataBox: metadataBox,
      );
      addTearDown(() async {
        await adapter.dispose();
      });

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
        metadataBox: metadataBox,
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
  });

  group('Per-Entity FIFO', () {
    test('blocks later operations for same entity across all blocking statuses',
        () async {
      final t0 = DateTime.utc(2026, 6, 1, 10);
      var counter = 0;
      final adapter = HiveLocalStorageAdapter(
        box: box,
        metadataBox: metadataBox,
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
        metadataBox: metadataBox,
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
      await metadataBox.close();

      // Simulate restart.
      await openBoxes();
      adapter = HiveLocalStorageAdapter(
        box: box,
        metadataBox: metadataBox,
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
        metadataBox: metadataBox,
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
        metadataBox: metadataBox,
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
      final adapter = HiveLocalStorageAdapter(
        box: box,
        metadataBox: metadataBox,
      );
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
        metadataBox: metadataBox,
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
        metadataBox: metadataBox,
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
      // Force failed status with NULL next_retry_at.
      final nullOpJson = box.get(nullRetryOp.operationId);
      if (nullOpJson != null) {
        final nullOp = HiveOutboxMapper.fromJson(nullOpJson);
        final updated = nullOp.copyWith(
          status: SyncOperationStatus.failed,
          clearNextRetryAt: true,
        );
        await box.put(
          nullRetryOp.operationId,
          HiveOutboxMapper.toJson(updated),
        );
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
        metadataBox: metadataBox,
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
        metadataBox: metadataBox,
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

  group('Edge cases', () {
    test('throws FormatException on corrupted outbox entry', () async {
      final adapter = HiveLocalStorageAdapter(
        box: box,
        metadataBox: metadataBox,
      );
      addTearDown(adapter.dispose);

      // Write corrupted JSON directly to the box.
      await box.put('corrupt-1', '{not valid json');

      // fetchEligibleOperations should skip corrupted entries.
      final ops = await adapter.fetchEligibleOperations(
        now: DateTime.utc(2026),
        limit: 10,
      );
      expect(ops, isEmpty);
    });

    test('markSucceeded is idempotent for non-existent operation', () async {
      final adapter = HiveLocalStorageAdapter(
        box: box,
        metadataBox: metadataBox,
      );
      addTearDown(adapter.dispose);

      // Should not throw.
      await adapter.markSucceeded('non-existent');
      await adapter.markSucceeded('non-existent');
    });

    test('markInFlight on non-existent operation is a no-op', () async {
      final adapter = HiveLocalStorageAdapter(
        box: box,
        metadataBox: metadataBox,
      );
      addTearDown(adapter.dispose);

      // Should not throw.
      await adapter.markInFlight('non-existent');
    });
  });
}
