import 'dart:io';

import 'package:flutter_test/flutter_test.dart';
import 'package:local_remote_sqflite/local_remote_sqflite.dart';
import 'package:local_remote_synchronization/local_remote_synchronization.dart';
import 'package:sqflite_common_ffi/sqflite_ffi.dart';

void main() {
  sqfliteFfiInit();
  final databaseFactory = databaseFactoryFfi;

  late Directory tempDir;
  late String dbPath;

  Future<Database> openTestDb() => databaseFactory.openDatabase(
        dbPath,
        options: OpenDatabaseOptions(
          version: 1,
          onCreate: (db, _) async {
            await db.execute('''
              CREATE TABLE tasks (
                id TEXT PRIMARY KEY,
                title TEXT NOT NULL
              )
            ''');
            await SqfliteOutboxSchema.createSchema(db);
          },
        ),
      );

  setUp(() async {
    tempDir = await Directory.systemTemp.createTemp('sqflite_outbox_test_');
    dbPath = '${tempDir.path}/test.db';
  });

  tearDown(() async {
    if (tempDir.existsSync()) {
      await tempDir.delete(recursive: true);
    }
  });

  group('SqfliteOutboxSchema', () {
    test('rejects invalid tableName identifiers to prevent SQL injection',
        () async {
      final db = await openTestDb();
      addTearDown(db.close);

      expect(
        () => SqfliteLocalStorageAdapter(
          database: db,
          tableName: 'sync_outbox; DROP TABLE tasks',
        ),
        throwsArgumentError,
      );
      expect(
        () => SqfliteLocalStorageAdapter(
          database: db,
          tableName: '123_invalid',
        ),
        throwsArgumentError,
      );
    });

    test('ensureInitialized is idempotent when called repeatedly', () async {
      final db = await openTestDb();
      addTearDown(db.close);

      final adapter = SqfliteLocalStorageAdapter(database: db);
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
    test('persists and restores all fields across database close and reopen',
        () async {
      final fixedNow = DateTime.utc(2026, 6, 1, 10);
      final occurredLocal = DateTime(2026, 6, 1, 9, 30);

      var db = await openTestDb();
      var adapter = SqfliteLocalStorageAdapter(
        database: db,
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
      await db.close();

      // Reopen database and read back persisted operation.
      db = await openTestDb();
      adapter = SqfliteLocalStorageAdapter(database: db);
      addTearDown(() async {
        await adapter.dispose();
        await db.close();
      });

      final fetched = await adapter.fetchEligibleOperations(
        now: fixedNow,
        limit: 10,
      );
      expect(fetched, hasLength(1));
      expect(fetched.single, equals(enqueued));
      expect(fetched.single.operationId, 'op-persist-1');
      expect(fetched.single.idempotencyKey, 'custom-idem-1');
      expect(fetched.single.createdAt.isUtc, isTrue);
      expect(fetched.single.sequenceNumber, isNotNull);
    });

    test('preserves nullable fields for delete operations', () async {
      final db = await openTestDb();
      final adapter = SqfliteLocalStorageAdapter(
        database: db,
        operationIdGenerator: () => 'op-del-1',
        clock: () => DateTime.utc(2026, 6, 1, 12),
      );
      addTearDown(() async {
        await adapter.dispose();
        await db.close();
      });

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
      final db = await openTestDb();
      final adapter = SqfliteLocalStorageAdapter(
        database: db,
        operationIdGenerator: () => 'op-${++counter}',
        clock: () => t0.add(Duration(seconds: counter)),
      );
      addTearDown(() async {
        await adapter.dispose();
        await db.close();
      });

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
      var db = await openTestDb();
      var adapter = SqfliteLocalStorageAdapter(
        database: db,
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
      await db.close();

      // Simulate restart.
      db = await openTestDb();
      adapter = SqfliteLocalStorageAdapter(
        database: db,
        clock: () => t0.add(const Duration(seconds: 5)),
      );
      addTearDown(() async {
        await adapter.dispose();
        await db.close();
      });

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
      final db = await openTestDb();
      var counter = 0;
      final adapter = SqfliteLocalStorageAdapter(
        database: db,
        operationIdGenerator: () => 'op-watch-${++counter}',
      );
      addTearDown(() async {
        await adapter.dispose();
        await db.close();
      });

      final emitted = <int>[];
      final sub = adapter.watchPendingCount().listen(emitted.add);
      addTearDown(sub.cancel);

      await Future<void>.delayed(const Duration(milliseconds: 20));
      expect(emitted, <int>[0]);

      final op = await adapter.runInTransaction(
        (outbox) => outbox.enqueue(
          LocalMutation(
            entityType: 'tasks',
            entityId: 't-1',
            type: SyncOperationType.delete,
          ),
        ),
      );
      await Future<void>.delayed(const Duration(milliseconds: 20));
      expect(emitted, <int>[0, 1]);

      await adapter.markSucceeded(op.operationId);
      await Future<void>.delayed(const Duration(milliseconds: 20));
      expect(emitted, <int>[0, 1, 0]);
    });
  });

  group('Atomicity', () {
    test(
        'domain write and outbox enqueue commit together in runWithTransaction',
        () async {
      final db = await openTestDb();
      final adapter = SqfliteLocalStorageAdapter(
        database: db,
        operationIdGenerator: () => 'op-atom-1',
      );
      addTearDown(() async {
        await adapter.dispose();
        await db.close();
      });

      await adapter.runWithTransaction((txn, outbox) async {
        await txn.insert('tasks', <String, Object?>{
          'id': 't-1',
          'title': 'Atomic task',
        });
        await outbox.enqueue(
          LocalMutation(
            entityType: 'tasks',
            entityId: 't-1',
            type: SyncOperationType.create,
            payload: const <String, dynamic>{'id': 't-1', 'title': 'Atomic'},
          ),
        );
      });

      final tasks = await db.query('tasks');
      final ops = await adapter.fetchEligibleOperations(
        now: DateTime.now().toUtc().add(const Duration(minutes: 1)),
        limit: 10,
      );
      expect(tasks, hasLength(1));
      expect(ops, hasLength(1));
    });

    test('outbox failure rolls back domain write', () async {
      final db = await openTestDb();
      // Always returns the same operationId to trigger UNIQUE violation on 2nd.
      final adapter = SqfliteLocalStorageAdapter(
        database: db,
        operationIdGenerator: () => 'duplicate-op-id',
      );
      addTearDown(() async {
        await adapter.dispose();
        await db.close();
      });

      await adapter.runWithTransaction((txn, outbox) async {
        await txn.insert('tasks', <String, Object?>{
          'id': 't-1',
          'title': 'First',
        });
        await outbox.enqueue(
          LocalMutation(
            entityType: 'tasks',
            entityId: 't-1',
            type: SyncOperationType.create,
            payload: const <String, dynamic>{'id': 't-1'},
          ),
        );
      });

      await expectLater(
        () => adapter.runWithTransaction((txn, outbox) async {
          await txn.insert('tasks', <String, Object?>{
            'id': 't-2',
            'title': 'Should roll back',
          });
          await outbox.enqueue(
            LocalMutation(
              entityType: 'tasks',
              entityId: 't-2',
              type: SyncOperationType.create,
              payload: const <String, dynamic>{'id': 't-2'},
            ),
          );
        }),
        throwsA(isA<DatabaseException>()),
      );

      final tasks = await db.query('tasks');
      expect(tasks, hasLength(1));
      expect(tasks.single['id'], 't-1');
    });

    test('domain write failure rolls back outbox enqueue', () async {
      final db = await openTestDb();
      final adapter = SqfliteLocalStorageAdapter(
        database: db,
        operationIdGenerator: () => 'op-rollback-1',
      );
      addTearDown(() async {
        await adapter.dispose();
        await db.close();
      });

      await expectLater(
        () => adapter.runWithTransaction((txn, outbox) async {
          await outbox.enqueue(
            LocalMutation(
              entityType: 'tasks',
              entityId: 't-1',
              type: SyncOperationType.create,
              payload: const <String, dynamic>{'id': 't-1'},
            ),
          );
          // Violate NOT NULL constraint on tasks.title.
          await txn.insert('tasks', <String, Object?>{
            'id': 't-1',
            'title': null,
          });
        }),
        throwsA(isA<DatabaseException>()),
      );

      final ops = await adapter.fetchEligibleOperations(
        now: DateTime.now().toUtc().add(const Duration(minutes: 1)),
        limit: 10,
      );
      expect(ops, isEmpty);
    });
  });

  group('Edge cases', () {
    test('throws descriptive FormatException on corrupted outbox row',
        () async {
      final db = await openTestDb();
      final adapter = SqfliteLocalStorageAdapter(database: db);
      addTearDown(() async {
        await adapter.dispose();
        await db.close();
      });

      await db.insert('sync_outbox', <String, Object?>{
        'operation_id': 'corrupt-1',
        'idempotency_key': 'corrupt-1',
        'entity_type': 'tasks',
        'entity_id': 't-1',
        'operation_type': 'create',
        'payload_json': '{not valid json',
        'metadata_json': '{}',
        'status': 'pending',
        'attempt_count': 0,
        'created_at_ms': 1000,
        'updated_at_ms': 1000,
      });

      await expectLater(
        () => adapter.fetchEligibleOperations(
          now: DateTime.utc(2026),
          limit: 10,
        ),
        throwsA(
          isA<FormatException>().having(
            (e) => e.message,
            'message',
            contains('corrupt-1'),
          ),
        ),
      );
    });

    test('throws DatabaseException when database is closed', () async {
      final db = await openTestDb();
      final adapter = SqfliteLocalStorageAdapter(database: db);
      await adapter.dispose();
      await db.close();

      await expectLater(
        () => adapter.fetchEligibleOperations(
          now: DateTime.utc(2026),
          limit: 10,
        ),
        throwsA(isA<DatabaseException>()),
      );
    });
  });
}
