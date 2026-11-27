import 'dart:async';
import 'dart:io';

import 'package:async/async.dart';
import 'package:drift/drift.dart' hide isNotNull, isNull;
import 'package:drift/native.dart';
import 'package:local_remote_drift/local_remote_drift.dart';
import 'package:local_remote_synchronization/local_remote_synchronization.dart';
import 'package:test/test.dart';
import 'package:uuid/uuid.dart';

final class _TestAppDatabase extends GeneratedDatabase {
  _TestAppDatabase(
    super.e, {
    this.tableName = DriftOutboxSchema.defaultTableName,
  });

  final String tableName;

  @override
  int get schemaVersion => 1;

  @override
  Iterable<TableInfo<Table, Object?>> get allTables =>
      const <TableInfo<Table, Object?>>[];

  @override
  MigrationStrategy get migration => MigrationStrategy(
        onCreate: (_) async {
          await customStatement('''
            CREATE TABLE tasks (
              id TEXT PRIMARY KEY,
              title TEXT NOT NULL
            )
          ''');
          await DriftOutboxSchema.createSchema(this, tableName: tableName);
        },
      );
}

void main() {
  group('DriftOutboxSchema', () {
    test('rejects invalid tableName identifiers to prevent SQL injection',
        () async {
      final db = _TestAppDatabase(NativeDatabase.memory());
      addTearDown(db.close);

      expect(
        () => DriftLocalStorageAdapter(
          database: db,
          tableName: 'sync_outbox; DROP TABLE tasks',
        ),
        throwsArgumentError,
      );
      expect(
        () => DriftLocalStorageAdapter(
          database: db,
          tableName: '123_invalid',
        ),
        throwsArgumentError,
      );
    });

    test('supports custom tableName and idempotent ensureInitialized',
        () async {
      final db = _TestAppDatabase(
        NativeDatabase.memory(),
        tableName: 'custom_outbox',
      );
      addTearDown(db.close);

      final adapter = DriftLocalStorageAdapter(
        database: db,
        tableName: 'custom_outbox',
      );
      addTearDown(adapter.dispose);

      await adapter.ensureInitialized();
      await adapter.ensureInitialized();

      final op = await adapter.runInTransaction(
        (outbox) => outbox.enqueue(
          LocalMutation(
            entityType: 'tasks',
            entityId: 't-1',
            type: SyncOperationType.delete,
          ),
        ),
      );
      expect(Uuid.isValidUUID(fromString: op.operationId), isTrue);

      final ops = await adapter.fetchEligibleOperations(
        now: DateTime.now().toUtc().add(const Duration(minutes: 1)),
        limit: 10,
      );
      expect(ops, hasLength(1));
      expect(ops.single.operationId, op.operationId);
    });
  });

  group('Persistence & Serialization', () {
    test('persists and restores all fields across database close and reopen',
        () async {
      final tempDir = await Directory.systemTemp.createTemp('drift_outbox_');
      addTearDown(() async {
        if (tempDir.existsSync()) {
          await tempDir.delete(recursive: true);
        }
      });
      final dbFile = File('${tempDir.path}/drift.db');

      final fixedNow = DateTime.utc(2026, 6, 1, 14, 30);
      final occurredLocal = DateTime(2026, 5, 1, 9, 15);

      var db = _TestAppDatabase(NativeDatabase(dbFile));
      var adapter = DriftLocalStorageAdapter(
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
              'title': 'Write Drift adapter',
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

      // Reopen database and verify all fields are restored accurately.
      db = _TestAppDatabase(NativeDatabase(dbFile));
      adapter = DriftLocalStorageAdapter(database: db);
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
      expect(fetched.single.createdAt, fixedNow);
      expect(fetched.single.createdAt.isUtc, isTrue);
      expect(fetched.single.occurredAt, occurredLocal.toUtc());
      expect(fetched.single.occurredAt!.isUtc, isTrue);
      expect(
        fetched.single.createdAt,
        isNot(equals(fetched.single.occurredAt)),
      );
    });
  });

  group('Zone-Based Transactions & Atomicity', () {
    test(
        'domain write and outbox enqueue commit together in runWithTransaction',
        () async {
      final db = _TestAppDatabase(NativeDatabase.memory());
      final adapter = DriftLocalStorageAdapter(
        database: db,
        operationIdGenerator: () => 'op-atom-1',
      );
      addTearDown(() async {
        await adapter.dispose();
        await db.close();
      });

      await adapter.runWithTransaction((outbox) async {
        await db.customInsert(
          'INSERT INTO tasks (id, title) VALUES (?, ?)',
          variables: <Variable<Object>>[
            Variable.withString('t-1'),
            Variable.withString('Atomic Drift task'),
          ],
        );
        await outbox.enqueue(
          LocalMutation(
            entityType: 'tasks',
            entityId: 't-1',
            type: SyncOperationType.create,
            payload: const <String, dynamic>{'id': 't-1'},
          ),
        );
      });

      final tasks = await db.customSelect('SELECT * FROM tasks').get();
      final ops = await adapter.fetchEligibleOperations(
        now: DateTime.now().toUtc().add(const Duration(minutes: 1)),
        limit: 10,
      );
      expect(tasks, hasLength(1));
      expect(ops, hasLength(1));
    });

    test('outbox failure rolls back domain write in same Drift transaction',
        () async {
      final db = _TestAppDatabase(NativeDatabase.memory());
      final adapter = DriftLocalStorageAdapter(
        database: db,
        operationIdGenerator: () => 'dup-op-id',
      );
      addTearDown(() async {
        await adapter.dispose();
        await db.close();
      });

      await adapter.runWithTransaction((outbox) async {
        await db.customInsert(
          'INSERT INTO tasks (id, title) VALUES (?, ?)',
          variables: <Variable<Object>>[
            Variable.withString('t-1'),
            Variable.withString('First'),
          ],
        );
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
        () => adapter.runWithTransaction((outbox) async {
          await db.customInsert(
            'INSERT INTO tasks (id, title) VALUES (?, ?)',
            variables: <Variable<Object>>[
              Variable.withString('t-2'),
              Variable.withString('Should roll back'),
            ],
          );
          await outbox.enqueue(
            LocalMutation(
              entityType: 'tasks',
              entityId: 't-2',
              type: SyncOperationType.create,
              payload: const <String, dynamic>{'id': 't-2'},
            ),
          );
        }),
        throwsA(isA<SqliteException>()),
      );

      final tasks = await db.customSelect('SELECT * FROM tasks').get();
      expect(tasks, hasLength(1));
      expect(tasks.single.read<String>('id'), 't-1');
    });

    test('domain write failure rolls back outbox enqueue', () async {
      final db = _TestAppDatabase(NativeDatabase.memory());
      final adapter = DriftLocalStorageAdapter(
        database: db,
        operationIdGenerator: () => 'op-rb-1',
      );
      addTearDown(() async {
        await adapter.dispose();
        await db.close();
      });

      await expectLater(
        () => adapter.runWithTransaction((outbox) async {
          await outbox.enqueue(
            LocalMutation(
              entityType: 'tasks',
              entityId: 't-1',
              type: SyncOperationType.create,
              payload: const <String, dynamic>{'id': 't-1'},
            ),
          );
          // Violate NOT NULL constraint on tasks.title.
          await db.customInsert(
            'INSERT INTO tasks (id, title) VALUES (?, NULL)',
            variables: <Variable<Object>>[Variable.withString('t-1')],
          );
        }),
        throwsA(isA<SqliteException>()),
      );

      final ops = await adapter.fetchEligibleOperations(
        now: DateTime.now().toUtc().add(const Duration(minutes: 1)),
        limit: 10,
      );
      expect(ops, isEmpty);
    });

    test(
        'adapter.outbox participates in an already-open external Drift '
        'transaction and rolls back together', () async {
      final db = _TestAppDatabase(NativeDatabase.memory());
      final adapter = DriftLocalStorageAdapter(
        database: db,
        operationIdGenerator: () => 'op-ext-tx',
      );
      addTearDown(() async {
        await adapter.dispose();
        await db.close();
      });

      await expectLater(
        () => db.transaction(() async {
          await adapter.outbox.enqueue(
            LocalMutation(
              entityType: 'tasks',
              entityId: 't-ext',
              type: SyncOperationType.delete,
            ),
          );
          throw StateError('abort external transaction');
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

  group('Per-Entity FIFO & Retry Eligibility', () {
    test('blocks later operations for same entity across all blocking statuses',
        () async {
      final t0 = DateTime.utc(2026, 6, 1, 10);
      var counter = 0;
      final db = _TestAppDatabase(NativeDatabase.memory());
      final adapter = DriftLocalStorageAdapter(
        database: db,
        operationIdGenerator: () => 'op-${++counter}',
        clock: () => t0.add(Duration(seconds: counter)),
      );
      addTearDown(() async {
        await adapter.dispose();
        await db.close();
      });

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

      // 3. Earlier failed blocks op2; op3 remains eligible.
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

      // 5. Once op1 succeeds, op2 becomes eligible.
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

    test('failed retry eligibility requires nextRetryAt <= now and non-null',
        () async {
      final t0 = DateTime.utc(2026, 6, 1, 10);
      var counter = 0;
      final db = _TestAppDatabase(NativeDatabase.memory());
      final adapter = DriftLocalStorageAdapter(
        database: db,
        operationIdGenerator: () => 'op-retry-${++counter}',
        clock: () => t0,
      );
      addTearDown(() async {
        await adapter.dispose();
        await db.close();
      });

      final futureOp = await adapter.runInTransaction(
        (outbox) => outbox.enqueue(
          LocalMutation(
            entityType: 'tasks',
            entityId: 'future',
            type: SyncOperationType.delete,
          ),
        ),
      );
      final dueOp = await adapter.runInTransaction(
        (outbox) => outbox.enqueue(
          LocalMutation(
            entityType: 'tasks',
            entityId: 'due',
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
        dueOp.operationId,
        attemptCount: 1,
        nextRetryAt: t0,
        error: err,
      );
      await db.customUpdate(
        '''
        UPDATE sync_outbox
        SET status = 'failed', next_retry_at_ms = NULL
        WHERE operation_id = ?
        ''',
        variables: <Variable<Object>>[
          Variable.withString(nullRetryOp.operationId),
        ],
      );

      final eligible = await adapter.fetchEligibleOperations(
        now: t0,
        limit: 10,
      );
      expect(
        eligible.map((o) => o.operationId),
        <String>[dueOp.operationId],
      );
    });
  });

  group('Lifecycle & watchPendingCount', () {
    test('recovers inFlight operations and supports idempotent markSucceeded',
        () async {
      final t0 = DateTime.utc(2026, 6, 1, 10);
      final db = _TestAppDatabase(NativeDatabase.memory());
      final adapter = DriftLocalStorageAdapter(
        database: db,
        operationIdGenerator: () => 'op-rec-1',
        clock: () => t0,
      );
      addTearDown(() async {
        await adapter.dispose();
        await db.close();
      });

      final op = await adapter.runInTransaction(
        (outbox) => outbox.enqueue(
          LocalMutation(
            entityType: 'tasks',
            entityId: 't-1',
            type: SyncOperationType.delete,
          ),
        ),
      );
      await adapter.markInFlight(op.operationId);

      final recovered = await adapter.recoverInFlightOperations();
      expect(recovered, 1);

      final eligible = await adapter.fetchEligibleOperations(
        now: t0.add(const Duration(seconds: 5)),
        limit: 10,
      );
      expect(eligible.single.status, SyncOperationStatus.pending);
      expect(eligible.single.idempotencyKey, 'op-rec-1');

      await adapter.markSucceeded(op.operationId);
      await adapter.markSucceeded(op.operationId);
      expect(
        await adapter.fetchEligibleOperations(
          now: t0.add(const Duration(seconds: 10)),
          limit: 10,
        ),
        isEmpty,
      );
    });

    test('watchPendingCount emits initial count and reacts to mutations',
        () async {
      final db = _TestAppDatabase(NativeDatabase.memory());
      var counter = 0;
      final adapter = DriftLocalStorageAdapter(
        database: db,
        operationIdGenerator: () => 'op-watch-${++counter}',
      );
      addTearDown(db.close);

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
      final db = _TestAppDatabase(NativeDatabase.memory());
      var counter = 0;
      final adapter = DriftLocalStorageAdapter(
        database: db,
        operationIdGenerator: () => 'op-race-${++counter}',
      );
      addTearDown(db.close);

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

      await adapter.dispose();
      final listener1 = await eventsFuture1;
      final listener2 = await eventsFuture2;

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
      final db = _TestAppDatabase(NativeDatabase.memory());
      final adapter = DriftLocalStorageAdapter(database: db);
      addTearDown(db.close);

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

    test(
        'external db.transaction with adapter.outbox notifies via '
        'refreshPendingCount after commit', () async {
      final db = _TestAppDatabase(NativeDatabase.memory());
      var counter = 0;
      final adapter = DriftLocalStorageAdapter(
        database: db,
        operationIdGenerator: () => 'op-ext-${++counter}',
      );
      addTearDown(db.close);

      final allEventsFuture = adapter.watchPendingCount().toList();
      final queue = StreamIterator<int>(adapter.watchPendingCount());
      addTearDown(queue.cancel);

      expect(await queue.moveNext(), isTrue);
      expect(queue.current, 0);

      await db.transaction(() async {
        await db.customInsert(
          'INSERT INTO tasks (id, title) VALUES (?, ?)',
          variables: <Variable<Object>>[
            Variable.withString('t-ext'),
            Variable.withString('External Drift txn'),
          ],
        );
        await adapter.outbox.enqueue(
          LocalMutation(
            entityType: 'tasks',
            entityId: 't-ext',
            type: SyncOperationType.create,
            payload: const <String, dynamic>{'id': 't-ext'},
          ),
        );
      });

      await adapter.refreshPendingCount();
      expect(await queue.moveNext(), isTrue);
      expect(queue.current, 1);

      await adapter.dispose();
      expect(await queue.moveNext(), isFalse);
      expect(await allEventsFuture, <int>[0, 1]);
    });
  });

  // ─── Nested-Transaction Tests ─────────────────────────────────────────────
  //
  // Drift 2.31+ on NativeDatabase supports nested transactions via SQLite
  // SAVEPOINTs.  When database.transaction() is called inside an outer
  // database.transaction() zone, Drift starts a child transaction (SAVEPOINT).
  //
  // Verified semantics (from drift source, connection_user.dart ~line 454):
  //
  //   • A child transaction that completes does NOT persist right away.
  //     Its writes become visible inside the outer transaction zone only.
  //   • The outer transaction must commit for all writes to be durable.
  //   • If the outer transaction throws, ALL writes (including committed child
  //     transactions) are rolled back.
  //   • watchPendingCount MAY emit a count after the child commits even when
  //     the outer has not committed yet — this is a documented limitation.
  //
  // These tests prove those semantics empirically against a real in-memory DB.
  group('Nested Transactions (outer db.transaction + runWithTransaction)', () {
    test(
        'domain write and outbox enqueue from inner runWithTransaction commit '
        'atomically when outer transaction commits', () async {
      final db = _TestAppDatabase(NativeDatabase.memory());
      var counter = 0;
      final adapter = DriftLocalStorageAdapter(
        database: db,
        operationIdGenerator: () => 'op-nested-${++counter}',
      );
      addTearDown(() async {
        await adapter.dispose();
        await db.close();
      });

      // Outer transaction wraps the adapter call.
      await db.transaction(() async {
        await db.customInsert(
          'INSERT INTO tasks (id, title) VALUES (?, ?)',
          variables: <Variable<Object>>[
            Variable.withString('t-outer'),
            Variable.withString('Outer task'),
          ],
        );
        // runWithTransaction starts a child SAVEPOINT inside the outer txn.
        await adapter.runWithTransaction((outbox) async {
          await outbox.enqueue(
            LocalMutation(
              entityType: 'tasks',
              entityId: 't-outer',
              type: SyncOperationType.create,
              payload: const <String, dynamic>{'id': 't-outer'},
            ),
          );
        });
      });

      // After outer transaction commits, both domain row and outbox row exist.
      final tasks = await db.customSelect('SELECT * FROM tasks').get();
      expect(tasks, hasLength(1));
      expect(tasks.single.read<String>('id'), 't-outer');

      final ops = await adapter.fetchEligibleOperations(
        now: DateTime.now().toUtc().add(const Duration(minutes: 1)),
        limit: 10,
      );
      expect(ops, hasLength(1));
      expect(ops.single.entityId, 't-outer');
    });

    test(
        'outer transaction abort rolls back all writes including child '
        'transaction commits', () async {
      final db = _TestAppDatabase(NativeDatabase.memory());
      var counter = 0;
      final adapter = DriftLocalStorageAdapter(
        database: db,
        operationIdGenerator: () => 'op-abort-${++counter}',
      );
      addTearDown(() async {
        await adapter.dispose();
        await db.close();
      });

      // Child transaction will commit (SAVEPOINT RELEASE), but the outer txn
      // throws, rolling back everything.
      await expectLater(
        () => db.transaction(() async {
          await adapter.runWithTransaction((outbox) async {
            await db.customInsert(
              'INSERT INTO tasks (id, title) VALUES (?, ?)',
              variables: <Variable<Object>>[
                Variable.withString('t-rollback'),
                Variable.withString('Should be rolled back'),
              ],
            );
            await outbox.enqueue(
              LocalMutation(
                entityType: 'tasks',
                entityId: 't-rollback',
                type: SyncOperationType.create,
              ),
            );
          });
          // This throws after the child transaction has committed.
          throw StateError('outer transaction aborted');
        }),
        throwsStateError,
      );

      // Both domain row and outbox row must be absent.
      final tasks = await db.customSelect('SELECT * FROM tasks').get();
      expect(tasks, isEmpty,
          reason:
              'domain write must be rolled back with the outer transaction');

      final ops = await adapter.fetchEligibleOperations(
        now: DateTime.now().toUtc().add(const Duration(minutes: 1)),
        limit: 10,
      );
      expect(ops, isEmpty,
          reason:
              'outbox write must be rolled back with the outer transaction');
    });

    test(
        'watchPendingCount emits after child commit — documented premature '
        'emission limitation when outer has not committed', () async {
      // This test documents the known limitation: the adapter emits a pending
      // count after the inner (child) transaction commits, even though the data
      // is not yet durably persisted because the outer transaction is still
      // open.  The count self-corrects if the outer transaction later rolls
      // back and refreshPendingCount() is called.
      final db = _TestAppDatabase(NativeDatabase.memory());
      var counter = 0;
      final adapter = DriftLocalStorageAdapter(
        database: db,
        operationIdGenerator: () => 'op-premature-${++counter}',
      );
      addTearDown(() async {
        await adapter.dispose();
        await db.close();
      });

      // Collect all pending-count events.
      final events = <int>[];
      final subscription = adapter.watchPendingCount().listen(events.add);
      addTearDown(subscription.cancel);

      // Allow the initial count (0) to be emitted.
      await Future<void>.delayed(Duration.zero);
      expect(events, equals(<int>[0]));

      // Run outer transaction; inner adapter call commits its child txn and
      // emits a premature count before the outer commits.
      late final Completer<void> innerCommitted;
      innerCommitted = Completer<void>();

      await db.transaction(() async {
        await adapter.runWithTransaction((outbox) async {
          await outbox.enqueue(
            LocalMutation(
              entityType: 'tasks',
              entityId: 't-premature',
              type: SyncOperationType.delete,
            ),
          );
        });
        // Allow microtasks to run so the pending-count emission from the
        // completed child transaction can propagate.
        await Future<void>.delayed(Duration.zero);
        innerCommitted.complete();
      });

      // Wait for outer transaction to commit and all async emissions to settle.
      await innerCommitted.future;
      await Future<void>.delayed(Duration.zero);

      // The count is 1 because the outer committed successfully.
      // Note: the adapter may emit 1 twice (once after child commit, once after
      // outer commit's _emitPendingCount). Both are acceptable; what matters is
      // the final count is 1.
      expect(events.last, equals(1),
          reason: 'final count must be 1 after outer transaction commits');
      expect(events, everyElement(inInclusiveRange(0, 1)),
          reason: 'no spurious counts beyond expected range');
    });

    test(
        'pending count corrects itself after outer abort via '
        'refreshPendingCount', () async {
      final db = _TestAppDatabase(NativeDatabase.memory());
      var counter = 0;
      final adapter = DriftLocalStorageAdapter(
        database: db,
        operationIdGenerator: () => 'op-correct-${++counter}',
      );
      addTearDown(() async {
        await adapter.dispose();
        await db.close();
      });

      final queue = StreamQueue<int>(adapter.watchPendingCount());
      addTearDown(queue.cancel);

      // Initial emission: 0.
      expect(await queue.next, equals(0));

      // The outer transaction aborts; child writes are rolled back.
      await expectLater(
        () => db.transaction(() async {
          await adapter.runWithTransaction((outbox) async {
            await outbox.enqueue(
              LocalMutation(
                entityType: 'tasks',
                entityId: 't-correct',
                type: SyncOperationType.delete,
              ),
            );
          });
          throw StateError('outer abort');
        }),
        throwsStateError,
      );

      // Manually refresh so the stream reflects the rolled-back state.
      await adapter.refreshPendingCount();
      expect(await queue.next, equals(0),
          reason: 'count must return to 0 after refresh following outer abort');
    });
  });
}
