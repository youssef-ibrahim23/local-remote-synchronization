import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:basic_offline_first_app/data/todo_repository.dart';
import 'package:basic_offline_first_app/domain/todo.dart';
import 'package:basic_offline_first_app/sync/todo_rest_config.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:local_remote_sqflite/local_remote_sqflite.dart';
import 'package:local_remote_synchronization/local_remote_synchronization.dart';
import 'package:sqflite/sqflite.dart' show Sqflite;
import 'package:sqflite_common_ffi/sqflite_ffi.dart';

/// Integration tests for the reference application's integration boundary.
///
/// These tests use real SQLite (via sqflite_common_ffi) with deterministic
/// fakes for the remote adapter and connectivity provider.
///
/// No real backend or credentials are required.
///
/// Coverage:
///   A. Local Todo mutations and outbox enqueue use the same transaction.
///   B. Transaction rollback leaves neither a partial domain mutation nor a
///      partial outbox operation.
///   C. SyncEngine pushes an eligible operation through the configured adapter.
///   D. Remote failure does NOT remove the local todo.
///   E. Retry can eventually succeed.
///   F. Resources and subscriptions are disposed correctly.

late Directory _tempDir;
late String _dbPath;

Future<Database> _openDb() => databaseFactoryFfi.openDatabase(
      _dbPath,
      options: OpenDatabaseOptions(
        version: 1,
        onCreate: (db, _) async {
          await TodoRepository.createSchema(db);
          await SqfliteOutboxSchema.createSchema(db);
        },
      ),
    );

void main() {
  sqfliteFfiInit();

  setUp(() async {
    _tempDir = await Directory.systemTemp.createTemp('todo_app_test_');
    _dbPath = '${_tempDir.path}/test.db';
  });

  tearDown(() async {
    if (_tempDir.existsSync()) {
      await _tempDir.delete(recursive: true);
    }
  });

  // ─── A. Atomic create ─────────────────────────────────────────────────────

  group('A. atomic create — same transaction', () {
    test(
        'creating a todo persists the domain row and the outbox operation '
        'atomically', () async {
      final h = _Harness();
      await h.open();
      addTearDown(h.dispose);

      const todo = Todo(id: 'todo-1', title: 'Buy groceries');
      final op = await h.repo.create(todo);

      // Domain row present.
      final rows = await h.db.query('todos');
      expect(rows, hasLength(1));
      expect(rows.single['id'], 'todo-1');
      expect(rows.single['title'], 'Buy groceries');
      expect(rows.single['completed'], 0);

      // Outbox row present.
      expect(op.entityType, 'todos');
      expect(op.entityId, 'todo-1');
      expect(op.type, SyncOperationType.create);
      expect(op.payload?['title'], 'Buy groceries');
      expect(await h.outboxCount(), 1);
    });

    test('updating a todo enqueues an update operation', () async {
      final h = _Harness();
      await h.open();
      addTearDown(h.dispose);

      const todo = Todo(id: 'todo-upd', title: 'Original');
      await h.repo.create(todo);

      final updated = todo.copyWith(completed: true);
      final op = await h.repo.update(updated);

      expect(op.type, SyncOperationType.update);
      expect(op.payload?['completed'], true);
      expect(await h.outboxCount(), 2); // create + update

      final rows = await h.db.query('todos');
      expect(rows.single['completed'], 1);
    });

    test('deleting a todo enqueues a delete operation', () async {
      final h = _Harness();
      await h.open();
      addTearDown(h.dispose);

      const todo = Todo(id: 'todo-del', title: 'To delete');
      await h.repo.create(todo);
      final delOp = await h.repo.delete(todo.id);

      expect(delOp.type, SyncOperationType.delete);
      expect(delOp.entityId, 'todo-del');
      expect(await h.outboxCount(), 2); // create + delete

      // Domain row is gone.
      final rows = await h.db.query('todos', where: "id = 'todo-del'");
      expect(rows, isEmpty);
    });
  });

  // ─── B. Transaction rollback ───────────────────────────────────────────────

  group('B. transaction rollback', () {
    test('a failed transaction leaves neither a domain row nor an outbox row',
        () async {
      final h = _Harness();
      await h.open();
      addTearDown(h.dispose);

      await expectLater(
        h.storage.runWithTransaction((txn, outbox) async {
          await txn.insert('todos', <String, Object?>{
            'id': 'rollback-id',
            'title': 'Should not exist',
            'completed': 0,
          });
          await outbox.enqueue(
            LocalMutation(
              entityType: 'todos',
              entityId: 'rollback-id',
              type: SyncOperationType.create,
              payload: const <String, dynamic>{'title': 'Should not exist'},
            ),
          );
          throw StateError('forced rollback');
        }),
        throwsStateError,
      );

      expect(await h.db.query('todos'), isEmpty);
      expect(await h.outboxCount(), 0);
    });
  });

  // ─── C. SyncEngine pushes through the adapter ─────────────────────────────

  group('C. sync engine integration', () {
    test('engine pushes an eligible create operation and marks it succeeded',
        () async {
      final h = _Harness();
      await h.open();
      addTearDown(h.dispose);

      const todo = Todo(id: 'sync-todo', title: 'Synced item');
      await h.repo.create(todo);

      await h.engine.start();
      final report = await h.engine.syncNow();

      expect(h.remote.pushed, hasLength(1));
      expect(h.remote.pushed.single.entityType, 'todos');
      expect(h.remote.pushed.single.entityId, 'sync-todo');
      expect(h.remote.pushed.single.payload?['title'], 'Synced item');

      expect(report.succeeded, 1);
      expect(report.failed, 0);
      expect(report.deadLettered, 0);
      expect(report.cycleError, isNull);

      expect(await h.outboxCount(), 0);
    });
  });

  // ─── D. Remote failure does not remove the local todo ─────────────────────

  group('D. remote failure preserves local todo', () {
    test('a retryable remote failure leaves the todo in local storage',
        () async {
      final h = _Harness();
      await h.open();
      addTearDown(h.dispose);

      const todo = Todo(id: 'fail-todo', title: 'Fails remotely');
      await h.repo.create(todo);

      h.remote.resultFor = (op) => SyncFailure(
            operationId: op.operationId,
            error: SyncError(
              code: SyncErrorCode.network,
              message: 'simulated network error',
              isRetryable: true,
            ),
          );

      await h.engine.start();
      final report = await h.engine.syncNow();

      expect(report.failed, 1);
      expect(report.succeeded, 0);

      // Local todo is still present — remote sync and local persistence are
      // independent concerns.
      final rows = await h.db.query('todos', where: "id = 'fail-todo'");
      expect(rows, hasLength(1));
      expect(rows.single['title'], 'Fails remotely');

      // The outbox operation is still present (marked 'failed').
      expect(await h.outboxCount(), 1);
    });

    test('a non-retryable remote failure dead-letters but preserves local todo',
        () async {
      final h = _Harness();
      await h.open();
      addTearDown(h.dispose);

      const todo = Todo(id: 'perm-fail', title: 'Permanent fail');
      await h.repo.create(todo);

      h.remote.resultFor = (op) => SyncFailure(
            operationId: op.operationId,
            error: SyncError(
              code: SyncErrorCode.validation,
              message: 'permanent error',
              isRetryable: false,
            ),
          );

      await h.engine.start();
      final report = await h.engine.syncNow();

      expect(report.deadLettered, 1);
      expect(report.succeeded, 0);

      // Local todo is unaffected.
      final rows = await h.db.query('todos', where: "id = 'perm-fail'");
      expect(rows, hasLength(1));
    });
  });

  // ─── E. Retry eventually succeeds ─────────────────────────────────────────

  group('E. retry eventually succeeds', () {
    test('failed op is retried after nextRetryAt and eventually succeeds',
        () async {
      var attempts = 0;
      final fixedClock = _FixedClock(DateTime.utc(2026, 1, 1, 12));

      final h = _Harness(
        clock: fixedClock.call,
        retryPolicy: _FixedRetryPolicy(
          delay: const Duration(minutes: 10),
          maxAttempts: 5,
        ),
      );
      await h.open();
      addTearDown(h.dispose);

      const todo = Todo(id: 'retry-todo', title: 'Retried item');
      await h.repo.create(todo);

      h.remote.resultFor = (op) {
        attempts++;
        if (attempts < 2) {
          return SyncFailure(
            operationId: op.operationId,
            error: SyncError(
              code: SyncErrorCode.network,
              message: 'transient',
              isRetryable: true,
            ),
          );
        }
        return SyncSuccess(operationId: op.operationId);
      };

      await h.engine.start();

      // Cycle 1 at t0: fails.
      final report1 = await h.engine.syncNow();
      expect(report1.failed, 1);
      expect(report1.succeeded, 0);

      // Cycle 2 at t0+5m: not yet eligible.
      fixedClock.advance(const Duration(minutes: 5));
      final report2 = await h.engine.syncNow();
      expect(report2.fetched, 0);

      // Cycle 3 at t0+10m: eligible again, succeeds.
      fixedClock.advance(const Duration(minutes: 5));
      final report3 = await h.engine.syncNow();
      expect(report3.succeeded, 1);
      expect(await h.outboxCount(), 0);
      expect(attempts, 2);
    });
  });

  // ─── F. Resource disposal ─────────────────────────────────────────────────

  group('F. resource disposal', () {
    test('engine and storage adapter dispose without throwing', () async {
      final h = _Harness();
      await h.open();

      const todo = Todo(id: 'disp-1', title: 'Disposal test');
      await h.repo.create(todo);

      await h.engine.start();

      // No exception from disposing engine and adapter.
      await expectLater(h.engine.dispose(), completes);
      await expectLater(h.storage.dispose(), completes);
      await expectLater(h.db.close(), completes);
    });

    test('watchPendingCount stream closes after adapter dispose', () async {
      final h = _Harness();
      await h.open();
      addTearDown(h.dispose);

      final counts = <int>[];
      final done = Completer<void>();
      h.storage.watchPendingCount().listen(
            counts.add,
            onDone: done.complete,
          );

      const todo = Todo(id: 'watch-1', title: 'Watch test');
      await h.repo.create(todo);
      await h.storage.dispose();

      await done.future.timeout(const Duration(seconds: 2));
      // At least one count was emitted before the stream closed.
      expect(counts, isNotEmpty);
    });
  });

  // ─── G. REST adapter end-to-end ──────────────────────────────────────────

  group('G. REST adapter end-to-end', () {
    test(
        'SyncEngine pushes create operation through configured '
        'RestRemoteSyncAdapter to local test server', () async {
      final receivedRequests = <({
        String method,
        String path,
        String body,
        String? idempotencyKey
      })>[];
      final server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
      addTearDown(server.close);

      server.listen((request) async {
        final body = await utf8.decoder.bind(request).join();
        receivedRequests.add((
          method: request.method,
          path: request.uri.path,
          body: body,
          idempotencyKey: request.headers.value('Idempotency-Key'),
        ));
        request.response.statusCode = HttpStatus.created;
        await request.response.close();
      });

      final db = await _openDb();
      final storage = SqfliteLocalStorageAdapter(database: db);
      await storage.ensureInitialized();
      final repo = TodoRepository(database: db, storageAdapter: storage);
      final restAdapter = buildRestAdapter(
        baseUrl: 'http://127.0.0.1:${server.port}',
      );
      final engine = SyncEngine(
        localStorage: storage,
        remoteSync: restAdapter,
        connectivity: _AlwaysOnline(),
        config: SyncEngineConfig(
          syncOnStart: false,
          syncOnConnectivityRestore: false,
        ),
      );
      addTearDown(() async {
        await engine.dispose();
        restAdapter.close();
        await storage.dispose();
        await db.close();
      });

      const todo = Todo(id: 'rest-1', title: 'REST end-to-end');
      final op = await repo.create(todo);

      await engine.start();
      final report = await engine.syncNow();

      expect(report.succeeded, 1);
      expect(report.failed, 0);
      expect(
          await db
              .rawQuery('SELECT COUNT(*) AS count FROM sync_outbox')
              .then((r) => Sqflite.firstIntValue(r) ?? 0),
          0);

      expect(receivedRequests, hasLength(1));
      final req = receivedRequests.single;
      expect(req.method, 'POST');
      expect(req.path, '/todos');
      expect(req.idempotencyKey, op.idempotencyKey);
      final decoded = jsonDecode(req.body) as Map<String, dynamic>;
      expect(decoded['id'], 'rest-1');
      expect(decoded['title'], 'REST end-to-end');
      expect(decoded['completed'], false);
    });
  });
}

// ─── Harness ──────────────────────────────────────────────────────────────

final class _Harness {
  _Harness({
    DateTime Function()? clock,
    RetryPolicy? retryPolicy,
  })  : _clock = clock,
        _retryPolicy = retryPolicy;

  final DateTime Function()? _clock;
  final RetryPolicy? _retryPolicy;

  late final Database db;
  late final SqfliteLocalStorageAdapter storage;
  late final TodoRepository repo;
  late final _FakeRemote remote;
  late final SyncEngine engine;

  Future<void> open() async {
    db = await _openDb();
    storage = SqfliteLocalStorageAdapter(
      database: db,
      clock: _clock,
    );
    await storage.ensureInitialized();
    repo = TodoRepository(database: db, storageAdapter: storage);
    remote = _FakeRemote();
    engine = SyncEngine(
      localStorage: storage,
      remoteSync: remote,
      connectivity: _AlwaysOnline(),
      config: SyncEngineConfig(
        syncOnStart: false,
        syncOnConnectivityRestore: false,
      ),
      retryPolicy: _retryPolicy ??
          _FixedRetryPolicy(
            delay: const Duration(minutes: 10),
            maxAttempts: 5,
          ),
      clock: _clock,
    );
  }

  Future<void> dispose() async {
    await engine.dispose();
    await storage.dispose();
    await db.close();
  }

  Future<int> outboxCount() async {
    final rows = await db.rawQuery(
      'SELECT COUNT(*) AS count FROM sync_outbox',
    );
    return Sqflite.firstIntValue(rows) ?? 0;
  }
}

// ─── Fakes ────────────────────────────────────────────────────────────────

final class _FakeRemote implements RemoteSyncAdapter {
  final List<SyncOperation> pushed = <SyncOperation>[];
  SyncResult Function(SyncOperation)? resultFor;

  @override
  Future<SyncResult> push(SyncOperation operation) async {
    pushed.add(operation);
    return resultFor?.call(operation) ??
        SyncSuccess(operationId: operation.operationId);
  }
}

final class _AlwaysOnline implements ConnectivityProvider {
  @override
  Future<bool> get isConnected => Future.value(true);

  @override
  Stream<bool> get onConnectivityChanged => const Stream.empty();
}

final class _FixedRetryPolicy implements RetryPolicy {
  _FixedRetryPolicy({required this.delay, required this.maxAttempts});

  final Duration delay;
  final int maxAttempts;

  @override
  Duration computeDelay(int attemptCount, SyncError error) => delay;

  @override
  bool shouldDeadLetter(int attemptCount, SyncError error) =>
      !error.isRetryable || attemptCount >= maxAttempts;
}

/// Mutable clock for testing time-sensitive retry logic.
final class _FixedClock {
  _FixedClock(this._now);

  DateTime _now;

  DateTime call() => _now;

  void advance(Duration duration) => _now = _now.add(duration);
}
