import 'dart:async';
import 'dart:io';

import 'package:flutter_test/flutter_test.dart';
import 'package:local_remote_sqflite/local_remote_sqflite.dart';
import 'package:local_remote_synchronization/local_remote_synchronization.dart';
import 'package:sqflite_common_ffi/sqflite_ffi.dart';

/// Phase 3.5 integration tests: real [SyncEngine] + real
/// [SqfliteLocalStorageAdapter] backed by a real SQLite database file
/// (sqflite_common_ffi), with deterministic fakes for the remote adapter
/// and connectivity provider.
///
/// Dependency direction is preserved: this test lives in the Sqflite
/// adapter package (which already depends on Core), so Core never gains
/// a dependency on a concrete adapter.

late Directory tempDir;
late String dbPath;

/// Opens (creating on first use) the real SQLite database file shared by
/// a [Harness] and its simulated restarts.
Future<Database> openIntegrationDb() => databaseFactoryFfi.openDatabase(
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

void main() {
  sqfliteFfiInit();

  setUp(() async {
    tempDir = await Directory.systemTemp.createTemp('sync_engine_it_');
    dbPath = '${tempDir.path}/test.db';
  });

  tearDown(() async {
    if (tempDir.existsSync()) {
      await tempDir.delete(recursive: true);
    }
  });

  SyncFailure retryableFailure(String operationId) => SyncFailure(
        operationId: operationId,
        error: SyncError(
          code: SyncErrorCode.network,
          message: 'transient',
          isRetryable: true,
        ),
      );

  SyncFailure permanentFailure(String operationId) => SyncFailure(
        operationId: operationId,
        error: SyncError(
          code: SyncErrorCode.validation,
          message: 'permanent',
          isRetryable: false,
        ),
      );

  /// Pumps the event loop a bounded number of turns so already-scheduled
  /// microtasks/event-queue work can run. Never used to wait for a
  /// condition that requires elapsed time.
  Future<void> pumpEventQueue([int turns = 20]) async {
    for (var i = 0; i < turns; i++) {
      await Future<void>.delayed(Duration.zero);
    }
  }

  // -------------------------------------------------------------------------
  // A. Full sync cycle
  // -------------------------------------------------------------------------
  group('A. full sync cycle', () {
    test('enqueued mutations are pushed with payloads and removed', () async {
      final h = Harness();
      await h.open();
      addTearDown(h.dispose);

      final op1 = await h.enqueue(
        entityId: 'task-1',
        payload: const <String, dynamic>{'title': 'Alpha'},
      );
      final op2 = await h.enqueue(
        entityId: 'task-2',
        type: SyncOperationType.update,
        payload: const <String, dynamic>{'title': 'Beta', 'v': 2},
      );

      await h.engine.start();
      final report = await h.engine.syncNow();

      expect(h.remote.pushed, hasLength(2));
      expect(
        h.pushedIds(),
        containsAll(<String>[op1.operationId, op2.operationId]),
      );
      final pushed1 = h.remote.pushed.firstWhere(
        (op) => op.operationId == op1.operationId,
      );
      expect(pushed1.entityType, 'tasks');
      expect(pushed1.entityId, 'task-1');
      expect(pushed1.payload?['title'], 'Alpha');
      expect(pushed1.sequenceNumber, isNotNull);

      expect(report.fetched, 2);
      expect(report.succeeded, 2);
      expect(report.failed, 0);
      expect(report.conflicted, 0);
      expect(report.deadLettered, 0);
      expect(report.storageErrors, 0);
      expect(report.cycleError, isNull);
      expect(report.skippedOffline, isFalse);

      // Sqflite markSucceeded deletes the row — the outbox is empty.
      expect(await h.outboxCount(), 0);
      expect(await h.outboxRow(op1.operationId), isNull);
      expect(await h.outboxRow(op2.operationId), isNull);
    });
  });

  // -------------------------------------------------------------------------
  // B. Retry across cycles
  // -------------------------------------------------------------------------
  group('B. retry across cycles', () {
    test('failed op waits for nextRetryAt, then succeeds on a later cycle',
        () async {
      final h = Harness();
      await h.open();
      addTearDown(h.dispose);

      final op = await h.enqueue(entityId: 'retry-1');
      var pushAttempts = 0;
      h.remote.resultFor = (pushed) {
        pushAttempts++;
        return pushAttempts == 1
            ? retryableFailure(pushed.operationId)
            : SyncSuccess(operationId: pushed.operationId);
      };

      await h.engine.start();

      // Cycle 1 at t0: retryable failure -> failed with backoff.
      final report1 = await h.engine.syncNow();
      expect(report1.failed, 1);
      expect(report1.succeeded, 0);
      expect(h.pushedIds(), <String>[op.operationId]);

      final retryAt = Harness.t0.add(const Duration(minutes: 10));
      final row1 = await h.outboxRow(op.operationId);
      expect(row1?['status'], SyncOperationStatus.failed.name);
      expect(row1?['attempt_count'], 1);
      expect(row1?['next_retry_at_ms'], retryAt.millisecondsSinceEpoch);

      // Cycle 2 at t0+5m: before nextRetryAt -> not eligible, not pushed.
      h.now = Harness.t0.add(const Duration(minutes: 5));
      final report2 = await h.engine.syncNow();
      expect(report2.fetched, 0);
      expect(report2.succeeded, 0);
      expect(report2.failed, 0);
      expect(h.remote.pushed, hasLength(1));

      // Cycle 3 at exactly nextRetryAt -> eligible (<= comparison).
      h.now = retryAt;
      final report3 = await h.engine.syncNow();
      expect(report3.succeeded, 1);
      expect(h.remote.pushed, hasLength(2));
      expect(await h.outboxCount(), 0);
    });
  });

  // -------------------------------------------------------------------------
  // C. Conflict and dead-letter integration
  // -------------------------------------------------------------------------
  group('C. conflict integration', () {
    test(
        'conflict persists deadLetter before emitting the event and '
        'blocks later same-entity operations', () async {
      final h = Harness();
      await h.open();
      addTearDown(h.dispose);

      final c1 = await h.enqueue(entityId: 'doc-1');

      final events = <SyncConflictEvent>[];
      final eventReceived = Completer<void>();
      String? statusAtEventTime;
      final statusChecks = <Future<void>>[];
      h.engine.conflicts.listen((event) {
        events.add(event);
        if (!eventReceived.isCompleted) eventReceived.complete();
        // The engine emits only after markDeadLetter commits; verify the
        // persisted status at event time is already deadLetter.
        statusChecks.add(
          h.db.rawQuery(
            'SELECT status FROM sync_outbox WHERE operation_id = ?',
            <Object?>[event.operation.operationId],
          ).then((rows) {
            statusAtEventTime = rows.single['status'] as String?;
          }),
        );
      });

      h.remote.resultFor = (pushed) => SyncConflict(
            operationId: pushed.operationId,
            remoteSnapshot: const <String, dynamic>{'server': 'value'},
          );

      await h.engine.start();
      final report = await h.engine.syncNow();
      // Broadcast delivery is asynchronous — wait for the event, then
      // for the persisted-status check it scheduled.
      await eventReceived.future;
      await Future.wait(statusChecks);

      expect(report.conflicted, 1);
      expect(report.deadLettered, 1);
      expect(report.succeeded, 0);
      expect(report.failed, 0);

      final row = await h.outboxRow(c1.operationId);
      expect(row?['status'], SyncOperationStatus.deadLetter.name);
      expect(row?['attempt_count'], 1);

      expect(events, hasLength(1));
      expect(events.single.operation.operationId, c1.operationId);
      // Pre-transition snapshot: status reflects fetch time (pending).
      expect(events.single.operation.status, SyncOperationStatus.pending);
      expect(statusAtEventTime, SyncOperationStatus.deadLetter.name);

      // A later operation for the same entity remains blocked: the
      // deadLetter predecessor makes it ineligible in every later cycle.
      final c2 = await h.enqueue(entityId: 'doc-1');
      h.now = Harness.t0.add(const Duration(hours: 1));
      final report2 = await h.engine.syncNow();
      expect(report2.fetched, 0);
      expect(h.pushedIds(), <String>[c1.operationId]);
      final row2 = await h.outboxRow(c2.operationId);
      expect(row2?['status'], SyncOperationStatus.pending.name);
    });
  });

  // -------------------------------------------------------------------------
  // D. Dead-letter integration
  // -------------------------------------------------------------------------
  group('D. dead-letter integration', () {
    test('non-retryable failure dead-letters and is never pushed again',
        () async {
      final h = Harness();
      await h.open();
      addTearDown(h.dispose);

      final d1 = await h.enqueue(entityId: 'dead-1');
      h.remote.resultFor = (pushed) => permanentFailure(pushed.operationId);

      await h.engine.start();
      final report1 = await h.engine.syncNow();
      expect(report1.deadLettered, 1);
      expect(report1.failed, 0);
      expect(report1.succeeded, 0);

      final row = await h.outboxRow(d1.operationId);
      expect(row?['status'], SyncOperationStatus.deadLetter.name);
      expect(row?['attempt_count'], 1);

      // Subsequent cycles never re-push a deadLetter operation.
      h.now = Harness.t0.add(const Duration(hours: 1));
      final report2 = await h.engine.syncNow();
      expect(report2.fetched, 0);
      expect(h.pushedIds(), <String>[d1.operationId]);
      expect(h.remote.pushed, hasLength(1));
    });
  });

  // -------------------------------------------------------------------------
  // E. Crash/restart recovery
  // -------------------------------------------------------------------------
  group('E. crash/restart recovery', () {
    test('stale inFlight operation is recovered to pending and pushed',
        () async {
      final h = Harness();
      await h.open();
      addTearDown(h.dispose);

      final op = await h.enqueue(entityId: 'crash-1');
      // Simulate a crash after the remote push but before any transition:
      // the row is left inFlight in the real database file.
      await h.sqfliteStorage.markInFlight(op.operationId);
      expect(
        (await h.outboxRow(op.operationId))?['status'],
        SyncOperationStatus.inFlight.name,
      );

      await h.restart();

      await h.engine.start();
      // Startup recovery ran: the operation is pending and eligible.
      final eligible = await h.sqfliteStorage.fetchEligibleOperations(
        now: h.now,
        limit: 10,
      );
      expect(eligible, hasLength(1));
      expect(eligible.single.operationId, op.operationId);
      expect(eligible.single.status, SyncOperationStatus.pending);

      final report = await h.engine.syncNow();
      expect(report.succeeded, 1);
      expect(h.pushedIds(), <String>[op.operationId]);
      expect(await h.outboxCount(), 0);
    });

    test('failed operation retains retry metadata across close/reopen',
        () async {
      final h = Harness();
      await h.open();
      addTearDown(h.dispose);

      final op = await h.enqueue(entityId: 'retry-persist');
      final nextRetryAt = Harness.t0.add(const Duration(minutes: 30));
      await h.sqfliteStorage.markFailed(
        op.operationId,
        attemptCount: 2,
        nextRetryAt: nextRetryAt,
        error: SyncError(
          code: SyncErrorCode.network,
          message: 'persisted failure',
          isRetryable: true,
        ),
      );

      await h.restart();

      // Before nextRetryAt the op is not eligible.
      h.now = Harness.t0.add(const Duration(minutes: 10));
      expect(
        await h.sqfliteStorage.fetchEligibleOperations(
          now: h.now,
          limit: 10,
        ),
        isEmpty,
      );

      // At/after nextRetryAt it is eligible with persisted metadata.
      h.now = nextRetryAt;
      final eligible = await h.sqfliteStorage.fetchEligibleOperations(
        now: h.now,
        limit: 10,
      );
      expect(eligible, hasLength(1));
      expect(eligible.single.status, SyncOperationStatus.failed);
      expect(eligible.single.attemptCount, 2);
      expect(eligible.single.nextRetryAt, nextRetryAt);
      expect(eligible.single.lastError?.message, 'persisted failure');
    });
  });

  // -------------------------------------------------------------------------
  // F. Remote push succeeds but markSucceeded fails
  // -------------------------------------------------------------------------
  group('F. markSucceeded storage failure', () {
    test('unknown persisted state is recovered and the op may be re-pushed',
        () async {
      final h = Harness();
      await h.open();
      // Test-only failure injection at the SQLite level: a BEFORE DELETE
      // trigger makes markSucceeded's row delete fail after the remote
      // push already succeeded. No production API is modified.
      await h.db.execute('''
        CREATE TRIGGER block_outbox_delete
        BEFORE DELETE ON sync_outbox
        BEGIN
          SELECT RAISE(ABORT, 'delete blocked for test');
        END
      ''');
      addTearDown(h.dispose);

      final op = await h.enqueue(entityId: 'at-least-once');

      await h.engine.start();
      final report1 = await h.engine.syncNow();

      // Remote accepted the push, but the success transition failed:
      // operation-level storage error, NOT counted as succeeded.
      expect(h.remote.pushed, hasLength(1));
      expect(report1.storageErrors, 1);
      expect(report1.succeeded, 0);
      expect(report1.cycleError, isNull);

      // Persisted state is unknown -> the row remains inFlight.
      expect(
        (await h.outboxRow(op.operationId))?['status'],
        SyncOperationStatus.inFlight.name,
      );

      // Simulate restart: close/reopen the database file, drop the
      // blocking trigger, rebuild adapter+engine, and let startup
      // recovery return the op to pending.
      await h.restart();
      await h.db.execute('DROP TRIGGER IF EXISTS block_outbox_delete');

      await h.engine.start();
      final report2 = await h.engine.syncNow();

      // The operation MAY be pushed again — remote delivery is
      // at-least-once. Backend idempotency enforcement (via
      // idempotencyKey) is required to protect against duplicate remote
      // effects; exactly-once is NOT claimed.
      expect(h.pushedIds(), <String>[op.operationId, op.operationId]);
      expect(report2.succeeded, 1);
      expect(report2.storageErrors, 0);
      expect(await h.outboxCount(), 0);
    });
  });

  // -------------------------------------------------------------------------
  // G. Connectivity loss during push
  // -------------------------------------------------------------------------
  group('G. connectivity loss during push', () {
    test(
        'offline transition does not disturb the active push; offline '
        'syncNow is skipped', () async {
      final h = Harness(
        config: SyncEngineConfig(
          syncOnStart: false,
          syncOnConnectivityRestore: false,
        ),
      );
      await h.open();
      addTearDown(h.dispose);

      final op = await h.enqueue(entityId: 'conn-1');
      final pushEntered = Completer<void>();
      final pushGate = Completer<SyncResult>();
      h.remote.onPushStarted = (_) {
        if (!pushEntered.isCompleted) pushEntered.complete();
      };
      h.remote.gates[op.operationId] = pushGate;

      final reports = <SyncCycleReport>[];
      h.engine.reports.listen(reports.add);

      await h.engine.start();
      final reportFuture = h.engine.syncNow();
      await pushEntered.future;

      // A second syncNow while the cycle is active returns the same
      // in-flight report — no overlapping cycle is started.
      expect(identical(h.engine.syncNow(), reportFuture), isTrue);

      // Connectivity drops while the push is pending.
      h.connectivity.emit(online: false);
      await pumpEventQueue();
      expect(h.remote.pushed, hasLength(1));

      // Completing the push settles the operation normally — the cycle
      // does not abort on the offline transition.
      pushGate.complete(SyncSuccess(operationId: op.operationId));
      final report = await reportFuture;
      expect(report.succeeded, 1);
      expect(await h.outboxCount(), 0);

      // An explicit sync while offline is skipped per syncWhenOffline.
      final offlineReport = await h.engine.syncNow();
      expect(offlineReport.skippedOffline, isTrue);
      expect(offlineReport.fetched, 0);
      expect(h.remote.pushed, hasLength(1));

      // Coming back online does not auto-trigger (syncOnConnectivityRestore
      // is false) and no additional push occurs.
      h.connectivity.emit(online: true);
      await pumpEventQueue();
      expect(h.remote.pushed, hasLength(1));
      expect(reports, hasLength(2));
    });
  });

  // -------------------------------------------------------------------------
  // H. Disposal during active cycle
  // -------------------------------------------------------------------------
  group('H. disposal during active cycle', () {
    test('dispose waits for the active cycle; streams close cleanly', () async {
      final h = Harness();
      await h.open();
      addTearDown(h.dispose);

      final op = await h.enqueue(entityId: 'dispose-1');
      final pushEntered = Completer<void>();
      final pushGate = Completer<SyncResult>();
      h.remote.onPushStarted = (_) {
        if (!pushEntered.isCompleted) pushEntered.complete();
      };
      h.remote.gates[op.operationId] = pushGate;

      final reports = <SyncCycleReport>[];
      final conflicts = <SyncConflictEvent>[];
      final reportsDone = Completer<void>();
      final conflictsDone = Completer<void>();
      h.engine.reports.listen(reports.add, onDone: reportsDone.complete);
      h.engine.conflicts.listen(conflicts.add, onDone: conflictsDone.complete);

      await h.engine.start();
      final reportFuture = h.engine.syncNow();
      await pushEntered.future;

      var disposeCompleted = false;
      final disposeFuture = h.engine.dispose().then((_) {
        disposeCompleted = true;
      });

      // Disposal cannot complete while the push is still gated.
      await pumpEventQueue();
      expect(disposeCompleted, isFalse);

      pushGate.complete(SyncSuccess(operationId: op.operationId));
      final report = await reportFuture;
      expect(report.succeeded, 1);

      await disposeFuture;
      expect(disposeCompleted, isTrue);
      expect(h.engine.isRunning, isFalse);

      // The report was emitted before stream closure; nothing is emitted
      // after the controllers close.
      await reportsDone.future;
      await conflictsDone.future;
      expect(reports, hasLength(1));
      expect(conflicts, isEmpty);
      expect(() => h.engine.syncNow(), throwsStateError);
    });
  });

  // -------------------------------------------------------------------------
  // I. Disposal during pending recovery
  // -------------------------------------------------------------------------
  group('I. disposal during pending recovery', () {
    test(
        'dispose during gated recoverInFlightOperations aborts startup '
        'cleanly', () async {
      late GatedStorage gated;
      final h = Harness.wrapping(
        (inner) => gated = GatedStorage(inner),
        // syncOnStart defaults to true — a successful startup would
        // auto-schedule a cycle, which must NOT happen after abort.
        config: SyncEngineConfig(),
      );
      await h.open();
      addTearDown(h.dispose);

      await h.enqueue(entityId: 'recover-gate');

      final startFuture = h.engine.start();
      await pumpEventQueue();
      expect(gated.recoverCalls, 1);

      var disposeCompleted = false;
      final disposeFuture = h.engine.dispose().then((_) {
        disposeCompleted = true;
      });
      await pumpEventQueue();
      expect(disposeCompleted, isFalse);

      // Release recovery; startup observes the disposal request and
      // aborts before subscribing to connectivity or scheduling sync.
      gated.recoverGate.complete();
      await startFuture;
      await disposeFuture;
      expect(disposeCompleted, isTrue);

      expect(h.engine.isRunning, isFalse);
      expect(h.connectivity.subscribeCount, 0);
      expect(h.connectivity.sampleCount, 0);
      expect(h.remote.pushed, isEmpty);
      expect(() => h.engine.syncNow(), throwsStateError);

      // The outbox row is untouched — the engine never reached a cycle.
      expect(await h.outboxCount(), 1);
    });
  });

  // -------------------------------------------------------------------------
  // J. Multiple sequential cycles
  // -------------------------------------------------------------------------
  group('J. multiple sequential cycles', () {
    test('sequential cycles each produce a report and drain the outbox',
        () async {
      final h = Harness();
      await h.open();
      addTearDown(h.dispose);

      final reports = <SyncCycleReport>[];
      h.engine.reports.listen(reports.add);

      await h.enqueue(entityId: 'seq-1');
      await h.enqueue(entityId: 'seq-2');
      await h.enqueue(entityId: 'seq-3');

      await h.engine.start();
      final report1 = await h.engine.syncNow();
      expect(report1.fetched, 3);
      expect(report1.succeeded, 3);
      expect(h.remote.pushed, hasLength(3));

      // The cycle reservation is released — a later cycle runs normally.
      await h.enqueue(entityId: 'seq-4');
      final report2 = await h.engine.syncNow();
      expect(report2.fetched, 1);
      expect(report2.succeeded, 1);
      expect(h.remote.pushed, hasLength(4));

      await pumpEventQueue();
      expect(reports, hasLength(2));
      expect(reports[0].succeeded, 3);
      expect(reports[1].succeeded, 1);
      expect(await h.outboxCount(), 0);
    });
  });

  // -------------------------------------------------------------------------
  // Transaction behavior through runWithTransaction + SyncEngine
  // -------------------------------------------------------------------------
  group('transaction behavior', () {
    test(
        'domain write and outbox enqueue commit together and sync '
        'pushes the enqueued operation', () async {
      final h = Harness();
      await h.open();
      addTearDown(h.dispose);

      await h.sqfliteStorage.runWithTransaction((txn, outbox) async {
        await txn.insert('tasks', <String, Object?>{
          'id': 't-atomic',
          'title': 'Atomic task',
        });
        await outbox.enqueue(
          LocalMutation(
            entityType: 'tasks',
            entityId: 't-atomic',
            type: SyncOperationType.create,
            payload: const <String, dynamic>{
              'id': 't-atomic',
              'title': 'Atomic task',
            },
          ),
        );
      });

      await h.engine.start();
      final report = await h.engine.syncNow();
      expect(report.succeeded, 1);
      expect(h.remote.pushed.single.payload?['id'], 't-atomic');

      final tasks = await h.db.query('tasks');
      expect(tasks, hasLength(1));
      expect(tasks.single['title'], 'Atomic task');
      expect(await h.outboxCount(), 0);
    });

    test(
        'a failed transaction rolls back both the domain write and the '
        'outbox enqueue', () async {
      final h = Harness();
      await h.open();
      addTearDown(h.dispose);

      await expectLater(
        h.sqfliteStorage.runWithTransaction((txn, outbox) async {
          await txn.insert('tasks', <String, Object?>{
            'id': 't-rollback',
            'title': 'Should not persist',
          });
          await outbox.enqueue(
            LocalMutation(
              entityType: 'tasks',
              entityId: 't-rollback',
              type: SyncOperationType.create,
              payload: const <String, dynamic>{'id': 't-rollback'},
            ),
          );
          throw StateError('forced rollback');
        }),
        throwsStateError,
      );

      expect(await h.db.query('tasks'), isEmpty);
      expect(await h.outboxCount(), 0);
    });
  });
}

/// Deterministic remote fake. [resultFor] scripts per-operation
/// results; [gates] holds a push open until the completer completes;
/// [onPushStarted] fires synchronously when a push begins (used
/// instead of timing assumptions).
final class FakeRemote implements RemoteSyncAdapter {
  final List<SyncOperation> pushed = <SyncOperation>[];
  final Map<String, Completer<SyncResult>> gates =
      <String, Completer<SyncResult>>{};
  SyncResult Function(SyncOperation operation)? resultFor;
  void Function(SyncOperation operation)? onPushStarted;

  @override
  Future<SyncResult> push(SyncOperation operation) async {
    pushed.add(operation);
    onPushStarted?.call(operation);
    final gate = gates[operation.operationId];
    if (gate != null) {
      await gate.future;
    }
    final scripted = resultFor?.call(operation);
    return scripted ?? SyncSuccess(operationId: operation.operationId);
  }
}

/// Deterministic connectivity fake with manual transitions.
final class FakeConnectivity implements ConnectivityProvider {
  FakeConnectivity({bool online = true}) : _online = online;

  bool _online;
  final StreamController<bool> _controller = StreamController<bool>.broadcast();
  int subscribeCount = 0;
  int sampleCount = 0;

  @override
  Future<bool> get isConnected {
    sampleCount++;
    return Future<bool>.value(_online);
  }

  @override
  Stream<bool> get onConnectivityChanged {
    subscribeCount++;
    return _controller.stream;
  }

  void emit({required bool online}) {
    _online = online;
    _controller.add(online);
  }

  Future<void> close() => _controller.close();
}

/// Deterministic retry policy: fixed delay, dead-letters on
/// non-retryable errors or after [maxAttempts].
final class FixedRetryPolicy implements RetryPolicy {
  FixedRetryPolicy({required this.delay, this.maxAttempts = 5});

  final Duration delay;
  final int maxAttempts;

  @override
  Duration computeDelay(int attemptCount, SyncError error) => delay;

  @override
  bool shouldDeadLetter(int attemptCount, SyncError error) =>
      !error.isRetryable || attemptCount >= maxAttempts;
}

/// Test-only delegating storage that can hold
/// [recoverInFlightOperations] open on a [Completer]. Used for the
/// disposal-during-recovery scenario; the production adapter API is
/// unchanged.
final class GatedStorage implements LocalStorageAdapter {
  GatedStorage(this._inner);

  final LocalStorageAdapter _inner;
  final Completer<void> recoverGate = Completer<void>();
  int recoverCalls = 0;

  @override
  Future<T> runInTransaction<T>(
    Future<T> Function(OutboxWriter outbox) action,
  ) =>
      _inner.runInTransaction(action);

  @override
  Future<int> recoverInFlightOperations() async {
    recoverCalls++;
    await recoverGate.future;
    return _inner.recoverInFlightOperations();
  }

  @override
  Future<List<SyncOperation>> fetchEligibleOperations({
    required DateTime now,
    required int limit,
    bool preservePerEntityOrder = true,
  }) =>
      _inner.fetchEligibleOperations(
        now: now,
        limit: limit,
        preservePerEntityOrder: preservePerEntityOrder,
      );

  @override
  Future<void> markInFlight(String operationId) =>
      _inner.markInFlight(operationId);

  @override
  Future<void> markSucceeded(String operationId) =>
      _inner.markSucceeded(operationId);

  @override
  Future<void> markFailed(
    String operationId, {
    required int attemptCount,
    required DateTime nextRetryAt,
    required SyncError error,
  }) =>
      _inner.markFailed(
        operationId,
        attemptCount: attemptCount,
        nextRetryAt: nextRetryAt,
        error: error,
      );

  @override
  Future<void> markDeadLetter(
    String operationId, {
    required int attemptCount,
    required SyncError error,
  }) =>
      _inner.markDeadLetter(
        operationId,
        attemptCount: attemptCount,
        error: error,
      );

  @override
  Stream<int> watchPendingCount() => _inner.watchPendingCount();
}

/// Harness bundling a real SQLite file, the real Sqflite adapter, a
/// real SyncEngine, and the deterministic fakes. [now] is a mutable
/// clock shared by the adapter and the engine so retry eligibility
/// can be advanced without real time.
final class Harness {
  Harness({SyncEngineConfig? config, RetryPolicy? retryPolicy})
      : _config = config,
        _retryPolicy = retryPolicy;

  /// Alternate constructor accepting a test-only storage wrapper
  /// (e.g. [GatedStorage]) applied to the real Sqflite adapter.
  factory Harness.wrapping(
    LocalStorageAdapter Function(SqfliteLocalStorageAdapter) wrap, {
    SyncEngineConfig? config,
    RetryPolicy? retryPolicy,
  }) {
    final h = Harness(config: config, retryPolicy: retryPolicy)
      .._wrapStorage = wrap;
    return h;
  }

  static final DateTime t0 = DateTime.utc(2026, 3, 1, 12);

  final SyncEngineConfig? _config;
  final RetryPolicy? _retryPolicy;
  LocalStorageAdapter Function(SqfliteLocalStorageAdapter)? _wrapStorage;

  late Database db;
  late SqfliteLocalStorageAdapter sqfliteStorage;
  late FakeRemote remote;
  late FakeConnectivity connectivity;
  late SyncEngine engine;

  /// Mutable clock shared by adapter and engine.
  DateTime now = t0;

  Future<void> open() async {
    db = await openIntegrationDb();
    // Remote and connectivity fakes persist across restarts so pushes
    // and connectivity state remain observable.
    remote = FakeRemote();
    connectivity = FakeConnectivity();
    _rebuild();
  }

  void _rebuild() {
    sqfliteStorage = SqfliteLocalStorageAdapter(
      database: db,
      clock: () => now,
    );
    final storage = _wrapStorage?.call(sqfliteStorage) ?? sqfliteStorage;
    engine = SyncEngine(
      localStorage: storage,
      remoteSync: remote,
      connectivity: connectivity,
      config: _config ?? SyncEngineConfig(syncOnStart: false),
      retryPolicy:
          _retryPolicy ?? FixedRetryPolicy(delay: const Duration(minutes: 10)),
      clock: () => now,
    );
  }

  /// Simulates a process restart: disposes engine and adapter, closes
  /// the database file, reopens it, and builds a fresh engine and
  /// adapter while preserving [remote], [connectivity], and [now].
  Future<void> restart() async {
    await engine.dispose();
    await sqfliteStorage.dispose();
    await db.close();
    db = await openIntegrationDb();
    _rebuild();
  }

  Future<void> dispose() async {
    await engine.dispose();
    await sqfliteStorage.dispose();
    try {
      await db.close();
    } catch (_) {
      // Already closed (e.g. after an explicit restart).
    }
    await connectivity.close();
  }

  Future<SyncOperation> enqueue({
    required String entityId,
    String entityType = 'tasks',
    SyncOperationType type = SyncOperationType.create,
    Map<String, dynamic>? payload,
    String? idempotencyKey,
  }) =>
      sqfliteStorage.runInTransaction(
        (outbox) => outbox.enqueue(
          LocalMutation(
            entityType: entityType,
            entityId: entityId,
            type: type,
            payload: payload ??
                (type == SyncOperationType.delete
                    ? null
                    : <String, dynamic>{'entityId': entityId}),
            idempotencyKey: idempotencyKey,
          ),
        ),
      );

  Future<Map<String, Object?>?> outboxRow(String operationId) async {
    final rows = await db.rawQuery(
      'SELECT * FROM sync_outbox WHERE operation_id = ?',
      <Object?>[operationId],
    );
    return rows.isEmpty ? null : rows.single;
  }

  Future<int> outboxCount() async {
    final rows = await db.rawQuery(
      'SELECT COUNT(*) AS c FROM sync_outbox',
    );
    return rows.single['c']! as int;
  }

  List<String> pushedIds() =>
      remote.pushed.map((op) => op.operationId).toList(growable: false);
}
