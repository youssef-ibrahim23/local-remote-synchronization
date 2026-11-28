import 'dart:async';

import 'package:local_remote_synchronization/local_remote_synchronization.dart';
import 'package:test/test.dart';

import 'fake_adapters.dart';

void main() {
  late List<String> events;
  late FakeLocalStorageAdapter storage;
  late FakeRemoteSyncAdapter remote;
  late FakeConnectivityProvider connectivity;
  late SyncEngine engine;

  SyncEngine buildEngine({
    SyncEngineConfig? config,
    RetryPolicy? retryPolicy,
    DateTime Function()? clock,
  }) {
    events = <String>[];
    storage = FakeLocalStorageAdapter(events: events);
    remote = FakeRemoteSyncAdapter(events: events);
    connectivity = FakeConnectivityProvider(events: events);
    return SyncEngine(
      localStorage: storage,
      remoteSync: remote,
      connectivity: connectivity,
      config: config ?? SyncEngineConfig(syncOnStart: false),
      retryPolicy: retryPolicy,
      clock: clock,
    );
  }

  setUp(() {
    engine = buildEngine();
  });

  tearDown(() async {
    await engine.dispose();
    await connectivity.close();
  });

  // ---------------------------------------------------------------------------
  // A. Successful conflict handling
  // ---------------------------------------------------------------------------
  group('A. successful conflict handling', () {
    test('valid conflict is dead-lettered and counters are updated', () async {
      storage.operations.add(makeOperation('op-1'));
      remote.resultFor = (op) => SyncConflict(
            operationId: op.operationId,
            remoteSnapshot: const {'key': 'remote-value'},
            remoteVersion: 'v2',
          );

      await engine.start();
      final conflictFuture = engine.conflicts.first;
      final report = await engine.syncNow();

      // markDeadLetter must have been called once with the right arguments.
      expect(storage.deadLetterCalls, ['op-1']);
      expect(storage.deadLetterTransitions.single.operationId, 'op-1');
      // attemptCount must be original (0) + 1.
      expect(storage.deadLetterTransitions.single.attemptCount, 1);

      // Report counters.
      expect(report.deadLettered, 1);
      expect(report.conflicted, 1);
      expect(report.storageErrors, 0);
      expect(report.succeeded, 0);
      expect(report.failed, 0);
      expect(report.cycleError, isNull);
      expect(report.isSuccess, isFalse);

      // The operation must be marked deadLetter in the outbox.
      expect(storage.statusOf('op-1'), SyncOperationStatus.deadLetter);

      // No retry policy must have been invoked.
      // (The default ExponentialBackoffRetryPolicy never gets called for
      // conflicts — but we can verify via storage that no markFailed occurred.)
      expect(storage.failedCalls, isEmpty);

      // The conflict event was emitted.
      final event = await conflictFuture;
      expect(event.operation.operationId, 'op-1');
      expect(event.conflict.remoteSnapshot, {'key': 'remote-value'});
    });

    test('error stored in markDeadLetter has conflict code and is not retryable',
        () async {
      // We capture the SyncError passed to markDeadLetter by wrapping the fake.
      SyncError? capturedError;
      storage.operations.add(makeOperation('op-1'));
      remote.resultFor = (op) => SyncConflict(
            operationId: op.operationId,
            remoteSnapshot: const {'x': 1},
            remoteVersion: 'rv-99',
          );

      // Extend FakeLocalStorageAdapter via composition to intercept markDeadLetter.
      final interceptStorage = _InterceptingStorage(storage, onDeadLetter: (e) {
        capturedError = e;
      });
      engine = SyncEngine(
        localStorage: interceptStorage,
        remoteSync: remote,
        connectivity: connectivity,
        config: SyncEngineConfig(syncOnStart: false),
      );

      await engine.start();
      await engine.syncNow();

      expect(capturedError, isNotNull);
      expect(capturedError!.code, SyncErrorCode.conflict);
      expect(capturedError!.isRetryable, isFalse);
      // remoteVersion is embedded in the message.
      expect(capturedError!.message, contains('rv-99'));
    });

    test(
        'SyncConflict does not invoke retry policy (no shouldDeadLetter / computeDelay calls)',
        () async {
      final fakePolicy = _RecordingRetryPolicy();
      engine = buildEngine(retryPolicy: fakePolicy);

      storage.operations.add(makeOperation('op-1'));
      remote.resultFor = (op) => SyncConflict(
            operationId: op.operationId,
            remoteSnapshot: const {'x': 1},
          );

      await engine.start();
      await engine.syncNow();

      expect(fakePolicy.shouldDeadLetterCalls, isEmpty);
      expect(fakePolicy.computeDelayCalls, isEmpty);
    });
  });

  // ---------------------------------------------------------------------------
  // B. Persistence-before-event ordering
  // ---------------------------------------------------------------------------
  group('B. persistence-before-event ordering', () {
    test('no event is emitted before markDeadLetter completes', () async {
      final markDeadLetterGate = Completer<void>();
      final conflictEvents = <SyncConflictEvent>[];

      // We use a gate on markDeadLetter to freeze it mid-way.
      storage.operations.add(makeOperation('op-1'));
      remote.resultFor = (op) => SyncConflict(
            operationId: op.operationId,
            remoteSnapshot: const {'x': 1},
          );

      // We capture events on the stream.
      engine.conflicts.listen(conflictEvents.add);

      // Replace storage to introduce a gate.
      final gatedStorage = _GatedDeadLetterStorage(storage, markDeadLetterGate);
      engine = SyncEngine(
        localStorage: gatedStorage,
        remoteSync: remote,
        connectivity: connectivity,
        config: SyncEngineConfig(syncOnStart: false),
      );
      // Re-seed after rebuilding.
      storage.operations.add(makeOperation('op-1'));
      engine.conflicts.listen(conflictEvents.add);

      await engine.start();
      final reportFuture = engine.syncNow();

      // Allow the cycle to progress until it hits the gate.
      await waitFor(() => gatedStorage.gateHit);

      // Before the gate is released, no event must have been emitted.
      expect(conflictEvents, isEmpty,
          reason: 'No event before markDeadLetter resolves');

      // Now release the gate.
      markDeadLetterGate.complete();
      await reportFuture;

      // After persistence completes the event is emitted.
      expect(conflictEvents, hasLength(1));
    });

    test('emitted event carries the correct operation and conflict', () async {
      final conflictResult = SyncConflict(
        operationId: 'op-1',
        remoteSnapshot: const {'field': 'value', 'num': 42},
        remoteVersion: 'etag-abc',
      );
      storage.operations.add(makeOperation('op-1'));
      remote.resultFor = (_) => conflictResult;

      await engine.start();
      final conflictFuture = engine.conflicts.first;
      await engine.syncNow();

      final event = await conflictFuture;
      expect(event.operation.operationId, 'op-1');
      // The conflict result is the same object returned by the adapter.
      expect(identical(event.conflict, conflictResult), isTrue);
    });
  });

  // ---------------------------------------------------------------------------
  // C. Storage transition failure
  // ---------------------------------------------------------------------------
  group('C. storage transition failure', () {
    test('markDeadLetter failure counts storageErrors and no event is emitted',
        () async {
      storage.operations.add(makeOperation('op-1'));
      storage.markDeadLetterFailures.add('op-1');
      remote.resultFor = (op) => SyncConflict(
            operationId: op.operationId,
            remoteSnapshot: const {'x': 1},
          );

      final conflictEvents = <SyncConflictEvent>[];
      engine.conflicts.listen(conflictEvents.add);

      await engine.start();
      final report = await engine.syncNow();

      expect(report.storageErrors, 1);
      expect(report.conflicted, 0);
      expect(report.deadLettered, 0);
      expect(report.cycleError, isNull,
          reason: 'Storage failure is operation-level, not cycle-level');
      expect(conflictEvents, isEmpty,
          reason: 'No event when persistence fails');
    });

    test('cycle continues after a conflict storage failure', () async {
      engine = buildEngine(
        config: SyncEngineConfig(batchSize: 2, maxConcurrency: 2, syncOnStart: false),
      );
      storage.operations.addAll([
        makeOperation('op-1'),
        makeOperation('op-2'),
      ]);
      storage.markDeadLetterFailures.add('op-1');
      remote.resultFor = (op) => op.operationId == 'op-1'
          ? SyncConflict(operationId: op.operationId, remoteSnapshot: const {'x': 1})
          : SyncSuccess(operationId: op.operationId);

      await engine.start();
      final report = await engine.syncNow();

      // op-1 storage failure, op-2 succeeds.
      expect(report.storageErrors, 1);
      expect(report.succeeded, 1);
      expect(report.conflicted, 0);
      expect(report.cycleError, isNull);
    });
  });

  // ---------------------------------------------------------------------------
  // D. Pre-transition snapshot
  // ---------------------------------------------------------------------------
  group('D. pre-transition snapshot', () {
    test('event.operation.status is pending (original fetch-time status)',
        () async {
      // makeOperation default status is pending.
      storage.operations.add(makeOperation('op-1'));
      remote.resultFor = (op) => SyncConflict(
            operationId: op.operationId,
            remoteSnapshot: const {'x': 1},
          );

      await engine.start();
      final conflictFuture = engine.conflicts.first;
      await engine.syncNow();

      final event = await conflictFuture;
      expect(event.operation.status, SyncOperationStatus.pending,
          reason: 'The event carries the pre-transition status');
    });

    test('event.operation.status is failed when fetched as failed op', () async {
      // Seed a failed operation (retryAt in the past = eligible).
      storage.operations.add(
        makeOperation(
          'op-failed',
          attemptCount: 2,
          nextRetryAt: DateTime.utc(2020, 1, 1), // already past
        ),
      );
      // Force its status to failed so it appears eligible.
      storage.operations.last = SyncOperation(
        operationId: 'op-failed',
        idempotencyKey: 'idem-op-failed',
        entityType: 'task',
        entityId: 'entity-op-failed',
        type: SyncOperationType.update,
        createdAt: DateTime.utc(2024, 1, 1),
        attemptCount: 2,
        nextRetryAt: DateTime.utc(2020, 1, 1),
        payload: const {'value': 'op-failed'},
        status: SyncOperationStatus.failed,
      );
      remote.resultFor = (op) => SyncConflict(
            operationId: op.operationId,
            remoteSnapshot: const {'x': 1},
          );

      await engine.start();
      final conflictFuture = engine.conflicts.first;
      await engine.syncNow();

      final event = await conflictFuture;
      expect(event.operation.operationId, 'op-failed');
      // The event reflects the status at fetch time, not deadLetter.
      expect(event.operation.status, SyncOperationStatus.failed);

      // But the outbox record must be deadLetter after persistence.
      expect(storage.statusOf('op-failed'), SyncOperationStatus.deadLetter);
    });

    test('event holds original immutable operation reference', () async {
      final original = makeOperation('op-1');
      storage.operations.add(original);
      remote.resultFor = (op) => SyncConflict(
            operationId: op.operationId,
            remoteSnapshot: const {'x': 1},
          );

      await engine.start();
      final conflictFuture = engine.conflicts.first;
      await engine.syncNow();

      final event = await conflictFuture;
      // The same object that was seeded must be in the event.
      expect(identical(event.operation, original), isTrue);
    });
  });

  // ---------------------------------------------------------------------------
  // E. Multiple conflicts in one cycle
  // ---------------------------------------------------------------------------
  group('E. multiple conflicts in one cycle', () {
    test('each conflict is persisted separately and each produces one event',
        () async {
      engine = buildEngine(
        config: SyncEngineConfig(
            batchSize: 3, maxConcurrency: 3, syncOnStart: false),
      );
      storage.operations.addAll([
        makeOperation('op-1'),
        makeOperation('op-2'),
        makeOperation('op-3'),
      ]);
      remote.resultFor = (op) => SyncConflict(
            operationId: op.operationId,
            remoteSnapshot: {'id': op.operationId},
          );

      final receivedEvents = <SyncConflictEvent>[];
      engine.conflicts.listen(receivedEvents.add);

      await engine.start();
      final report = await engine.syncNow();

      expect(report.conflicted, 3);
      expect(report.deadLettered, 3);
      expect(report.storageErrors, 0);
      expect(storage.deadLetterCalls,
          unorderedEquals(['op-1', 'op-2', 'op-3']));
      expect(receivedEvents, hasLength(3));
      expect(
        receivedEvents.map((e) => e.operation.operationId),
        unorderedEquals(['op-1', 'op-2', 'op-3']),
      );
    });

    test('mixed result cycle: some conflicts, some successes', () async {
      engine = buildEngine(
        config: SyncEngineConfig(
            batchSize: 3, maxConcurrency: 3, syncOnStart: false),
      );
      storage.operations.addAll([
        makeOperation('op-1'),
        makeOperation('op-2'),
        makeOperation('op-3'),
      ]);
      remote.resultFor = (op) => switch (op.operationId) {
            'op-2' => SyncConflict(
                operationId: op.operationId,
                remoteSnapshot: const {'x': 1},
              ),
            _ => SyncSuccess(operationId: op.operationId),
          };

      final receivedEvents = <SyncConflictEvent>[];
      engine.conflicts.listen(receivedEvents.add);

      await engine.start();
      final report = await engine.syncNow();

      expect(report.succeeded, 2);
      expect(report.conflicted, 1);
      expect(report.deadLettered, 1);
      expect(report.storageErrors, 0);
      expect(receivedEvents, hasLength(1));
      expect(receivedEvents.first.operation.operationId, 'op-2');
    });
  });

  // ---------------------------------------------------------------------------
  // F. Broadcast stream semantics
  // ---------------------------------------------------------------------------
  group('F. broadcast stream semantics', () {
    test('conflicts stream is a broadcast stream', () {
      expect(engine.conflicts.isBroadcast, isTrue);
    });

    test('multiple listeners each receive the conflict event', () async {
      storage.operations.add(makeOperation('op-1'));
      remote.resultFor = (op) => SyncConflict(
            operationId: op.operationId,
            remoteSnapshot: const {'x': 1},
          );

      final listener1 = <SyncConflictEvent>[];
      final listener2 = <SyncConflictEvent>[];
      engine.conflicts.listen(listener1.add);
      engine.conflicts.listen(listener2.add);

      await engine.start();
      await engine.syncNow();

      expect(listener1, hasLength(1));
      expect(listener2, hasLength(1));
      expect(listener1.first.operation.operationId, 'op-1');
      expect(listener2.first.operation.operationId, 'op-1');
    });

    test('event emitted with no listener is not replayed to a later listener',
        () async {
      storage.operations.add(makeOperation('op-1'));
      remote.resultFor = (op) => SyncConflict(
            operationId: op.operationId,
            remoteSnapshot: const {'x': 1},
          );

      await engine.start();
      // Trigger cycle with NO listener attached.
      await engine.syncNow();

      // Add a listener only AFTER the event would have been emitted.
      final lateEvents = <SyncConflictEvent>[];
      engine.conflicts.listen(lateEvents.add);

      // Allow microtasks to settle.
      await Future<void>.delayed(Duration.zero);

      expect(lateEvents, isEmpty,
          reason: 'Broadcast stream does not replay past events');
    });
  });

  // ---------------------------------------------------------------------------
  // G. Disposal
  // ---------------------------------------------------------------------------
  group('G. disposal', () {
    test('dispose closes the conflicts stream', () async {
      await engine.start();
      final done = Completer<void>();
      engine.conflicts.listen(null, onDone: done.complete);

      await engine.dispose();

      // The stream should have closed (onDone fires).
      await done.future.timeout(const Duration(seconds: 2));
    });

    test('dispose waits for active cycle before closing stream', () async {
      storage.operations.add(makeOperation('op-1'));
      final gate = Completer<SyncResult>();
      remote.gates['op-1'] = gate;
      remote.resultFor = (op) => SyncConflict(
            operationId: op.operationId,
            remoteSnapshot: const {'x': 1},
          );

      final receivedEvents = <SyncConflictEvent>[];
      engine.conflicts.listen(receivedEvents.add);

      await engine.start();
      final reportFuture = engine.syncNow();

      // Wait until the push is in progress.
      await waitFor(() => remote.pushCalls == 1);

      // Start disposal while the cycle is still running.
      final disposeFuture = engine.dispose();

      // Release the gate — the cycle completes and emits the conflict.
      gate.complete(SyncConflict(
        operationId: 'op-1',
        remoteSnapshot: const {'x': 1},
      ));

      await reportFuture;
      await disposeFuture;

      // The event was emitted before stream closure.
      expect(receivedEvents, hasLength(1));
    });

    test('no event is emitted after stream is closed', () async {
      await engine.start();
      await engine.dispose();

      final lateEvents = <SyncConflictEvent>[];
      // Attempting to listen after closure should not receive events.
      try {
        engine.conflicts.listen(lateEvents.add);
      } catch (_) {
        // A closed broadcast stream may throw on listen — that's acceptable.
      }

      await Future<void>.delayed(Duration.zero);
      expect(lateEvents, isEmpty);
    });

    test('repeated dispose calls are safe', () async {
      await engine.start();
      await Future.wait([engine.dispose(), engine.dispose(), engine.dispose()]);
      // No exception means idempotent disposal works.
    });
  });

  // ---------------------------------------------------------------------------
  // H. Operation ID mismatch with SyncConflict
  // ---------------------------------------------------------------------------
  group('H. operationId mismatch with SyncConflict', () {
    test(
        'SyncConflict with mismatched operationId follows protocol-violation path',
        () async {
      storage.operations.add(makeOperation('op-1'));
      // Return a SyncConflict with a DIFFERENT operationId.
      remote.resultFor = (op) => SyncConflict(
            operationId: 'different-id',
            remoteSnapshot: const {'x': 1},
          );

      final conflictEvents = <SyncConflictEvent>[];
      engine.conflicts.listen(conflictEvents.add);

      await engine.start();
      final report = await engine.syncNow();

      // No conflict event — this is a protocol violation, not a true conflict.
      expect(conflictEvents, isEmpty);
      // No conflicted counter.
      expect(report.conflicted, 0);
      // The mismatch is treated as a protocol-violation dead-letter.
      expect(report.deadLettered, 1);
      expect(report.storageErrors, 0);
      // No failed / succeeded.
      expect(report.failed, 0);
      expect(report.succeeded, 0);
      expect(report.cycleError, isNull);
      expect(storage.statusOf('op-1'), SyncOperationStatus.deadLetter);
    });

    test(
        'SyncConflict mismatch does not invoke retry policy',
        () async {
      final fakePolicy = _RecordingRetryPolicy();
      engine = buildEngine(retryPolicy: fakePolicy);

      storage.operations.add(makeOperation('op-1'));
      remote.resultFor = (op) => SyncConflict(
            operationId: 'wrong-id',
            remoteSnapshot: const {'x': 1},
          );

      await engine.start();
      await engine.syncNow();

      expect(fakePolicy.shouldDeadLetterCalls, isEmpty);
      expect(fakePolicy.computeDelayCalls, isEmpty);
    });
  });

  // ---------------------------------------------------------------------------
  // I. Regression — existing Phase 3.1 and 3.2 behavior preserved
  // ---------------------------------------------------------------------------
  group('I. regression — Phase 3.1 and 3.2 behavior preserved', () {
    test('successful operations still work after Phase 3.3 changes', () async {
      storage.operations.add(makeOperation('op-1'));
      await engine.start();
      final report = await engine.syncNow();

      expect(report.succeeded, 1);
      expect(report.isSuccess, isTrue);
      expect(storage.succeededCalls, ['op-1']);
    });

    test('SyncFailure still retries via RetryPolicy', () async {
      storage.operations.add(makeOperation('op-1'));
      remote.resultFor = (op) => SyncFailure(
            operationId: op.operationId,
            error: SyncError(
              code: SyncErrorCode.network,
              message: 'net fail',
              isRetryable: true,
            ),
          );

      await engine.start();
      final report = await engine.syncNow();

      expect(report.failed, 1);
      expect(report.conflicted, 0);
      expect(storage.failedCalls, ['op-1']);
    });

    test('protocol violation still dead-letters without conflict event',
        () async {
      storage.operations.add(makeOperation('op-1'));
      remote.resultFor = (op) => SyncSuccess(operationId: 'wrong-id');

      final conflictEvents = <SyncConflictEvent>[];
      engine.conflicts.listen(conflictEvents.add);

      await engine.start();
      final report = await engine.syncNow();

      expect(conflictEvents, isEmpty);
      expect(report.deadLettered, 1);
      expect(report.conflicted, 0);
      expect(storage.statusOf('op-1'), SyncOperationStatus.deadLetter);
    });

    test('markInFlight failure still aborts cycle', () async {
      storage.operations.add(makeOperation('op-1'));
      storage.markInFlightFailures.add('op-1');

      await engine.start();
      final report = await engine.syncNow();

      expect(report.cycleError, isNotNull);
      expect(report.cycleError!.code, SyncErrorCode.storage);
      expect(remote.pushCalls, 0);
    });

    test('markSucceeded failure counts storageErrors', () async {
      storage.operations.add(makeOperation('op-1'));
      storage.markSucceededFailures.add('op-1');

      await engine.start();
      final report = await engine.syncNow();

      expect(report.storageErrors, 1);
      expect(report.succeeded, 0);
      expect(report.cycleError, isNull);
    });
  });
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// [RetryPolicy] that records all calls without actually doing policy work.
final class _RecordingRetryPolicy implements RetryPolicy {
  final List<int> shouldDeadLetterCalls = <int>[];
  final List<int> computeDelayCalls = <int>[];

  @override
  Duration computeDelay(int attemptCount, SyncError error) {
    computeDelayCalls.add(attemptCount);
    return const Duration(minutes: 1);
  }

  @override
  bool shouldDeadLetter(int attemptCount, SyncError error) {
    shouldDeadLetterCalls.add(attemptCount);
    return !error.isRetryable;
  }
}

/// [LocalStorageAdapter] that wraps another adapter and intercepts
/// [markDeadLetter] to capture the [SyncError] passed.
final class _InterceptingStorage implements LocalStorageAdapter {
  _InterceptingStorage(this._inner, {required this.onDeadLetter});

  final LocalStorageAdapter _inner;
  final void Function(SyncError error) onDeadLetter;

  @override
  Future<T> runInTransaction<T>(
    Future<T> Function(OutboxWriter outbox) action,
  ) =>
      _inner.runInTransaction(action);

  @override
  Future<int> recoverInFlightOperations() => _inner.recoverInFlightOperations();

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
  }) {
    onDeadLetter(error);
    return _inner.markDeadLetter(
      operationId,
      attemptCount: attemptCount,
      error: error,
    );
  }

  @override
  Stream<int> watchPendingCount() => _inner.watchPendingCount();
}

/// [LocalStorageAdapter] that gates [markDeadLetter] on a [Completer],
/// letting tests prove ordering guarantees.
final class _GatedDeadLetterStorage implements LocalStorageAdapter {
  _GatedDeadLetterStorage(this._inner, this._gate);

  final LocalStorageAdapter _inner;
  final Completer<void> _gate;
  bool gateHit = false;

  @override
  Future<T> runInTransaction<T>(
    Future<T> Function(OutboxWriter outbox) action,
  ) =>
      _inner.runInTransaction(action);

  @override
  Future<int> recoverInFlightOperations() => _inner.recoverInFlightOperations();

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
  }) async {
    gateHit = true;
    await _gate.future;
    return _inner.markDeadLetter(
      operationId,
      attemptCount: attemptCount,
      error: error,
    );
  }

  @override
  Stream<int> watchPendingCount() => _inner.watchPendingCount();
}
