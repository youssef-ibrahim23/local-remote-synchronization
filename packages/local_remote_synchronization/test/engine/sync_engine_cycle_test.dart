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

  group('basic sync cycle', () {
    test('empty outbox produces a successful empty report', () async {
      await engine.start();
      final report = await engine.syncNow();

      expect(report.fetched, 0);
      expect(report.succeeded, 0);
      expect(report.failed, 0);
      expect(report.conflicted, 0);
      expect(report.deadLettered, 0);
      expect(report.storageErrors, 0);
      expect(report.cycleError, isNull);
      expect(report.skippedOffline, isFalse);
      expect(report.isSuccess, isTrue);
      expect(report.completedAt, isNotNull);
      expect(report.duration, greaterThanOrEqualTo(Duration.zero));
    });

    test('one successful operation is claimed, pushed, and removed', () async {
      storage.operations.add(makeOperation('op-1'));
      await engine.start();

      final report = await engine.syncNow();

      expect(report.fetched, 1);
      expect(report.succeeded, 1);
      expect(report.isSuccess, isTrue);
      expect(storage.inFlightCalls, ['op-1']);
      expect(storage.succeededCalls, ['op-1']);
      expect(remote.pushed.single.operationId, 'op-1');
      expect(storage.operations, isEmpty);
    });

    test('multiple batches are fetched and pushed', () async {
      engine = buildEngine(
        config: SyncEngineConfig(batchSize: 2, syncOnStart: false),
      );
      storage.operations.addAll([
        makeOperation('op-1'),
        makeOperation('op-2'),
        makeOperation('op-3'),
      ]);
      await engine.start();

      final report = await engine.syncNow();

      expect(report.fetched, 3);
      expect(report.succeeded, 3);
      expect(storage.fetchCalls, 2); // full batch + partial batch ends loop
      expect(remote.pushCalls, 3);
    });

    test('all operations in a chunk are claimed before any push', () async {
      engine = buildEngine(
        config: SyncEngineConfig(
            batchSize: 2, maxConcurrency: 2, syncOnStart: false),
      );
      storage.operations.addAll([
        makeOperation('op-1'),
        makeOperation('op-2'),
      ]);
      remote.gates['op-1'] = Completer<SyncResult>();
      remote.gates['op-2'] = Completer<SyncResult>();
      await engine.start();

      final reportFuture = engine.syncNow();
      await waitFor(() => remote.pushCalls == 2);

      final claimA = events.indexOf('markInFlight:op-1');
      final claimB = events.indexOf('markInFlight:op-2');
      final pushA = events.indexOf('push:op-1');
      final pushB = events.indexOf('push:op-2');

      expect(claimA, isNonNegative);
      expect(claimB, isNonNegative);
      expect(claimA, lessThan(pushA));
      expect(claimB, lessThan(pushA));
      expect(claimA, lessThan(pushB));
      expect(claimB, lessThan(pushB));

      remote.gates['op-1']!.complete(SyncSuccess(operationId: 'op-1'));
      remote.gates['op-2']!.complete(SyncSuccess(operationId: 'op-2'));
      final report = await reportFuture;
      expect(report.succeeded, 2);
    });

    test('maxConcurrency limits parallel pushes', () async {
      engine = buildEngine(
        config: SyncEngineConfig(
            batchSize: 3, maxConcurrency: 2, syncOnStart: false),
      );
      storage.operations.addAll([
        makeOperation('op-1'),
        makeOperation('op-2'),
        makeOperation('op-3'),
      ]);
      for (final id in ['op-1', 'op-2', 'op-3']) {
        remote.gates[id] = Completer<SyncResult>();
      }
      await engine.start();

      final reportFuture = engine.syncNow();
      // First chunk (op-1, op-2) starts concurrently; op-3 waits.
      await waitFor(() => remote.pushCalls == 2);
      expect(remote.pushed.map((op) => op.operationId),
          containsAll(['op-1', 'op-2']));

      remote.gates['op-1']!.complete(SyncSuccess(operationId: 'op-1'));
      remote.gates['op-2']!.complete(SyncSuccess(operationId: 'op-2'));
      // Second chunk starts once the first settles.
      await waitFor(() => remote.pushCalls == 3);
      remote.gates['op-3']!.complete(SyncSuccess(operationId: 'op-3'));

      final report = await reportFuture;
      expect(report.succeeded, 3);
    });

    test('report instance is shared between syncNow and reports stream',
        () async {
      storage.operations.add(makeOperation('op-1'));
      await engine.start();

      final emitted = engine.reports.first;
      final returned = engine.syncNow();
      final results = await Future.wait([emitted, returned]);

      expect(identical(results[0], results[1]), isTrue);
    });
  });

  group('markInFlight failures', () {
    Future<SyncCycleReport> runChunkedCycle({
      required List<String> ids,
      required String failingId,
    }) async {
      engine = buildEngine(
        config: SyncEngineConfig(
            batchSize: ids.length, maxConcurrency: 3, syncOnStart: false),
      );
      storage.operations.addAll(ids.map(makeOperation));
      storage.markInFlightFailures.add(failingId);
      await engine.start();
      return engine.syncNow();
    }

    test('first claim failure aborts before any push', () async {
      final report = await runChunkedCycle(
        ids: ['op-1', 'op-2', 'op-3'],
        failingId: 'op-1',
      );

      expect(remote.pushCalls, 0);
      expect(report.cycleError, isNotNull);
      expect(report.cycleError!.code, SyncErrorCode.storage);
      expect(report.isSuccess, isFalse);
      expect(report.succeeded, 0);
      // No operation was claimed.
      expect(storage.inFlightCalls, ['op-1']);
    });

    test('middle claim failure aborts before any push', () async {
      final report = await runChunkedCycle(
        ids: ['op-1', 'op-2', 'op-3'],
        failingId: 'op-2',
      );

      expect(remote.pushCalls, 0);
      expect(report.cycleError, isNotNull);
      expect(report.cycleError!.code, SyncErrorCode.storage);
      // op-1 was claimed before the failure; op-3 never claimed.
      expect(storage.inFlightCalls, ['op-1', 'op-2']);
      expect(storage.statusOf('op-1'), SyncOperationStatus.inFlight);
      expect(storage.statusOf('op-3'), SyncOperationStatus.pending);
    });

    test('last claim failure aborts before any push', () async {
      final report = await runChunkedCycle(
        ids: ['op-1', 'op-2', 'op-3'],
        failingId: 'op-3',
      );

      expect(remote.pushCalls, 0);
      expect(report.cycleError, isNotNull);
      expect(storage.inFlightCalls, ['op-1', 'op-2', 'op-3']);
      expect(storage.statusOf('op-1'), SyncOperationStatus.inFlight);
      expect(storage.statusOf('op-2'), SyncOperationStatus.inFlight);
    });

    test('fetch failure sets cycleError and aborts', () async {
      storage.fetchError = StateError('fetch failed');
      await engine.start();

      final report = await engine.syncNow();

      expect(report.cycleError, isNotNull);
      expect(report.cycleError!.code, SyncErrorCode.storage);
      expect(report.isSuccess, isFalse);
      expect(remote.pushCalls, 0);
    });
  });

  group('result validation', () {
    test('matching operationId succeeds', () async {
      storage.operations.add(makeOperation('op-1'));
      remote.resultFor = (op) => SyncSuccess(
            operationId: op.operationId,
            remoteVersion: 'v1',
          );
      await engine.start();

      final report = await engine.syncNow();
      expect(report.succeeded, 1);
      expect(storage.operations, isEmpty);
    });

    test('mismatched operationId is dead-lettered without success/failure',
        () async {
      storage.operations.add(makeOperation('op-1'));
      remote.resultFor = (op) => SyncSuccess(operationId: 'different-id');
      await engine.start();

      final report = await engine.syncNow();

      // Phase 3.2: protocol violation -> markDeadLetter only.
      expect(storage.succeededCalls, isEmpty);
      expect(storage.failedCalls, isEmpty);
      expect(storage.deadLetterCalls, ['op-1']);
      expect(storage.deadLetterTransitions.single.attemptCount, 1);
      expect(report.succeeded, 0);
      expect(report.failed, 0);
      expect(report.deadLettered, 1);
      expect(report.storageErrors, 0);
      expect(report.cycleError, isNull);
      expect(report.isSuccess, isFalse);
      expect(storage.statusOf('op-1'), SyncOperationStatus.deadLetter);
    });

    test('mismatch settles the chunk and the cycle continues', () async {
      engine = buildEngine(
        config: SyncEngineConfig(
            batchSize: 2, maxConcurrency: 2, syncOnStart: false),
      );
      storage.operations.addAll([
        makeOperation('op-1'),
        makeOperation('op-2'),
      ]);
      remote.gates['op-1'] = Completer<SyncResult>();
      remote.gates['op-2'] = Completer<SyncResult>();
      remote.resultFor = (op) => op.operationId == 'op-1'
          ? SyncSuccess(operationId: 'wrong-id')
          : SyncSuccess(operationId: op.operationId);
      await engine.start();

      final reportFuture = engine.syncNow();
      await waitFor(() => remote.pushCalls == 2);

      // op-1 mismatches; op-2 still completes its transition because
      // Future.wait settles the whole chunk.
      remote.gates['op-1']!.complete(
        SyncSuccess(operationId: 'wrong-id'),
      );
      remote.gates['op-2']!.complete(SyncSuccess(operationId: 'op-2'));

      final report = await reportFuture;
      // No cycle abort: the mismatch is an operation-level outcome.
      expect(report.cycleError, isNull);
      expect(report.succeeded, 1); // only op-2
      expect(report.deadLettered, 1); // op-1 protocol violation
      expect(storage.succeededCalls, ['op-2']);
      expect(storage.deadLetterCalls, ['op-1']);
      // Cycle continued — the full batch triggered a second fetch.
      expect(storage.fetchCalls, 2);
    });
  });

  group('Phase 3.2 failure handling', () {
    test('markSucceeded failure counts storageErrors and continues', () async {
      engine = buildEngine(
        config: SyncEngineConfig(
            batchSize: 2, maxConcurrency: 2, syncOnStart: false),
      );
      storage.operations.addAll([
        makeOperation('op-1'),
        makeOperation('op-2'),
      ]);
      storage.markSucceededFailures.add('op-1');
      remote.gates['op-1'] = Completer<SyncResult>();
      remote.gates['op-2'] = Completer<SyncResult>();
      await engine.start();

      final reportFuture = engine.syncNow();
      await waitFor(() => remote.pushCalls == 2);

      remote.gates['op-1']!.complete(SyncSuccess(operationId: 'op-1'));
      remote.gates['op-2']!.complete(SyncSuccess(operationId: 'op-2'));

      final report = await reportFuture;

      // Chunk settled: op-2's transition completed.
      expect(storage.succeededCalls, contains('op-2'));
      expect(report.succeeded, 1);
      // op-1's transition failure is operation-level, not cycle-level.
      expect(report.cycleError, isNull);
      expect(report.storageErrors, 1);
      expect(report.isSuccess, isFalse);
      // Cycle continued — the full batch triggered a second fetch.
      expect(storage.fetchCalls, 2);
    });

    test('SyncFailure result schedules retry via markFailed', () async {
      storage.operations.add(makeOperation('op-1'));
      remote.resultFor = (op) => SyncFailure(
            operationId: op.operationId,
            error: SyncError(
              code: SyncErrorCode.network,
              message: 'network down',
              isRetryable: true,
            ),
          );
      await engine.start();

      final report = await engine.syncNow();

      expect(storage.failedCalls, ['op-1']);
      expect(storage.deadLetterCalls, isEmpty);
      expect(storage.succeededCalls, isEmpty);
      expect(report.cycleError, isNull);
      expect(report.isSuccess, isFalse);
      expect(report.failed, 1);
      expect(report.deadLettered, 0);
      expect(storage.statusOf('op-1'), SyncOperationStatus.failed);
    });

    test('SyncConflict result is dead-lettered and emits conflict event',
        () async {
      storage.operations.add(makeOperation('op-1'));
      remote.resultFor = (op) => SyncConflict(
            operationId: op.operationId,
            remoteSnapshot: const {'remote': 'state'},
          );
      await engine.start();

      final conflictFuture = engine.conflicts.first;
      final report = await engine.syncNow();

      // Phase 3.3: conflict is dead-lettered, NOT a cycle-level abort.
      expect(report.cycleError, isNull);
      expect(report.conflicted, 1);
      expect(report.deadLettered, 1);
      expect(report.storageErrors, 0);
      expect(report.isSuccess, isFalse);
      expect(storage.deadLetterCalls, ['op-1']);
      expect(storage.failedCalls, isEmpty);
      expect(storage.statusOf('op-1'), SyncOperationStatus.deadLetter);

      final event = await conflictFuture;
      expect(event.operation.operationId, 'op-1');
      expect(event.conflict.remoteSnapshot, {'remote': 'state'});
    });

    test('remote push throw is converted to retryable SyncFailure', () async {
      storage.operations.add(makeOperation('op-1'));
      remote.throwFor = (op) => StateError('adapter exploded');
      await engine.start();

      final report = await engine.syncNow();

      // Unexpected push errors become retryable SyncFailures handled by
      // the retry policy — not cycle-level errors.
      expect(report.cycleError, isNull);
      expect(report.failed, 1);
      expect(storage.failedCalls, ['op-1']);
      expect(storage.statusOf('op-1'), SyncOperationStatus.failed);
    });
  });

  group('eligibility cutoff', () {
    test('all fetches in a cycle use the same fixed cutoff', () async {
      final base = DateTime.utc(2024, 6, 1);
      var tick = 0;
      DateTime fakeClock() => base.add(Duration(minutes: tick++));

      engine = buildEngine(
        config: SyncEngineConfig(batchSize: 1, syncOnStart: false),
        clock: fakeClock,
      );
      storage.operations.addAll([
        makeOperation('op-1'),
        makeOperation('op-2'),
      ]);
      await engine.start();
      final report = await engine.syncNow();

      // Phase 3.2: every fetch receives the identical cycle-start
      // cutoff, regardless of later clock readings.
      expect(storage.fetchNows.length, 3);
      expect(storage.fetchNows.toSet().length, 1);
      expect(storage.fetchNows.first, report.startedAt);
      expect(report.succeeded, 2);
    });
  });
}
