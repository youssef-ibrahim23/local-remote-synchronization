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

    test('mismatched operationId aborts cycle without transitions', () async {
      storage.operations.add(makeOperation('op-1'));
      remote.resultFor = (op) => SyncSuccess(operationId: 'different-id');
      await engine.start();

      final report = await engine.syncNow();

      // No outcome transition of any kind.
      expect(storage.succeededCalls, isEmpty);
      expect(storage.failedCalls, isEmpty);
      expect(storage.deadLetterCalls, isEmpty);
      expect(report.succeeded, 0);
      expect(report.failed, 0);
      expect(report.deadLettered, 0);
      expect(report.storageErrors, 0);
      // Cycle-level protocol error.
      expect(report.cycleError, isNotNull);
      expect(report.cycleError!.code, SyncErrorCode.unknown);
      expect(report.isSuccess, isFalse);
      // Operation remains inFlight (state treated as unknown).
      expect(storage.statusOf('op-1'), SyncOperationStatus.inFlight);
    });

    test('mismatch aborts after the current chunk settles', () async {
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
      // Future.wait settles the whole chunk before aborting.
      remote.gates['op-1']!.complete(
        SyncSuccess(operationId: 'wrong-id'),
      );
      remote.gates['op-2']!.complete(SyncSuccess(operationId: 'op-2'));

      final report = await reportFuture;
      expect(report.cycleError, isNotNull);
      expect(report.succeeded, 1); // only op-2
      expect(storage.succeededCalls, ['op-2']);
      // Cycle aborted — no further fetch iterations.
      expect(storage.fetchCalls, 1);
    });
  });

  group('Phase 3.1 interim failure handling', () {
    test('markSucceeded failure sets cycleError and aborts', () async {
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
      // op-1's failure is cycle-level, not operation-level.
      expect(report.cycleError, isNotNull);
      expect(report.cycleError!.code, SyncErrorCode.storage);
      expect(report.storageErrors, 0);
      expect(report.isSuccess, isFalse);
      // Cycle aborted — no second fetch even though the batch was full.
      expect(storage.fetchCalls, 1);
    });

    test('SyncFailure result aborts cycle as cycle-level error', () async {
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

      // Phase 3.1 has no retry scheduling: no markFailed/markDeadLetter.
      expect(storage.failedCalls, isEmpty);
      expect(storage.deadLetterCalls, isEmpty);
      expect(storage.succeededCalls, isEmpty);
      expect(report.cycleError, isNotNull);
      expect(report.isSuccess, isFalse);
      expect(report.failed, 0);
      expect(report.deadLettered, 0);
      expect(storage.statusOf('op-1'), SyncOperationStatus.inFlight);
    });

    test('SyncConflict result aborts cycle as cycle-level error', () async {
      storage.operations.add(makeOperation('op-1'));
      remote.resultFor = (op) => SyncConflict(
            operationId: op.operationId,
            remoteSnapshot: const {'remote': 'state'},
          );
      await engine.start();

      final report = await engine.syncNow();

      expect(storage.deadLetterCalls, isEmpty);
      expect(storage.failedCalls, isEmpty);
      expect(report.cycleError, isNotNull);
      expect(report.cycleError!.code, SyncErrorCode.conflict);
      expect(report.conflicted, 0);
      expect(report.isSuccess, isFalse);
    });

    test('remote push throw is wrapped and aborts cycle', () async {
      storage.operations.add(makeOperation('op-1'));
      remote.throwFor = (op) => StateError('adapter exploded');
      await engine.start();

      final report = await engine.syncNow();

      expect(report.cycleError, isNotNull);
      expect(report.isSuccess, isFalse);
      expect(storage.succeededCalls, isEmpty);
      expect(storage.statusOf('op-1'), SyncOperationStatus.inFlight);
    });
  });

  group('eligibility time', () {
    test('each fetch samples the clock independently', () async {
      final base = DateTime.utc(2024, 6, 1);
      var tick = 0;
      final clockReadings = <DateTime>[];
      DateTime fakeClock() {
        final value = base.add(Duration(minutes: tick));
        clockReadings.add(value);
        tick++;
        return value;
      }

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

      // fetch calls: op-1 batch, op-2 batch, empty batch — each got its
      // own clock reading (no fixed cycle-wide cutoff).
      expect(storage.fetchNows.length, 3);
      expect(storage.fetchNows[0], isNot(storage.fetchNows[1]));
      expect(storage.fetchNows[1], isNot(storage.fetchNows[2]));
      expect(storage.fetchNows[0].isBefore(storage.fetchNows[1]), isTrue);
      expect(storage.fetchNows[1].isBefore(storage.fetchNows[2]), isTrue);

      // The fetch times are distinct clock readings, not one cutoff.
      // startedAt consumed reading 0; the three fetches consumed
      // readings 1..3; completedAt consumed reading 4.
      expect(storage.fetchNows, [
        clockReadings[1],
        clockReadings[2],
        clockReadings[3],
      ]);
      expect(report.succeeded, 2);
    });
  });
}
