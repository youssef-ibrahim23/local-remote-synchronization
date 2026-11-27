import 'dart:async';

import 'package:local_remote_synchronization/local_remote_synchronization.dart';
import 'package:test/test.dart';

import 'fake_adapters.dart';

/// Deterministic [RetryPolicy] for engine tests. Records calls and can
/// be scripted to return fixed delays or throw.
final class FakeRetryPolicy implements RetryPolicy {
  /// Delay returned by [computeDelay]. Default: 1 minute.
  Duration delay = const Duration(minutes: 1);

  /// When true, [shouldDeadLetter] always returns false.
  bool forceRetry = false;

  /// When non-null, thrown by [shouldDeadLetter].
  Object? shouldDeadLetterError;

  /// When non-null, thrown by [computeDelay].
  Object? computeDelayError;

  final List<({int attemptCount, SyncError error})> shouldDeadLetterCalls =
      <({int attemptCount, SyncError error})>[];
  final List<({int attemptCount, SyncError error})> computeDelayCalls =
      <({int attemptCount, SyncError error})>[];

  @override
  Duration computeDelay(int attemptCount, SyncError error) {
    computeDelayCalls.add((attemptCount: attemptCount, error: error));
    final thrown = computeDelayError;
    if (thrown != null) {
      // ignore: only_throw_errors
      throw thrown;
    }
    return delay;
  }

  @override
  bool shouldDeadLetter(int attemptCount, SyncError error) {
    shouldDeadLetterCalls.add((attemptCount: attemptCount, error: error));
    final thrown = shouldDeadLetterError;
    if (thrown != null) {
      // ignore: only_throw_errors
      throw thrown;
    }
    if (forceRetry) {
      return false;
    }
    return !error.isRetryable;
  }
}

SyncFailure networkFailure(String operationId,
        {bool isRetryable = true, Duration? retryAfter}) =>
    SyncFailure(
      operationId: operationId,
      error: SyncError(
        code: SyncErrorCode.network,
        message: 'network failure',
        isRetryable: isRetryable,
        retryAfter: retryAfter,
      ),
    );

void main() {
  late List<String> events;
  late FakeLocalStorageAdapter storage;
  late FakeRemoteSyncAdapter remote;
  late FakeConnectivityProvider connectivity;
  late FakeRetryPolicy retryPolicy;
  late SyncEngine engine;

  SyncEngine buildEngine({
    SyncEngineConfig? config,
    RetryPolicy? policy,
    DateTime Function()? clock,
  }) {
    events = <String>[];
    storage = FakeLocalStorageAdapter(events: events);
    remote = FakeRemoteSyncAdapter(events: events);
    connectivity = FakeConnectivityProvider(events: events);
    retryPolicy = FakeRetryPolicy();
    return SyncEngine(
      localStorage: storage,
      remoteSync: remote,
      connectivity: connectivity,
      config: config ?? SyncEngineConfig(syncOnStart: false),
      retryPolicy: policy ?? retryPolicy,
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

  group('retry integration', () {
    test(
        'retryable SyncFailure calls markFailed with incremented '
        'attemptCount and clock-based nextRetryAt', () async {
      final t0 = DateTime.utc(2024, 3, 1, 12);
      var clockCalls = 0;
      DateTime fixedClock() {
        // Cycle start, markFailed base, and completedAt all read t0.
        clockCalls++;
        return t0;
      }

      engine = buildEngine(clock: fixedClock);
      retryPolicy.delay = const Duration(seconds: 90);
      storage.operations.add(makeOperation('op-1', attemptCount: 4));
      remote.resultFor = (op) => networkFailure(op.operationId);
      await engine.start();

      final report = await engine.syncNow();

      // Policy saw the completed-attempt count: 4 + 1.
      expect(retryPolicy.shouldDeadLetterCalls.single.attemptCount, 5);
      expect(retryPolicy.computeDelayCalls.single.attemptCount, 5);

      expect(storage.failedCalls, ['op-1']);
      expect(storage.deadLetterCalls, isEmpty);
      final transition = storage.failedTransitions.single;
      expect(transition.operationId, 'op-1');
      expect(transition.attemptCount, 5);
      // nextRetryAt = engine clock at transition + policy delay.
      expect(transition.nextRetryAt, t0.add(const Duration(seconds: 90)));

      expect(report.failed, 1);
      expect(report.deadLettered, 0);
      expect(report.cycleError, isNull);
      expect(clockCalls, greaterThanOrEqualTo(3));
    });

    test('non-retryable SyncFailure calls markDeadLetter', () async {
      engine = buildEngine();
      storage.operations.add(makeOperation('op-1', attemptCount: 2));
      remote.resultFor =
          (op) => networkFailure(op.operationId, isRetryable: false);
      await engine.start();

      final report = await engine.syncNow();

      expect(storage.deadLetterCalls, ['op-1']);
      expect(storage.failedCalls, isEmpty);
      expect(storage.deadLetterTransitions.single.attemptCount, 3);
      expect(storage.failedTransitions, isEmpty);
      expect(report.deadLettered, 1);
      expect(report.failed, 0);
      expect(report.cycleError, isNull);
      expect(storage.statusOf('op-1'), SyncOperationStatus.deadLetter);
    });

    test('max-attempt exhaustion calls markDeadLetter', () async {
      engine = buildEngine(
        policy: ExponentialBackoffRetryPolicy(maxAttempts: 3),
      );
      storage.operations.add(makeOperation('op-1', attemptCount: 2));
      remote.resultFor = (op) => networkFailure(op.operationId);
      await engine.start();

      final report = await engine.syncNow();

      // newAttemptCount = 3 >= maxAttempts = 3 -> dead letter.
      expect(storage.deadLetterCalls, ['op-1']);
      expect(storage.failedCalls, isEmpty);
      expect(storage.deadLetterTransitions.single.attemptCount, 3);
      expect(report.deadLettered, 1);
      expect(report.failed, 0);
    });

    test('retryable failure below maxAttempts calls markFailed', () async {
      engine = buildEngine(
        policy: ExponentialBackoffRetryPolicy(maxAttempts: 5),
      );
      storage.operations.add(makeOperation('op-1', attemptCount: 3));
      remote.resultFor = (op) => networkFailure(op.operationId);
      await engine.start();

      final report = await engine.syncNow();

      // newAttemptCount = 4 < 5 -> scheduled retry.
      expect(storage.failedCalls, ['op-1']);
      expect(storage.deadLetterCalls, isEmpty);
      expect(storage.failedTransitions.single.attemptCount, 4);
      expect(report.failed, 1);
      expect(report.deadLettered, 0);
    });

    test('SyncSuccess still calls markSucceeded', () async {
      storage.operations.add(makeOperation('op-1'));
      remote.resultFor =
          (op) => SyncSuccess(operationId: op.operationId, remoteVersion: 'v9');
      await engine.start();

      final report = await engine.syncNow();

      expect(retryPolicy.shouldDeadLetterCalls, isEmpty);
      expect(retryPolicy.computeDelayCalls, isEmpty);
      expect(storage.succeededCalls, ['op-1']);
      expect(storage.failedCalls, isEmpty);
      expect(storage.deadLetterCalls, isEmpty);
      expect(report.succeeded, 1);
    });

    test(
        'remote push exception becomes retryable SyncFailure handled '
        'by the policy', () async {
      engine = buildEngine();
      storage.operations.add(makeOperation('op-1'));
      remote.throwFor = (op) => StateError('socket closed');
      await engine.start();

      final report = await engine.syncNow();

      // Wrapped as retryable -> markFailed, no cycle error.
      expect(storage.failedCalls, ['op-1']);
      expect(retryPolicy.computeDelayCalls.single.error.isRetryable, isTrue);
      expect(report.failed, 1);
      expect(report.cycleError, isNull);
    });
  });

  group('fixed eligibility cutoff', () {
    test('a retry becoming due mid-cycle is not fetched again', () async {
      final t0 = DateTime.utc(2024, 3, 1);
      var clockCalls = 0;
      // call 1: cycle start/cutoff -> t0
      // call 2: markFailed nextRetryAt base -> t0 (nextRetryAt = t0+1m)
      // call 3+: completedAt -> t0+10m (simulates time passing)
      DateTime advancingClock() {
        clockCalls++;
        return clockCalls <= 2 ? t0 : t0.add(const Duration(minutes: 10));
      }

      engine = buildEngine(
        config: SyncEngineConfig(batchSize: 1, syncOnStart: false),
        clock: advancingClock,
      );
      storage.operations.add(makeOperation('op-1'));
      remote.resultFor = (op) => networkFailure(op.operationId);
      await engine.start();

      final report = await engine.syncNow();

      // Second fetch happened (batch was full) but used the SAME
      // cutoff t0 — op-1's nextRetryAt (t0+1m) is after t0, so it is
      // not eligible again even though "now" advanced past it.
      expect(storage.fetchCalls, 2);
      expect(storage.fetchNows, everyElement(t0));
      expect(remote.pushCalls, 1);
      expect(report.fetched, 1);
      expect(report.failed, 1);
    });
  });

  group('attemptedOperationIds', () {
    test('same operation is never pushed twice in one cycle', () async {
      engine = buildEngine(
        config: SyncEngineConfig(batchSize: 1, syncOnStart: false),
      );
      storage.operations.add(makeOperation('op-1'));
      // Adapter keeps returning op-1 even after it was removed —
      // attemptedOperationIds must prevent a second push.
      final op = storage.operations.single;
      storage.fetchScript.addAll([
        [op],
        [op],
        [op],
      ]);
      await engine.start();

      final report = await engine.syncNow();

      expect(remote.pushCalls, 1);
      expect(report.succeeded, 1);
      // fetched counts raw adapter results including repeats.
      expect(report.fetched, 2); // second scripted fetch was filtered
      expect(storage.fetchCalls, 2);
    });

    test(
        'duplicate operationIds within one fetch are claimed and '
        'pushed only once', () async {
      engine = buildEngine(
        config: SyncEngineConfig(
            batchSize: 2, maxConcurrency: 2, syncOnStart: false),
      );
      storage.operations.add(makeOperation('op-1'));
      final op = storage.operations.single;
      // One fetch response containing the same operation twice.
      storage.fetchScript.add([op, op]);
      await engine.start();

      final report = await engine.syncNow();

      expect(storage.inFlightCalls, ['op-1']);
      expect(remote.pushCalls, 1);
      expect(report.succeeded, 1);
      expect(report.fetched, 2); // raw adapter results, incl. duplicate
      expect(report.cycleError, isNull);
      expect(report.isSuccess, isTrue);
      expect(storage.operations, isEmpty);
    });

    test('no infinite loop when adapter returns only attempted IDs', () async {
      engine = buildEngine(
        config: SyncEngineConfig(batchSize: 1, syncOnStart: false),
      );
      storage.operations.add(makeOperation('op-1'));
      final op = storage.operations.single;
      // Every scripted fetch returns the already-attempted op.
      storage.fetchScript.addAll(List.filled(10, [op]));
      await engine.start();

      final report = await engine.syncNow();

      expect(remote.pushCalls, 1);
      expect(storage.fetchCalls, 2); // one filtered fetch ends the loop
      expect(report.isSuccess, isTrue);
    });
  });

  group('operation-level storage failures', () {
    test(
        'markFailed throw -> storageErrors, no failed counter, cycle '
        'continues', () async {
      engine = buildEngine(
        config: SyncEngineConfig(batchSize: 1, syncOnStart: false),
      );
      storage.operations.add(makeOperation('op-1'));
      storage.markFailedFailures.add('op-1');
      remote.resultFor = (op) => networkFailure(op.operationId);
      await engine.start();

      final report = await engine.syncNow();

      expect(report.cycleError, isNull);
      expect(report.storageErrors, 1);
      expect(report.failed, 0);
      expect(report.isSuccess, isFalse);
      // Transition threw before applying — op stays inFlight.
      expect(storage.statusOf('op-1'), SyncOperationStatus.inFlight);
      // Cycle continued to the next fetch.
      expect(storage.fetchCalls, 2);
    });

    test(
        'markDeadLetter throw -> storageErrors, no deadLettered '
        'counter', () async {
      storage.operations.add(makeOperation('op-1'));
      storage.markDeadLetterFailures.add('op-1');
      remote.resultFor =
          (op) => networkFailure(op.operationId, isRetryable: false);
      await engine.start();

      final report = await engine.syncNow();

      expect(report.cycleError, isNull);
      expect(report.storageErrors, 1);
      expect(report.deadLettered, 0);
      expect(report.failed, 0);
      expect(storage.statusOf('op-1'), SyncOperationStatus.inFlight);
    });

    test(
        'mismatch markDeadLetter throw -> storageErrors, no '
        'deadLettered', () async {
      storage.operations.add(makeOperation('op-1'));
      storage.markDeadLetterFailures.add('op-1');
      remote.resultFor = (op) => SyncSuccess(operationId: 'wrong-id');
      await engine.start();

      final report = await engine.syncNow();

      expect(report.cycleError, isNull);
      expect(report.storageErrors, 1);
      expect(report.deadLettered, 0);
      expect(report.failed, 0);
      expect(report.succeeded, 0);
    });

    test(
        'other operations in the chunk still settle after a storage '
        'failure', () async {
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
      expect(report.succeeded, 1);
      expect(report.storageErrors, 1);
      expect(report.cycleError, isNull);
    });

    test('fetch and claim failures still abort with cycleError', () async {
      engine = buildEngine(
        config: SyncEngineConfig(batchSize: 1, syncOnStart: false),
      );
      storage.operations.add(makeOperation('op-1'));
      storage.markInFlightFailures.add('op-1');
      await engine.start();

      final report = await engine.syncNow();

      expect(report.cycleError, isNotNull);
      expect(report.cycleError!.code, SyncErrorCode.storage);
      expect(report.storageErrors, 0);
      expect(remote.pushCalls, 0);
    });
  });

  group('policy exceptions', () {
    test('shouldDeadLetter throw aborts cycle after chunk settles', () async {
      final sentinel = StateError('policy exploded');
      engine = buildEngine(
        config: SyncEngineConfig(
            batchSize: 2, maxConcurrency: 2, syncOnStart: false),
      );
      retryPolicy.shouldDeadLetterError = sentinel;
      storage.operations.addAll([
        makeOperation('op-1'),
        makeOperation('op-2'),
      ]);
      remote.gates['op-1'] = Completer<SyncResult>();
      remote.gates['op-2'] = Completer<SyncResult>();
      remote.resultFor = (op) => op.operationId == 'op-1'
          ? networkFailure(op.operationId)
          : SyncSuccess(operationId: op.operationId);
      await engine.start();

      final reportFuture = engine.syncNow();
      await waitFor(() => remote.pushCalls == 2);

      remote.gates['op-1']!.complete(networkFailure('op-1'));
      remote.gates['op-2']!.complete(SyncSuccess(operationId: 'op-2'));

      final report = await reportFuture;

      // The policy exception reached the cycle error boundary; the
      // rest of the chunk settled first.
      expect(report.cycleError, isNotNull);
      expect(report.cycleError!.cause, same(sentinel));
      expect(report.succeeded, 1); // op-2 completed normally
      expect(report.failed, 0); // op-1 got no transition/counter
      expect(report.storageErrors, 0);
      expect(storage.failedCalls, isEmpty);
      expect(storage.deadLetterCalls, isEmpty);
      // Cycle aborted — no second fetch despite the full batch.
      expect(storage.fetchCalls, 1);
      // op-1 remains inFlight for startup recovery.
      expect(storage.statusOf('op-1'), SyncOperationStatus.inFlight);
    });

    test('computeDelay throw aborts cycle with no transition', () async {
      final sentinel = StateError('computeDelay exploded');
      engine = buildEngine();
      retryPolicy
        ..forceRetry = true
        ..computeDelayError = sentinel;
      storage.operations.add(makeOperation('op-1'));
      remote.resultFor = (op) => networkFailure(op.operationId);
      await engine.start();

      final report = await engine.syncNow();

      expect(report.cycleError, isNotNull);
      expect(report.cycleError!.cause, same(sentinel));
      expect(report.failed, 0);
      expect(report.deadLettered, 0);
      expect(report.storageErrors, 0);
      expect(storage.failedCalls, isEmpty);
      expect(storage.deadLetterCalls, isEmpty);
      expect(storage.statusOf('op-1'), SyncOperationStatus.inFlight);
    });
  });
}
