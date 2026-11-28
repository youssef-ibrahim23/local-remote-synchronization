import 'dart:async';

import 'package:local_remote_synchronization/local_remote_synchronization.dart';
import 'package:test/test.dart';

import 'fake_adapters.dart';

void main() {
  late FakeLocalStorageAdapter storage;
  late FakeRemoteSyncAdapter remote;
  late FakeConnectivityProvider connectivity;
  late SyncEngine engine;

  SyncEngine buildEngine({
    SyncEngineConfig? config,
    RetryPolicy? retryPolicy,
  }) {
    storage = FakeLocalStorageAdapter();
    remote = FakeRemoteSyncAdapter();
    connectivity = FakeConnectivityProvider();
    return SyncEngine(
      localStorage: storage,
      remoteSync: remote,
      connectivity: connectivity,
      config: config ?? SyncEngineConfig(syncOnStart: false),
      retryPolicy: retryPolicy,
    );
  }

  setUp(() {
    engine = buildEngine();
  });

  tearDown(() async {
    await engine.dispose();
    await connectivity.close();
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

  List<String> pushedIds() =>
      remote.pushed.map((op) => op.operationId).toList(growable: false);

  // ---------------------------------------------------------------------------
  // A. Per-entity ordering
  // ---------------------------------------------------------------------------
  group('A. per-entity ordering', () {
    test(
        'only the earliest operation per entity is selected per pass; '
        'later operations are pushed sequentially after success', () async {
      // batchSize == op count keeps the fetch loop alive: the cycle only
      // stops on a partial batch, so each subsequent fetch selects the
      // next operation for the entity.
      engine = buildEngine(
        config: SyncEngineConfig(batchSize: 3, syncOnStart: false),
      );
      // Same entity, deliberately seeded out of order.
      storage.operations.addAll([
        makeOperation('s3', entityId: 'e1', sequenceNumber: 3),
        makeOperation('s1', entityId: 'e1', sequenceNumber: 1),
        makeOperation('s2', entityId: 'e1', sequenceNumber: 2),
      ]);
      // The fetch loop only continues on a FULL raw batch, so script
      // three full fetches — each re-sorts and selects the next
      // earliest unattempted operation for the entity.
      for (var i = 0; i < 3; i++) {
        storage.fetchScript.add(List.of(storage.operations));
      }
      for (final id in ['s1', 's2', 's3']) {
        remote.gates[id] = Completer<SyncResult>();
      }

      await engine.start();
      final reportFuture = engine.syncNow();

      // First pass selects only the earliest operation.
      await waitFor(() => remote.pushCalls == 1);
      expect(remote.pushed.single.operationId, 's1');
      // s2/s3 were never even claimed while s1 is in flight.
      expect(storage.inFlightCalls, ['s1']);

      // Release s1 -> next fetch selects s2.
      remote.gates['s1']!.complete(SyncSuccess(operationId: 's1'));
      await waitFor(() => remote.pushCalls == 2);
      expect(remote.pushed[1].operationId, 's2');
      expect(storage.inFlightCalls, ['s1', 's2']);

      remote.gates['s2']!.complete(SyncSuccess(operationId: 's2'));
      await waitFor(() => remote.pushCalls == 3);
      expect(remote.pushed[2].operationId, 's3');

      remote.gates['s3']!.complete(SyncSuccess(operationId: 's3'));
      final report = await reportFuture;

      expect(pushedIds(), ['s1', 's2', 's3']);
      expect(report.succeeded, 3);
      expect(report.cycleError, isNull);
    });

    test(
        'successful earliest operation allows the next operation '
        'for the same entity in a subsequent fetch', () async {
      engine = buildEngine(
        config: SyncEngineConfig(batchSize: 2, syncOnStart: false),
      );
      storage.operations.addAll([
        makeOperation('first', entityId: 'e1', sequenceNumber: 1),
        makeOperation('second', entityId: 'e1', sequenceNumber: 2),
      ]);

      await engine.start();
      final report = await engine.syncNow();

      // Both pushed in order across separate fetch passes.
      expect(pushedIds(), ['first', 'second']);
      expect(report.succeeded, 2);
      expect(storage.fetchCalls, greaterThanOrEqualTo(2));
    });

    test(
        'retryable failure of earliest operation blocks later '
        'operations for the same entity across batches', () async {
      engine = buildEngine(
        config: SyncEngineConfig(batchSize: 2, syncOnStart: false),
      );
      storage.operations.addAll([
        makeOperation('f1', entityId: 'e1', sequenceNumber: 1),
        makeOperation('f2', entityId: 'e1', sequenceNumber: 2),
        makeOperation('ok1', entityId: 'e2'),
      ]);
      remote.resultFor = (op) => op.operationId == 'f1'
          ? retryableFailure('f1')
          : SyncSuccess(operationId: op.operationId);

      await engine.start();
      final report = await engine.syncNow();

      // f1 failed; ok1 (different entity) still succeeded.
      expect(report.failed, 1);
      expect(report.succeeded, 1);
      // f2 was fetched in a later batch but never pushed — entity e1
      // stayed blocked for the rest of the cycle.
      expect(pushedIds(), isNot(contains('f2')));
      expect(pushedIds(), containsAll(['f1', 'ok1']));
      expect(storage.fetchCalls, greaterThanOrEqualTo(2));
    });

    test(
        'dead-lettered earliest operation blocks later operations '
        'for the same entity', () async {
      engine = buildEngine(
        config: SyncEngineConfig(batchSize: 2, syncOnStart: false),
      );
      storage.operations.addAll([
        makeOperation('d1', entityId: 'e1', sequenceNumber: 1),
        makeOperation('d2', entityId: 'e1', sequenceNumber: 2),
      ]);
      remote.resultFor = (op) => op.operationId == 'd1'
          ? permanentFailure('d1')
          : SyncSuccess(operationId: op.operationId);

      await engine.start();
      final report = await engine.syncNow();

      expect(report.deadLettered, 1);
      expect(report.failed, 0);
      expect(pushedIds(), ['d1']);
      expect(storage.statusOf('d1'), SyncOperationStatus.deadLetter);
    });

    test(
        'conflict blocks later operations for the same entity and '
        'preserves persistence-before-event', () async {
      engine = buildEngine(
        config: SyncEngineConfig(batchSize: 2, syncOnStart: false),
      );
      storage.operations.addAll([
        makeOperation('c1', entityId: 'e1', sequenceNumber: 1),
        makeOperation('c2', entityId: 'e1', sequenceNumber: 2),
      ]);
      remote.resultFor = (op) => SyncConflict(
            operationId: op.operationId,
            remoteSnapshot: const {'x': 1},
          );

      final events = <SyncConflictEvent>[];
      engine.conflicts.listen(events.add);

      await engine.start();
      final report = await engine.syncNow();

      // Broadcast delivery is asynchronous — wait for the event.
      await waitFor(() => events.isNotEmpty);

      // Exactly one conflict event — for c1 only. c2 never pushed.
      expect(events, hasLength(1));
      expect(events.single.operation.operationId, 'c1');
      expect(pushedIds(), ['c1']);
      expect(report.conflicted, 1);
      expect(report.deadLettered, 1);
      expect(storage.statusOf('c1'), SyncOperationStatus.deadLetter);
    });

    test(
        'markSucceeded storage failure blocks the entity while '
        'other entities continue', () async {
      engine = buildEngine(
        config: SyncEngineConfig(batchSize: 2, syncOnStart: false),
      );
      storage.operations.addAll([
        makeOperation('x1', entityId: 'e1', sequenceNumber: 1),
        makeOperation('x2', entityId: 'e1', sequenceNumber: 2),
        makeOperation('ok1', entityId: 'e2'),
      ]);
      storage.markSucceededFailures.add('x1');

      await engine.start();
      final report = await engine.syncNow();

      expect(report.storageErrors, 1);
      expect(report.succeeded, 1);
      // x2 was fetched in a later batch but blocked — persisted state
      // of x1 is unknown, so the entity is conservatively blocked.
      expect(pushedIds(), isNot(contains('x2')));
      expect(pushedIds(), containsAll(['x1', 'ok1']));
    });

    test('markFailed storage failure blocks the entity', () async {
      engine = buildEngine(
        config: SyncEngineConfig(batchSize: 2, syncOnStart: false),
      );
      storage.operations.addAll([
        makeOperation('m1', entityId: 'e1', sequenceNumber: 1),
        makeOperation('m2', entityId: 'e1', sequenceNumber: 2),
      ]);
      storage.markFailedFailures.add('m1');
      remote.resultFor = (op) => op.operationId == 'm1'
          ? retryableFailure('m1')
          : SyncSuccess(operationId: op.operationId);

      await engine.start();
      final report = await engine.syncNow();

      expect(report.storageErrors, 1);
      expect(report.failed, 0);
      expect(pushedIds(), ['m1']);
    });

    test('markDeadLetter storage failure blocks the entity', () async {
      engine = buildEngine(
        config: SyncEngineConfig(batchSize: 2, syncOnStart: false),
      );
      storage.operations.addAll([
        makeOperation('k1', entityId: 'e1', sequenceNumber: 1),
        makeOperation('k2', entityId: 'e1', sequenceNumber: 2),
      ]);
      storage.markDeadLetterFailures.add('k1');
      remote.resultFor = (op) => op.operationId == 'k1'
          ? permanentFailure('k1')
          : SyncSuccess(operationId: op.operationId);

      await engine.start();
      final report = await engine.syncNow();

      expect(report.storageErrors, 1);
      expect(report.deadLettered, 0);
      expect(pushedIds(), ['k1']);
    });
  });

  // ---------------------------------------------------------------------------
  // B. Cross-entity parallelism
  // ---------------------------------------------------------------------------
  group('B. cross-entity parallelism', () {
    test('operations for different entities push concurrently', () async {
      engine = buildEngine(
        config: SyncEngineConfig(maxConcurrency: 2, syncOnStart: false),
      );
      storage.operations.addAll([
        makeOperation('p1', entityId: 'ent-a'),
        makeOperation('p2', entityId: 'ent-b'),
      ]);
      remote.gates['p1'] = Completer<SyncResult>();
      remote.gates['p2'] = Completer<SyncResult>();

      await engine.start();
      final reportFuture = engine.syncNow();

      // Both pushes are in flight simultaneously — neither gate has
      // been completed yet.
      await waitFor(() => remote.pushCalls == 2);
      expect(pushedIds(), containsAll(['p1', 'p2']));

      remote.gates['p1']!.complete(SyncSuccess(operationId: 'p1'));
      remote.gates['p2']!.complete(SyncSuccess(operationId: 'p2'));
      final report = await reportFuture;
      expect(report.succeeded, 2);
    });

    test('operations for the same entity never push concurrently', () async {
      // batchSize == op count so the fetch loop continues after the
      // first success and selects the next operation for the entity.
      engine = buildEngine(
        config: SyncEngineConfig(
            batchSize: 2, maxConcurrency: 5, syncOnStart: false),
      );
      storage.operations.addAll([
        makeOperation('a1', entityId: 'e1', sequenceNumber: 1),
        makeOperation('a2', entityId: 'e1', sequenceNumber: 2),
      ]);
      remote.gates['a1'] = Completer<SyncResult>();
      remote.gates['a2'] = Completer<SyncResult>();

      await engine.start();
      final reportFuture = engine.syncNow();

      await waitFor(() => remote.pushCalls == 1);
      // a2 was never claimed while a1 is in flight — proof it was not
      // even selected for the chunk.
      expect(storage.inFlightCalls, ['a1']);
      expect(remote.pushed.single.operationId, 'a1');

      remote.gates['a1']!.complete(SyncSuccess(operationId: 'a1'));
      await waitFor(() => remote.pushCalls == 2);
      expect(remote.pushed[1].operationId, 'a2');

      remote.gates['a2']!.complete(SyncSuccess(operationId: 'a2'));
      final report = await reportFuture;
      expect(report.succeeded, 2);
    });
  });

  // ---------------------------------------------------------------------------
  // C. Ordering comparator
  // ---------------------------------------------------------------------------
  group('C. ordering comparator', () {
    test('different sequenceNumbers select in ascending order', () async {
      engine = buildEngine(
        config: SyncEngineConfig(batchSize: 3, syncOnStart: false),
      );
      storage.operations.addAll([
        makeOperation('o9', entityId: 'e1', sequenceNumber: 9),
        makeOperation('o1', entityId: 'e1', sequenceNumber: 1),
        makeOperation('o5', entityId: 'e1', sequenceNumber: 5),
      ]);
      // Full-batch fetches keep the loop alive so each pass selects
      // the next earliest operation for the entity.
      for (var i = 0; i < 3; i++) {
        storage.fetchScript.add(List.of(storage.operations));
      }

      await engine.start();
      await engine.syncNow();

      expect(pushedIds(), ['o1', 'o5', 'o9']);
    });

    test('equal sequenceNumbers order by createdAt', () async {
      engine = buildEngine(
        config: SyncEngineConfig(batchSize: 3, syncOnStart: false),
      );
      storage.operations.addAll([
        makeOperation(
          'late',
          entityId: 'e1',
          sequenceNumber: 7,
          createdAt: DateTime.utc(2024, 1, 3),
        ),
        makeOperation(
          'early',
          entityId: 'e1',
          sequenceNumber: 7,
          createdAt: DateTime.utc(2024, 1, 1),
        ),
        makeOperation(
          'mid',
          entityId: 'e1',
          sequenceNumber: 7,
          createdAt: DateTime.utc(2024, 1, 2),
        ),
      ]);
      for (var i = 0; i < 3; i++) {
        storage.fetchScript.add(List.of(storage.operations));
      }

      await engine.start();
      await engine.syncNow();

      expect(pushedIds(), ['early', 'mid', 'late']);
    });

    test('equal sequenceNumbers and createdAt order by operationId', () async {
      engine = buildEngine(
        config: SyncEngineConfig(batchSize: 2, syncOnStart: false),
      );
      final sameTime = DateTime.utc(2024, 1, 1);
      storage.operations.addAll([
        makeOperation(
          'op-b',
          entityId: 'e1',
          sequenceNumber: 7,
          createdAt: sameTime,
        ),
        makeOperation(
          'op-a',
          entityId: 'e1',
          sequenceNumber: 7,
          createdAt: sameTime,
        ),
      ]);

      await engine.start();
      await engine.syncNow();

      expect(pushedIds(), ['op-a', 'op-b']);
    });

    test('null sequenceNumber is treated as 0 per documented semantics',
        () async {
      engine = buildEngine(
        config: SyncEngineConfig(batchSize: 3, syncOnStart: false),
      );
      storage.operations.addAll([
        // null sorts as 0: ties with seq 0, loses on createdAt.
        makeOperation(
          'null-seq',
          entityId: 'e1',
          sequenceNumber: null,
          createdAt: DateTime.utc(2024, 1, 2),
        ),
        makeOperation(
          'zero-seq',
          entityId: 'e1',
          sequenceNumber: 0,
          createdAt: DateTime.utc(2024, 1, 1),
        ),
        // seq 1 always sorts after the 0-group.
        makeOperation(
          'one-seq',
          entityId: 'e1',
          sequenceNumber: 1,
          createdAt: DateTime.utc(2024, 1, 3),
        ),
      ]);
      for (var i = 0; i < 3; i++) {
        storage.fetchScript.add(List.of(storage.operations));
      }

      await engine.start();
      await engine.syncNow();

      expect(pushedIds(), ['zero-seq', 'null-seq', 'one-seq']);
    });
  });

  // ---------------------------------------------------------------------------
  // D. Collision-safe entity keys
  // ---------------------------------------------------------------------------
  group('D. collision-safe entity keys', () {
    test(
        'entityType/entityId pairs that collide under delimiter '
        'concatenation are treated as distinct entities', () async {
      engine = buildEngine(
        config: SyncEngineConfig(
            batchSize: 3, maxConcurrency: 3, syncOnStart: false),
      );
      storage.operations.addAll([
        // Would both produce 'a:b:c' under a naive 'type:id' join.
        makeOperation('ab-c-1',
            entityType: 'a:b', entityId: 'c', sequenceNumber: 1),
        makeOperation('ab-c-2',
            entityType: 'a:b', entityId: 'c', sequenceNumber: 2),
        makeOperation('a-bc', entityType: 'a', entityId: 'b:c'),
      ]);

      await engine.start();
      final report = await engine.syncNow();

      // Both entities are independent: ab-c-1 and a-bc are selected in
      // the first pass; ab-c-2 follows after ab-c-1 succeeds.
      expect(report.succeeded, 3);
      expect(pushedIds().take(2), containsAll(['ab-c-1', 'a-bc']));
      expect(pushedIds().last, 'ab-c-2');
    });

    test(
        'special characters in identifiers do not cause false '
        'entity grouping', () async {
      storage.operations.addAll([
        makeOperation('w1', entityType: 't', entityId: 'x"]'),
        makeOperation('w2', entityType: 't', entityId: 'x'),
        makeOperation('w3', entityType: 't', entityId: r'x\'),
      ]);

      await engine.start();
      final report = await engine.syncNow();

      // All three are distinct entities — none blocked another.
      expect(report.succeeded, 3);
      expect(pushedIds(), containsAll(['w1', 'w2', 'w3']));
    });
  });

  // ---------------------------------------------------------------------------
  // E. Batch and concurrency limits
  // ---------------------------------------------------------------------------
  group('E. batch and concurrency limits', () {
    test('every fetch request uses config.batchSize as the limit', () async {
      engine = buildEngine(
        config: SyncEngineConfig(batchSize: 3, syncOnStart: false),
      );
      storage.operations.addAll([
        for (var i = 0; i < 5; i++) makeOperation('b$i', entityId: 'ent-$i'),
      ]);

      await engine.start();
      final report = await engine.syncNow();

      expect(report.succeeded, 5);
      expect(storage.fetchCalls, greaterThanOrEqualTo(2));
      expect(storage.fetchLimits, everyElement(3));
    });

    test('active pushes never exceed maxConcurrency', () async {
      engine = buildEngine(
        config: SyncEngineConfig(
            batchSize: 4, maxConcurrency: 2, syncOnStart: false),
      );
      storage.operations.addAll([
        for (var i = 0; i < 4; i++) makeOperation('c$i', entityId: 'ent-$i'),
      ]);
      for (var i = 0; i < 4; i++) {
        remote.gates['c$i'] = Completer<SyncResult>();
      }

      await engine.start();
      final reportFuture = engine.syncNow();

      // First chunk of 2 is claimed and pushed; the second chunk's ops
      // are not even claimed yet.
      await waitFor(() => remote.pushCalls == 2);
      expect(storage.inFlightCalls, hasLength(2));

      remote.gates['c0']!.complete(SyncSuccess(operationId: 'c0'));
      remote.gates['c1']!.complete(SyncSuccess(operationId: 'c1'));

      // Second chunk is claimed and pushed — still at most 2 at once.
      await waitFor(() => remote.pushCalls == 4);
      expect(storage.inFlightCalls, hasLength(4));

      remote.gates['c2']!.complete(SyncSuccess(operationId: 'c2'));
      remote.gates['c3']!.complete(SyncSuccess(operationId: 'c3'));
      final report = await reportFuture;
      expect(report.succeeded, 4);
    });

    test('entity blocking is preserved across multiple batch fetches',
        () async {
      engine = buildEngine(
        config: SyncEngineConfig(batchSize: 1, syncOnStart: false),
      );
      storage.operations.addAll([
        makeOperation('m-fail', entityId: 'e1', sequenceNumber: 1),
        makeOperation('m-ok', entityId: 'e2'),
        makeOperation('m-next', entityId: 'e1', sequenceNumber: 2),
      ]);
      remote.resultFor = (op) => op.operationId == 'm-fail'
          ? retryableFailure('m-fail')
          : SyncSuccess(operationId: op.operationId);

      await engine.start();
      final report = await engine.syncNow();

      expect(report.failed, 1);
      expect(report.succeeded, 1);
      expect(pushedIds(), containsAll(['m-fail', 'm-ok']));
      expect(pushedIds(), isNot(contains('m-next')));
      // One op per fetch: fail, then ok, then blocked re-fetches.
      expect(storage.fetchCalls, greaterThanOrEqualTo(3));
    });
  });

  // ---------------------------------------------------------------------------
  // F. Adapter FIFO limitations (documented, engine-level verification)
  // ---------------------------------------------------------------------------
  group('F. adapter FIFO limitations', () {
    // The Core contract does not guarantee at most one eligible operation
    // per entity per fetch, and the Sqflite/Drift Hive adapters have
    // documented null/equal sequenceNumber limitations (see
    // docs/architecture/phase-3-sync-engine.md §5). The engine-level
    // guard verified here is the defensive layer that makes the engine
    // correct regardless of adapter ordering defects.
    //
    // These tests verify the engine-side contract only; adapter
    // implementations are intentionally not modified in Phase 3.4.

    test(
        'engine remains correct when the adapter returns multiple '
        'operations per entity out of order', () async {
      engine = buildEngine(
        config: SyncEngineConfig(batchSize: 3, syncOnStart: false),
      );
      // Simulates an adapter that ignores per-entity ordering entirely:
      // returns every pending operation in insertion order.
      storage.operations.addAll([
        makeOperation('z-last', entityId: 'e1', sequenceNumber: 9),
        makeOperation('a-first', entityId: 'e1', sequenceNumber: 1),
        makeOperation('m-mid', entityId: 'e1', sequenceNumber: 5),
      ]);
      // Full-batch fetches keep the loop alive so each pass selects
      // the next earliest operation for the entity.
      for (var i = 0; i < 3; i++) {
        storage.fetchScript.add(List.of(storage.operations));
      }

      await engine.start();
      final report = await engine.syncNow();

      // Engine re-sorts defensively: pushes happen in sequence order,
      // never concurrently for the same entity.
      expect(pushedIds(), ['a-first', 'm-mid', 'z-last']);
      expect(report.succeeded, 3);
    });

    test(
        'null sequenceNumber ordering matches Hive ?? 0 fallback '
        'semantics', () async {
      engine = buildEngine(
        config: SyncEngineConfig(batchSize: 2, syncOnStart: false),
      );
      // Hive treats null sequenceNumber as 0; the engine tiebreak does
      // the same. Sqflite/Drift place SQL NULLs first in ASC order —
      // an adapter-level divergence the engine normalizes via
      // createdAt/operationId tiebreaks.
      storage.operations.addAll([
        makeOperation('n1', entityId: 'e1', sequenceNumber: null),
        makeOperation('n2', entityId: 'e1', sequenceNumber: null),
      ]);

      await engine.start();
      final report = await engine.syncNow();

      // Both null-seq ops sort as 0; createdAt ties -> operationId order.
      expect(pushedIds(), ['n1', 'n2']);
      expect(report.succeeded, 2);
    });
  });
}
