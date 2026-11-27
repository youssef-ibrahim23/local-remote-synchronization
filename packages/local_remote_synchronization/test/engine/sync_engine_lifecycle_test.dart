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

  /// Config with all auto-triggers disabled for deterministic manual
  /// cycle tests.
  SyncEngineConfig manualConfig({bool syncOnConnectivityRestore = false}) =>
      SyncEngineConfig(
        syncOnStart: false,
        syncOnConnectivityRestore: syncOnConnectivityRestore,
      );

  SyncEngine buildEngine({
    SyncEngineConfig? config,
    DateTime Function()? clock,
    bool online = true,
  }) {
    events = <String>[];
    storage = FakeLocalStorageAdapter(events: events);
    remote = FakeRemoteSyncAdapter(events: events);
    connectivity = FakeConnectivityProvider(
      isConnected: online,
      events: events,
    );
    return SyncEngine(
      localStorage: storage,
      remoteSync: remote,
      connectivity: connectivity,
      config: config,
      clock: clock,
    );
  }

  setUp(() {
    engine = buildEngine(config: manualConfig());
  });

  tearDown(() async {
    await engine.dispose();
    await connectivity.close();
  });

  group('start', () {
    test('starts successfully and runs recovery once', () async {
      storage.operations.add(makeOperation('op-1'));
      await engine.start();

      expect(engine.isRunning, isTrue);
      expect(storage.recoverCalls, 1);
      expect(storage.recoveredCount, 0);
    });

    test('is idempotent once started', () async {
      await engine.start();
      await engine.start();
      await engine.start();

      expect(storage.recoverCalls, 1);
    });

    test('concurrent start calls share one startup', () async {
      storage.recoverGate = Completer<void>();

      final first = engine.start();
      final second = engine.start();
      await waitFor(() => storage.recoverCalls == 1);

      storage.recoverGate!.complete();
      await Future.wait([first, second]);

      expect(storage.recoverCalls, 1);
      expect(engine.isRunning, isTrue);
    });

    test('retries after recovery failure', () async {
      storage.recoverError = StateError('recovery failed');
      await expectLater(engine.start(), throwsStateError);
      expect(engine.isRunning, isFalse);

      storage.recoverError = null;
      await engine.start();
      expect(engine.isRunning, isTrue);
      expect(storage.recoverCalls, 2);
    });

    test('throws when called after dispose', () async {
      await engine.start();
      await engine.dispose();
      expect(() => engine.start(), throwsStateError);
    });

    test('dispose during startup aborts cleanly', () async {
      storage.recoverGate = Completer<void>();

      final startFuture = engine.start();
      await waitFor(() => storage.recoverCalls == 1);

      final disposeFuture = engine.dispose();
      storage.recoverGate!.complete();

      await disposeFuture;
      await startFuture;

      expect(engine.isRunning, isFalse);
      expect(storage.recoverCalls, 1);
    });

    test('dispose during connectivity sampling aborts cleanly', () async {
      connectivity.isConnectedGate = Completer<bool>();

      final startFuture = engine.start();
      await waitFor(() => events.contains('isConnectedSample'));

      final disposeFuture = engine.dispose();
      connectivity.isConnectedGate!.complete(true);

      await disposeFuture;
      await startFuture;

      expect(engine.isRunning, isFalse);
    });

    test('subscribes to connectivity before sampling it', () async {
      await engine.start();

      final subscribeIndex = events.indexOf('connectivitySubscribe');
      final sampleIndex = events.indexOf('isConnectedSample');
      expect(subscribeIndex, isNonNegative);
      expect(sampleIndex, isNonNegative);
      expect(subscribeIndex, lessThan(sampleIndex));
    });
  });

  group('syncNow guards', () {
    test('throws before start', () {
      expect(() => engine.syncNow(), throwsStateError);
    });

    test('throws after dispose', () async {
      await engine.start();
      await engine.dispose();
      expect(() => engine.syncNow(), throwsStateError);
    });
  });

  group('dispose', () {
    test('is idempotent and shares one disposal', () async {
      await engine.start();
      final first = engine.dispose();
      final second = engine.dispose();

      expect(identical(first, second), isTrue);
      await Future.wait([first, second]);
      expect(engine.isRunning, isFalse);
    });

    test('dispose before start is safe', () async {
      await engine.dispose();
      expect(engine.isRunning, isFalse);
      expect(() => engine.start(), throwsStateError);
    });

    test('waits for an active cycle to complete', () async {
      storage.operations.add(makeOperation('op-1'));
      remote.gates['op-1'] = Completer<SyncResult>();

      await engine.start();
      final reportFuture = engine.syncNow();
      await waitFor(() => remote.pushCalls == 1);

      var disposeCompleted = false;
      final disposeFuture = engine.dispose().then((_) {
        disposeCompleted = true;
      });

      // Give the event loop a chance — dispose must still be waiting.
      await Future<void>.delayed(Duration.zero);
      await Future<void>.delayed(Duration.zero);
      expect(disposeCompleted, isFalse);

      remote.gates['op-1']!.complete(SyncSuccess(operationId: 'op-1'));
      final report = await reportFuture;
      await disposeFuture;

      expect(disposeCompleted, isTrue);
      expect(report.succeeded, 1);
      expect(engine.isRunning, isFalse);
    });

    test('sequential offline syncNow calls produce distinct reports', () async {
      // Rebuild offline with manual triggers disabled.
      await engine.dispose();
      await connectivity.close();
      engine = buildEngine(config: manualConfig(), online: false);
      await engine.start();

      final first = await engine.syncNow();
      final second = await engine.syncNow();

      expect(identical(first, second), isFalse);
      expect(first.skippedOffline, isTrue);
      expect(second.skippedOffline, isTrue);
      expect(first.isSuccess, isTrue);
      expect(first.succeeded, 0);
      expect(storage.fetchCalls, 0);
    });
  });

  group('single-flight reservation', () {
    test('concurrent syncNow calls share one cycle', () async {
      storage.operations.add(makeOperation('op-1'));
      remote.gates['op-1'] = Completer<SyncResult>();

      await engine.start();
      final first = engine.syncNow();
      final second = engine.syncNow();

      remote.gates['op-1']!.complete(SyncSuccess(operationId: 'op-1'));
      final results = await Future.wait([first, second]);

      expect(identical(results[0], results[1]), isTrue);
      expect(storage.fetchCalls, 1);
      expect(remote.pushCalls, 1);
    });

    test('cycle completing before completer is awaited still delivers',
        () async {
      storage.operations.add(makeOperation('op-1'));
      await engine.start();

      final reportFuture = engine.syncNow();
      // Let the cycle run to completion without awaiting yet.
      await waitFor(() => storage.succeededCalls.contains('op-1'));

      final report = await reportFuture;
      expect(report.succeeded, 1);
    });
  });

  group('connectivity', () {
    test('offline->online transition triggers a sync cycle', () async {
      engine = buildEngine(
        config: manualConfig(syncOnConnectivityRestore: true),
        online: false,
      );
      await engine.start();
      expect(storage.fetchCalls, 0);

      connectivity.emit(true);
      await waitFor(() => storage.fetchCalls > 0);

      // The triggered cycle fetched the (empty) outbox once.
      expect(remote.pushCalls, 0);
    });

    test('repeated online events do not retrigger', () async {
      engine = buildEngine(
        config: manualConfig(syncOnConnectivityRestore: true),
        online: false,
      );
      await engine.start();

      connectivity.emit(true);
      await waitFor(() => storage.fetchCalls == 1);
      connectivity.emit(true);
      connectivity.emit(true);

      // Pump the event loop; no additional fetch should occur.
      for (var i = 0; i < 20; i++) {
        await Future<void>.delayed(Duration.zero);
      }
      expect(storage.fetchCalls, 1);
    });

    test('online->offline->online triggers again', () async {
      engine = buildEngine(
        config: manualConfig(syncOnConnectivityRestore: true),
        online: false,
      );
      await engine.start();

      connectivity.emit(true);
      await waitFor(() => storage.fetchCalls == 1);
      connectivity.emit(false);
      connectivity.emit(true);
      await waitFor(() => storage.fetchCalls == 2);
    });

    test('events during startup are buffered and reconciled', () async {
      // Sample says online, but the latest buffered event is offline —
      // the engine must treat the device as offline.
      connectivity.isConnectedGate = Completer<bool>();

      final startFuture = engine.start();
      await waitFor(() => events.contains('isConnectedSample'));

      connectivity.emit(false);
      connectivity.emit(true);
      connectivity.emit(false);
      connectivity.isConnectedGate!.complete(true);
      await startFuture;

      // Latest buffered event (false) wins over the online sample.
      final report = await engine.syncNow();
      expect(report.skippedOffline, isTrue);
    });

    test('latest buffered online event enables auto-sync on start', () async {
      // Use default config (syncOnStart: true), sample offline but
      // buffered events end online.
      await engine.dispose();
      await connectivity.close();
      events = <String>[];
      storage = FakeLocalStorageAdapter(events: events);
      remote = FakeRemoteSyncAdapter(events: events);
      connectivity = FakeConnectivityProvider(
        isConnected: false,
        events: events,
      );
      connectivity.isConnectedGate = Completer<bool>();
      engine = SyncEngine(
        localStorage: storage,
        remoteSync: remote,
        connectivity: connectivity,
      );

      final startFuture = engine.start();
      await waitFor(() => events.contains('isConnectedSample'));

      connectivity.emit(false);
      connectivity.emit(true);
      connectivity.isConnectedGate!.complete(false);
      await startFuture;

      // Buffered latest (true) overrides the offline sample → auto-sync.
      await waitFor(() => storage.fetchCalls > 0);
    });
  });

  group('auto-sync on start', () {
    test('syncOnStart triggers a cycle when online', () async {
      await engine.dispose();
      await connectivity.close();
      engine = buildEngine(); // default config: syncOnStart = true
      await engine.start();

      await waitFor(() => storage.fetchCalls > 0);
      expect(engine.isRunning, isTrue);
    });

    test('syncOnStart does not trigger when offline', () async {
      await engine.dispose();
      await connectivity.close();
      engine = buildEngine(online: false); // default config
      await engine.start();

      for (var i = 0; i < 20; i++) {
        await Future<void>.delayed(Duration.zero);
      }
      expect(storage.fetchCalls, 0);
    });
  });

  group('SyncEngineConfig', () {
    test('applies defaults', () {
      final config = SyncEngineConfig();
      expect(config.batchSize, 50);
      expect(config.maxConcurrency, 5);
      expect(config.syncOnStart, isTrue);
      expect(config.syncOnConnectivityRestore, isTrue);
      expect(config.syncWhenOffline, isFalse);
    });

    test('rejects non-positive batchSize', () {
      expect(() => SyncEngineConfig(batchSize: 0), throwsArgumentError);
      expect(() => SyncEngineConfig(batchSize: -1), throwsArgumentError);
    });

    test('rejects non-positive maxConcurrency', () {
      expect(() => SyncEngineConfig(maxConcurrency: 0), throwsArgumentError);
      expect(() => SyncEngineConfig(maxConcurrency: -2), throwsArgumentError);
    });
  });
}
