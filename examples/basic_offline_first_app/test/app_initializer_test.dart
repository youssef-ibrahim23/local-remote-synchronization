import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:basic_offline_first_app/app/app_initializer.dart';
import 'package:basic_offline_first_app/data/todo_repository.dart';
import 'package:basic_offline_first_app/domain/todo.dart';
import 'package:basic_offline_first_app/sync/todo_rest_config.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:http/http.dart' as http;
import 'package:local_remote_sqflite/local_remote_sqflite.dart';
import 'package:local_remote_synchronization/local_remote_synchronization.dart';
import 'package:sqflite_common_ffi/sqflite_ffi.dart';

void main() {
  sqfliteFfiInit();

  late Directory tempDir;
  late String dbPath;

  setUp(() async {
    tempDir = await Directory.systemTemp.createTemp('app_init_test_');
    dbPath = '${tempDir.path}/app_init.db';
  });

  tearDown(() async {
    if (tempDir.existsSync()) {
      await tempDir.delete(recursive: true);
    }
  });

  Future<Database> openTestDb() => databaseFactoryFfi.openDatabase(
        dbPath,
        options: OpenDatabaseOptions(
          version: 1,
          onCreate: (db, _) async {
            await TodoRepository.createSchema(db);
            await SqfliteOutboxSchema.createSchema(db);
          },
        ),
      );

  group('AppInitializer REST client ownership & disposal', () {
    test('closes adapter-owned HTTP client exactly once on dispose()',
        () async {
      final trackingClient = _TrackingHttpClient();

      await http.runWithClient(() async {
        final initializer = AppInitializer(
          openDb: openTestDb,
          createConnectivityProvider: _AlwaysOffline.new,
          syncConfig: SyncEngineConfig(
            syncOnStart: false,
            syncOnConnectivityRestore: false,
          ),
        );

        await initializer.start(restBaseUrl: 'http://localhost:8080');
        expect(trackingClient.closeCount, 0);

        await initializer.dispose();
        expect(trackingClient.closeCount, 1);

        // Repeated and concurrent dispose calls do not close the client again.
        await Future.wait([initializer.dispose(), initializer.dispose()]);
        expect(trackingClient.closeCount, 1);
      }, () => trackingClient);
    });

    test('does not close an injected HTTP client on dispose()', () async {
      final injectedClient = _TrackingHttpClient();
      final initializer = AppInitializer(
        openDb: openTestDb,
        createConnectivityProvider: _AlwaysOffline.new,
        httpClient: injectedClient,
        syncConfig: SyncEngineConfig(
          syncOnStart: false,
          syncOnConnectivityRestore: false,
        ),
      );

      await initializer.start(restBaseUrl: 'http://localhost:8080');
      await initializer.dispose();

      expect(injectedClient.closeCount, 0);
      injectedClient.close();
    });
  });

  group('AppInitializer partial startup failure cleanup', () {
    test('cleans up cleanly when database opening fails', () async {
      final initializer = AppInitializer(
        openDb: () async => throw StateError('db open failed'),
        createConnectivityProvider: _AlwaysOffline.new,
      );

      await expectLater(initializer.start(), throwsStateError);
      expect(() => initializer.appState, throwsStateError);
      await expectLater(initializer.dispose(), completes);
      expect(initializer.start, throwsStateError);
    });

    test('closes DB when adapter ensureInitialized() fails', () async {
      late Database openedDb;
      final initializer = AppInitializer(
        openDb: () async {
          openedDb = await openTestDb();
          // Close the DB immediately so ensureInitialized() fails.
          await openedDb.close();
          return openedDb;
        },
        createConnectivityProvider: _AlwaysOffline.new,
      );

      await expectLater(initializer.start(), throwsA(anything));
      expect(openedDb.isOpen, isFalse);
      expect(() => initializer.appState, throwsStateError);
      await expectLater(initializer.dispose(), completes);
    });

    test('closes DB and storage adapter when remote adapter creation fails',
        () async {
      late Database openedDb;
      var storageStreamClosed = false;

      final initializer = AppInitializer(
        openDb: () async => openedDb = await openTestDb(),
        createStorageAdapter: (db) {
          final adapter = SqfliteLocalStorageAdapter(database: db);
          adapter.watchPendingCount().listen(
                (_) {},
                onDone: () => storageStreamClosed = true,
              );
          return adapter;
        },
        createConnectivityProvider: _AlwaysOffline.new,
        createRemoteAdapter: (_) => throw StateError('remote config failed'),
      );

      await expectLater(initializer.start(), throwsStateError);

      expect(storageStreamClosed, isTrue);
      expect(openedDb.isOpen, isFalse);
      expect(() => initializer.appState, throwsStateError);
      await expectLater(initializer.dispose(), completes);
    });

    test(
        'cleans up all resources including owned REST client when engine '
        'startup fails, and continues cleanup even if a step throws', () async {
      late Database openedDb;
      var storageStreamClosed = false;
      final trackingClient = _TrackingHttpClient();

      await http.runWithClient(() async {
        final initializer = AppInitializer(
          openDb: () async => openedDb = await openTestDb(),
          createStorageAdapter: (db) {
            final adapter = SqfliteLocalStorageAdapter(database: db);
            adapter.watchPendingCount().listen(
                  (_) {},
                  onDone: () => storageStreamClosed = true,
                );
            return adapter;
          },
          createConnectivityProvider: _ThrowingConnectivity.new,
        );

        await expectLater(
          initializer.start(restBaseUrl: 'http://localhost:8080'),
          throwsA(isA<FormatException>()),
        );

        expect(trackingClient.closeCount, 1);
        expect(storageStreamClosed, isTrue);
        expect(openedDb.isOpen, isFalse);
        expect(() => initializer.appState, throwsStateError);
      }, () => trackingClient);
    });

    test('repeated start() calls throw StateError', () async {
      final initializer = AppInitializer(
        openDb: openTestDb,
        createConnectivityProvider: _AlwaysOffline.new,
        syncConfig: SyncEngineConfig(
          syncOnStart: false,
          syncOnConnectivityRestore: false,
        ),
      );
      addTearDown(initializer.dispose);

      await initializer.start(restBaseUrl: 'http://localhost:8080');
      expect(initializer.start, throwsStateError);
    });
  });

  group('AppInitializer stream subscription timing', () {
    test('initial automatic sync report on start() is not missed by appState',
        () async {
      // Seed a pending todo before AppInitializer.start().
      final seedDb = await openTestDb();
      final seedStorage = SqfliteLocalStorageAdapter(database: seedDb);
      await seedStorage.ensureInitialized();
      final seedRepo = TodoRepository(
        database: seedDb,
        storageAdapter: seedStorage,
      );
      await seedRepo.create(const Todo(id: 'seed-1', title: 'Pending todo'));
      await seedStorage.dispose();
      await seedDb.close();

      final reportReceived = Completer<void>();
      final fakeClient = _FakeSuccessHttpClient();

      final initializer = AppInitializer(
        openDb: openTestDb,
        createConnectivityProvider: _AlwaysOnline.new,
        httpClient: fakeClient,
        syncConfig: SyncEngineConfig(
          syncOnConnectivityRestore: false,
        ),
      );
      addTearDown(() async {
        await initializer.dispose();
        fakeClient.close();
      });

      await initializer.start(restBaseUrl: 'http://localhost:8080');

      final state = initializer.appState;
      void checkReport() {
        if (state.lastSyncAt != null && !reportReceived.isCompleted) {
          reportReceived.complete();
        }
      }

      state.addListener(checkReport);
      checkReport();

      await reportReceived.future.timeout(const Duration(seconds: 5));
      state.removeListener(checkReport);

      expect(state.lastSyncSucceeded, 1);
      expect(state.lastSyncFailed, 0);
      expect(state.lastSyncError, isNull);
    });
  });

  group('AppInitializer startup/disposal race coordination', () {
    test('Test A — dispose while database opening prevents further init',
        () async {
      final dbOpened = Completer<void>();
      final dbGate = Completer<Database>();
      var storageCreated = false;
      var connectivityCreated = false;
      var remoteCreated = false;

      final initializer = AppInitializer(
        openDb: () {
          dbOpened.complete();
          return dbGate.future;
        },
        createStorageAdapter: (db) {
          storageCreated = true;
          return SqfliteLocalStorageAdapter(database: db);
        },
        createConnectivityProvider: () {
          connectivityCreated = true;
          return _AlwaysOffline();
        },
        createRemoteAdapter: (baseUrl) {
          remoteCreated = true;
          return buildRestAdapter(baseUrl: baseUrl);
        },
      );

      final startFuture =
          initializer.start(restBaseUrl: 'http://localhost:8080');
      await dbOpened.future;

      var disposeCompleted = false;
      final disposeFuture = initializer.dispose().then((_) {
        disposeCompleted = true;
      });

      // dispose() must wait while start() is still opening the DB.
      await Future<void>.delayed(Duration.zero);
      expect(disposeCompleted, isFalse);

      final realDb = await openTestDb();
      dbGate.complete(realDb);

      await Future.wait([startFuture, disposeFuture]);

      expect(disposeCompleted, isTrue);
      expect(realDb.isOpen, isFalse);
      expect(storageCreated, isFalse);
      expect(connectivityCreated, isFalse);
      expect(remoteCreated, isFalse);
      expect(() => initializer.appState, throwsStateError);
      await expectLater(initializer.dispose(), completes);
    });

    test(
        'Test B — dispose during storage initialization cleans up & skips '
        'engine', () async {
      final executeEntered = Completer<void>();
      final executeGate = Completer<void>();
      var storageStreamClosed = false;
      var connectivityCreated = false;
      var remoteCreated = false;
      late Database realDb;

      final initializer = AppInitializer(
        openDb: () async {
          realDb = await openTestDb();
          return _GatedExecuteDatabase(
            realDb,
            onFirstExecute: () {
              if (!executeEntered.isCompleted) executeEntered.complete();
              return executeGate.future;
            },
          );
        },
        createStorageAdapter: (db) {
          final adapter = SqfliteLocalStorageAdapter(database: db);
          adapter.watchPendingCount().listen(
                (_) {},
                onDone: () => storageStreamClosed = true,
              );
          return adapter;
        },
        createConnectivityProvider: () {
          connectivityCreated = true;
          return _AlwaysOffline();
        },
        createRemoteAdapter: (baseUrl) {
          remoteCreated = true;
          return buildRestAdapter(baseUrl: baseUrl);
        },
      );

      final startFuture =
          initializer.start(restBaseUrl: 'http://localhost:8080');
      await executeEntered.future;

      var disposeCompleted = false;
      final disposeFuture = initializer.dispose().then((_) {
        disposeCompleted = true;
      });

      await Future<void>.delayed(Duration.zero);
      expect(disposeCompleted, isFalse);

      executeGate.complete();
      await Future.wait([startFuture, disposeFuture]);

      expect(disposeCompleted, isTrue);
      expect(storageStreamClosed, isTrue);
      expect(realDb.isOpen, isFalse);
      expect(connectivityCreated, isFalse);
      expect(remoteCreated, isFalse);
      expect(() => initializer.appState, throwsStateError);
    });

    test('Test C — dispose during engine startup cleans up all resources',
        () async {
      final connStarted = Completer<void>();
      final connGate = Completer<bool>();
      final trackingClient = _TrackingHttpClient();
      var storageStreamClosed = false;
      late Database realDb;

      await http.runWithClient(() async {
        final initializer = AppInitializer(
          openDb: () async => realDb = await openTestDb(),
          createStorageAdapter: (db) {
            final adapter = SqfliteLocalStorageAdapter(database: db);
            adapter.watchPendingCount().listen(
                  (_) {},
                  onDone: () => storageStreamClosed = true,
                );
            return adapter;
          },
          createConnectivityProvider: () => _GatedConnectivity(
            onIsConnected: () {
              if (!connStarted.isCompleted) connStarted.complete();
              return connGate.future;
            },
          ),
        );

        final startFuture =
            initializer.start(restBaseUrl: 'http://localhost:8080');
        await connStarted.future;

        var disposeCompleted = false;
        final disposeFuture = initializer.dispose().then((_) {
          disposeCompleted = true;
        });

        await Future<void>.delayed(Duration.zero);
        expect(disposeCompleted, isFalse);

        connGate.complete(false);
        await Future.wait([startFuture, disposeFuture]);

        expect(disposeCompleted, isTrue);
        expect(trackingClient.closeCount, 1);
        expect(storageStreamClosed, isTrue);
        expect(realDb.isOpen, isFalse);
        expect(() => initializer.appState, throwsStateError);
      }, () => trackingClient);
    });

    test(
        'Test D — concurrent dispose calls during startup close resources once',
        () async {
      final connStarted = Completer<void>();
      final connGate = Completer<bool>();
      final trackingClient = _TrackingHttpClient();
      var storageStreamClosed = false;
      late Database realDb;

      await http.runWithClient(() async {
        final initializer = AppInitializer(
          openDb: () async => realDb = await openTestDb(),
          createStorageAdapter: (db) {
            final adapter = SqfliteLocalStorageAdapter(database: db);
            adapter.watchPendingCount().listen(
                  (_) {},
                  onDone: () => storageStreamClosed = true,
                );
            return adapter;
          },
          createConnectivityProvider: () => _GatedConnectivity(
            onIsConnected: () {
              if (!connStarted.isCompleted) connStarted.complete();
              return connGate.future;
            },
          ),
        );

        final startFuture =
            initializer.start(restBaseUrl: 'http://localhost:8080');
        await connStarted.future;

        final d1 = initializer.dispose();
        final d2 = initializer.dispose();
        final d3 = initializer.dispose();
        expect(identical(d1, d2), isTrue);
        expect(identical(d2, d3), isTrue);

        connGate.complete(false);
        await Future.wait([startFuture, d1, d2, d3]);

        expect(trackingClient.closeCount, 1);
        expect(storageStreamClosed, isTrue);
        expect(realDb.isOpen, isFalse);
        expect(() => initializer.appState, throwsStateError);
      }, () => trackingClient);
    });
  });
}

final class _TrackingHttpClient extends http.BaseClient {
  int closeCount = 0;

  @override
  Future<http.StreamedResponse> send(http.BaseRequest request) async =>
      http.StreamedResponse(const Stream.empty(), HttpStatus.ok);

  @override
  void close() {
    closeCount++;
    super.close();
  }
}

final class _FakeSuccessHttpClient extends http.BaseClient {
  @override
  Future<http.StreamedResponse> send(http.BaseRequest request) async =>
      http.StreamedResponse(
        Stream.value(utf8.encode('{}')),
        HttpStatus.created,
      );
}

final class _AlwaysOffline implements ConnectivityProvider {
  @override
  Future<bool> get isConnected => Future.value(false);

  @override
  Stream<bool> get onConnectivityChanged => const Stream.empty();
}

final class _AlwaysOnline implements ConnectivityProvider {
  @override
  Future<bool> get isConnected => Future.value(true);

  @override
  Stream<bool> get onConnectivityChanged => const Stream.empty();
}

final class _ThrowingConnectivity implements ConnectivityProvider {
  @override
  Future<bool> get isConnected =>
      Future.error(const FormatException('connectivity probe failed'));

  @override
  Stream<bool> get onConnectivityChanged => const Stream.empty();
}

final class _GatedConnectivity implements ConnectivityProvider {
  _GatedConnectivity({required Future<bool> Function() onIsConnected})
      : _onIsConnected = onIsConnected;

  final Future<bool> Function() _onIsConnected;

  @override
  Future<bool> get isConnected => _onIsConnected();

  @override
  Stream<bool> get onConnectivityChanged => const Stream.empty();
}

/// Delegating [Database] that gates the first [execute] call on a Future so
/// tests can deterministically pause inside `ensureInitialized()`.
final class _GatedExecuteDatabase implements Database {
  _GatedExecuteDatabase(
    this._inner, {
    required Future<void> Function() onFirstExecute,
  }) : _onFirstExecute = onFirstExecute;

  final Database _inner;
  final Future<void> Function() _onFirstExecute;
  bool _gated = false;

  @override
  Future<void> execute(String sql, [List<Object?>? arguments]) async {
    if (!_gated) {
      _gated = true;
      await _onFirstExecute();
    }
    return _inner.execute(sql, arguments);
  }

  @override
  Future<List<Map<String, Object?>>> rawQuery(
    String sql, [
    List<Object?>? arguments,
  ]) =>
      _inner.rawQuery(sql, arguments);

  @override
  bool get isOpen => _inner.isOpen;

  @override
  Future<void> close() => _inner.close();

  @override
  dynamic noSuchMethod(Invocation invocation) => super.noSuchMethod(invocation);
}
