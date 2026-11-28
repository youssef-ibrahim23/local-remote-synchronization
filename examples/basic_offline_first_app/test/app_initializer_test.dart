import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:basic_offline_first_app/app/app_initializer.dart';
import 'package:basic_offline_first_app/data/todo_repository.dart';
import 'package:basic_offline_first_app/domain/todo.dart';
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
      await expectLater(initializer.start(), throwsStateError);
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
      await expectLater(initializer.start(), throwsStateError);
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
