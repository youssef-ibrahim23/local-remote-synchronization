import 'dart:async';

import 'package:flutter/widgets.dart';
import 'package:http/http.dart' as http;
import 'package:local_remote_rest/local_remote_rest.dart';
import 'package:local_remote_sqflite/local_remote_sqflite.dart';
import 'package:local_remote_synchronization/local_remote_synchronization.dart';
import 'package:sqflite/sqflite.dart';

import '../data/todo_repository.dart';
import '../state/todo_app_state.dart';
import '../sync/connectivity_plus_provider.dart';
import '../sync/todo_rest_config.dart';

/// Owns the entire application lifecycle: database, adapters, engine, and
/// [TodoAppState].
///
/// Lifecycle contract:
/// - [start] may only be called once on an instance. Calling [start] while
///   already starting, started, or disposed throws a [StateError].
/// - If [start] fails at any step, all resources already created are cleaned up
///   in reverse dependency order, the instance transitions to disposed, and the
///   original exception is rethrown.
/// - [dispose] is idempotent and safe to call at any time (including after a
///   partial or failed startup).
/// - [appState] is only accessible after [start] has completed successfully
///   and before [dispose] is called.
final class AppInitializer {
  AppInitializer({
    Future<Database> Function()? openDb,
    SqfliteLocalStorageAdapter Function(Database db)? createStorageAdapter,
    ConnectivityProvider Function()? createConnectivityProvider,
    RestRemoteSyncAdapter Function(String baseUrl)? createRemoteAdapter,
    http.Client? httpClient,
    SyncEngineConfig? syncConfig,
    RetryPolicy? retryPolicy,
  })  : _openDb = openDb,
        _createStorageAdapter = createStorageAdapter,
        _createConnectivityProvider = createConnectivityProvider,
        _createRemoteAdapter = createRemoteAdapter,
        _httpClient = httpClient,
        _syncConfig = syncConfig,
        _retryPolicy = retryPolicy;

  final Future<Database> Function()? _openDb;
  final SqfliteLocalStorageAdapter Function(Database db)? _createStorageAdapter;
  final ConnectivityProvider Function()? _createConnectivityProvider;
  final RestRemoteSyncAdapter Function(String baseUrl)? _createRemoteAdapter;
  final http.Client? _httpClient;
  final SyncEngineConfig? _syncConfig;
  final RetryPolicy? _retryPolicy;

  Database? _db;
  SqfliteLocalStorageAdapter? _storageAdapter;
  RestRemoteSyncAdapter? _remoteAdapter;
  SyncEngine? _engine;
  TodoAppState? _appState;

  bool _starting = false;
  bool _started = false;
  bool _disposed = false;
  Future<void>? _disposeFuture;

  /// The application state container.
  ///
  /// Throws [StateError] if accessed before [start] completes or after
  /// [dispose] has been called.
  TodoAppState get appState {
    final state = _appState;
    if (_disposed || !_started || state == null) {
      throw StateError(
        'AppInitializer.appState is unavailable before start() completes or '
        'after dispose().',
      );
    }
    return state;
  }

  /// Opens the database, initialises all adapters, wires [TodoAppState] to
  /// engine streams, starts the [SyncEngine], and loads initial todos.
  ///
  /// May only be called once. Throws [StateError] if already starting,
  /// started, or disposed.
  Future<void> start({
    String? restBaseUrl,
  }) async {
    if (_disposed) {
      throw StateError('AppInitializer is disposed.');
    }
    if (_starting || _started) {
      throw StateError('AppInitializer.start() may only be called once.');
    }
    _starting = true;

    try {
      final db = _openDb != null ? await _openDb() : await _defaultOpenDb();
      _db = db;

      final storageAdapter = _createStorageAdapter != null
          ? _createStorageAdapter(db)
          : SqfliteLocalStorageAdapter(database: db);
      _storageAdapter = storageAdapter;
      await storageAdapter.ensureInitialized();

      final repository = TodoRepository(
        database: db,
        storageAdapter: storageAdapter,
      );

      final connectivity = _createConnectivityProvider != null
          ? _createConnectivityProvider()
          : ConnectivityPlusProvider();

      final baseUrl = restBaseUrl ?? todoRestBaseUrl;
      final remoteAdapter = _createRemoteAdapter != null
          ? _createRemoteAdapter(baseUrl)
          : buildRestAdapter(baseUrl: baseUrl, httpClient: _httpClient);
      _remoteAdapter = remoteAdapter;

      final engine = SyncEngine(
        localStorage: storageAdapter,
        remoteSync: remoteAdapter,
        connectivity: connectivity,
        config: _syncConfig,
        retryPolicy: _retryPolicy,
      );
      _engine = engine;

      final state = TodoAppState(repository: repository);
      _appState = state;

      // Subscribe to engine and outbox streams BEFORE engine.start() so that
      // any automatic sync triggered on startup cannot emit a report or
      // conflict event before listeners are attached.
      state.attachEngine(
        engine,
        storageAdapter.watchPendingCount(),
      );

      // Start engine — recovers stale inFlight ops, subscribes connectivity,
      // and may schedule an initial sync.
      await engine.start();

      // Load initial todos from local storage.
      await state.loadTodos();

      _started = true;
    } catch (_) {
      _disposed = true;
      await _cleanupResources();
      rethrow;
    } finally {
      _starting = false;
    }
  }

  static Future<Database> _defaultOpenDb() async {
    final dbPath = await getDatabasesPath();
    final fullPath = '$dbPath/offline_first_todos.db';
    return openDatabase(
      fullPath,
      version: 1,
      onCreate: (db, _) async {
        await TodoRepository.createSchema(db);
        await SqfliteOutboxSchema.createSchema(db);
      },
    );
  }

  /// Disposes all created resources in reverse dependency order.
  ///
  /// - `TodoAppState` subscriptions are cancelled.
  /// - [SyncEngine] waits for any active cycle before closing its streams.
  /// - [RestRemoteSyncAdapter] closes its owned HTTP client (injected clients
  ///   remain owned by the caller).
  /// - [SqfliteLocalStorageAdapter] releases its stream controller.
  /// - The [Database] is closed.
  ///
  /// Idempotent: concurrent or repeated calls share the same cleanup and
  /// complete safely, even after a partial startup failure.
  Future<void> dispose() {
    final existing = _disposeFuture;
    if (existing != null) {
      return existing;
    }
    _disposed = true;
    final cleanup = _cleanupResources();
    _disposeFuture = cleanup;
    return cleanup;
  }

  Future<void> _cleanupResources() async {
    final state = _appState;
    _appState = null;
    if (state != null) {
      try {
        state.dispose();
      } catch (_) {}
    }

    final engine = _engine;
    _engine = null;
    if (engine != null) {
      try {
        await engine.dispose();
      } catch (_) {}
    }

    final remoteAdapter = _remoteAdapter;
    _remoteAdapter = null;
    if (remoteAdapter != null) {
      try {
        remoteAdapter.close();
      } catch (_) {}
    }

    final storageAdapter = _storageAdapter;
    _storageAdapter = null;
    if (storageAdapter != null) {
      try {
        await storageAdapter.dispose();
      } catch (_) {}
    }

    final db = _db;
    _db = null;
    if (db != null) {
      try {
        await db.close();
      } catch (_) {}
    }
  }
}

/// Provides [AppInitializer] through the widget tree via [InheritedWidget].
class AppInitializerScope extends InheritedWidget {
  const AppInitializerScope({
    required this.initializer,
    required super.child,
    super.key,
  });

  final AppInitializer initializer;

  static AppInitializer of(BuildContext context) {
    final scope =
        context.dependOnInheritedWidgetOfExactType<AppInitializerScope>();
    assert(scope != null, 'No AppInitializerScope found in widget tree.');
    return scope!.initializer;
  }

  @override
  bool updateShouldNotify(AppInitializerScope oldWidget) =>
      oldWidget.initializer != initializer;
}
