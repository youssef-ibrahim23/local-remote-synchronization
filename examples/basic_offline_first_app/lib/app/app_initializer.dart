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

enum _InitState {
  idle,
  starting,
  running,
  disposing,
  disposed,
}

/// Owns the entire application lifecycle: database, adapters, engine, and
/// [TodoAppState].
///
/// Lifecycle contract:
/// - [start] may only be called once on an instance. Calling [start] while
///   already starting, running, disposing, or disposed throws a [StateError].
/// - Every created resource is tracked immediately. After every asynchronous
///   boundary, [start] checks whether [dispose] has been requested; if so,
///   startup aborts without creating further resources and [dispose] cleans up
///   all resources created so far.
/// - If [start] fails with an exception, all resources created so far are
///   cleaned up in reverse dependency order, the initializer transitions to
///   disposed, and the original exception is preserved and rethrown.
/// - [dispose] is idempotent and safe to call at any time (including while
///   [start] is in progress or after a failed startup). Concurrent calls to
///   [dispose] share the same completion future, and [dispose] never completes
///   while [start] can still create additional resources.
/// - [appState] is only accessible while running (after [start] completes
///   without cancellation and before [dispose] is requested).
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

  _InitState _state = _InitState.idle;
  Future<void>? _startupFuture;
  Future<void>? _disposeFuture;

  bool get _isDisposalRequested =>
      _state == _InitState.disposing || _state == _InitState.disposed;

  /// The application state container.
  ///
  /// Throws [StateError] if accessed before [start] completes or after
  /// [dispose] has been requested.
  TodoAppState get appState {
    final state = _appState;
    if (_state != _InitState.running || state == null) {
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
  /// running, disposing, or disposed.
  Future<void> start({
    String? restBaseUrl,
  }) {
    if (_isDisposalRequested) {
      throw StateError('AppInitializer is disposed.');
    }
    if (_state != _InitState.idle) {
      throw StateError('AppInitializer.start() may only be called once.');
    }
    _state = _InitState.starting;
    final startup = _performStart(restBaseUrl: restBaseUrl);
    _startupFuture = startup;
    return startup;
  }

  Future<void> _performStart({String? restBaseUrl}) async {
    try {
      final db = _openDb != null ? await _openDb() : await _defaultOpenDb();
      _db = db;
      if (_isDisposalRequested) return;

      final storageAdapter = _createStorageAdapter != null
          ? _createStorageAdapter(db)
          : SqfliteLocalStorageAdapter(database: db);
      _storageAdapter = storageAdapter;
      await storageAdapter.ensureInitialized();
      if (_isDisposalRequested) return;

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
      if (_isDisposalRequested) return;

      // Load initial todos from local storage.
      await state.loadTodos();
      if (_isDisposalRequested) return;

      _state = _InitState.running;
    } catch (_) {
      // If dispose() was not already called, clean up created resources here.
      // If dispose() IS in flight, it is awaiting _startupFuture and will run
      // _cleanupResources() as soon as this catch rethrows — avoiding deadlock.
      if (_disposeFuture == null) {
        _state = _InitState.disposing;
        await _cleanupResources();
        _state = _InitState.disposed;
        _disposeFuture = Future<void>.value();
      }
      rethrow;
    } finally {
      _startupFuture = null;
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
  /// If [start] is currently in progress, [dispose] prevents further startup
  /// work, waits for the active startup step to settle, and then cleans up
  /// every resource that was created.
  ///
  /// - `TodoAppState` subscriptions are cancelled.
  /// - [SyncEngine] waits for any active cycle before closing its streams.
  /// - [RestRemoteSyncAdapter] closes its owned HTTP client (injected clients
  ///   remain owned by the caller).
  /// - [SqfliteLocalStorageAdapter] releases its stream controller.
  /// - The [Database] is closed.
  ///
  /// Idempotent: concurrent or repeated calls share the same completion future.
  Future<void> dispose() {
    final existing = _disposeFuture;
    if (existing != null) {
      return existing;
    }
    _state = _InitState.disposing;
    final disposal = _performDispose();
    _disposeFuture = disposal;
    return disposal;
  }

  Future<void> _performDispose() async {
    final startup = _startupFuture;
    if (startup != null) {
      try {
        await startup;
      } catch (_) {}
    }
    await _cleanupResources();
    _state = _InitState.disposed;
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
