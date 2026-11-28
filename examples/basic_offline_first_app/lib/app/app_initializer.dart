import 'dart:async';

import 'package:flutter/widgets.dart';
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
/// Usage:
/// ```dart
/// final init = AppInitializer();
/// await init.start();
/// // Use init.appState in the widget tree.
/// // On app exit:
/// await init.dispose();
/// ```
///
/// Error handling: [start] propagates any exception from database opening or
/// [SyncEngine.start]. The calling widget should catch and surface these.
final class AppInitializer {
  late final Database _db;
  late final SqfliteLocalStorageAdapter _storageAdapter;
  late final TodoRepository _repository;
  late final SyncEngine _engine;
  late final ConnectivityPlusProvider _connectivity;
  late final TodoAppState appState;

  bool _started = false;
  bool _disposed = false;

  /// Opens the database, initialises all adapters, starts the [SyncEngine],
  /// and wires [appState].
  Future<void> start({
    /// Configurable base URL — defaults to [todoRestBaseUrl].
    String? restBaseUrl,
  }) async {
    assert(!_started, 'AppInitializer.start() called more than once.');

    final dbPath = await getDatabasesPath();
    final fullPath = '$dbPath/offline_first_todos.db';

    _db = await openDatabase(
      fullPath,
      version: 1,
      onCreate: (db, _) async {
        await TodoRepository.createSchema(db);
        await SqfliteOutboxSchema.createSchema(db);
      },
    );

    _storageAdapter = SqfliteLocalStorageAdapter(database: _db);
    await _storageAdapter.ensureInitialized();

    _repository = TodoRepository(
      database: _db,
      storageAdapter: _storageAdapter,
    );

    _connectivity = ConnectivityPlusProvider();

    final remoteAdapter = buildRestAdapter(
      baseUrl: restBaseUrl ?? todoRestBaseUrl,
    );

    _engine = SyncEngine(
      localStorage: _storageAdapter,
      remoteSync: remoteAdapter,
      connectivity: _connectivity,
    );

    appState = TodoAppState(repository: _repository);

    // Start engine — recovers stale inFlight ops and subscribes connectivity.
    await _engine.start();

    // Wire engine streams into appState after start() completes.
    appState.attachEngine(
      _engine,
      _storageAdapter.watchPendingCount(),
    );

    // Load initial todos from local storage.
    await appState.loadTodos();

    _started = true;
  }

  /// Disposes all resources in reverse dependency order.
  ///
  /// - [TodoAppState] subscriptions are cancelled.
  /// - [SyncEngine] waits for any active cycle before closing its streams.
  /// - [SqfliteLocalStorageAdapter] releases its stream controller.
  /// - The [Database] is closed.
  ///
  /// Idempotent: calling twice is safe.
  Future<void> dispose() async {
    if (_disposed || !_started) return;
    _disposed = true;

    appState.dispose();
    await _engine.dispose();
    await _storageAdapter.dispose();
    await _db.close();
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
