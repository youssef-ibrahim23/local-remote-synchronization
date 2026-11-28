import 'dart:async';

import 'package:flutter/foundation.dart';
import 'package:local_remote_synchronization/local_remote_synchronization.dart';

import '../data/todo_repository.dart';
import '../domain/todo.dart';

/// Lightweight state container for the Todo list screen.
///
/// Uses [ChangeNotifier] so the UI can call [addListener] without
/// introducing a heavy state-management library.
///
/// Responsibilities:
/// - Holds the in-memory todo list (read from local SQLite).
/// - Tracks sync status, outbox count, and the last sync error.
/// - Exposes [createTodo], [toggleTodo], [deleteTodo] for mutations that
///   atomically persist the domain change and enqueue an outbox operation.
/// - Exposes [syncNow] to manually trigger a sync cycle.
///
/// The [SyncEngine] is wired externally and injected here via
/// [attachEngine]. This keeps engine lifecycle (start/dispose) in
/// `AppInitializer` and not in the state object.
final class TodoAppState extends ChangeNotifier {
  TodoAppState({required TodoRepository repository}) : _repo = repository;

  final TodoRepository _repo;

  // ── State fields ───────────────────────────────────────────────────────────

  List<Todo> _todos = const [];
  int _pendingCount = 0;
  bool _isSyncing = false;
  String? _lastSyncError;
  DateTime? _lastSyncAt;
  int _lastSyncSucceeded = 0;
  int _lastSyncFailed = 0;
  int _lastSyncDeadLettered = 0;
  bool _lastSyncSkippedOffline = false;

  SyncEngine? _engine;
  StreamSubscription<int>? _pendingCountSub;
  StreamSubscription<SyncCycleReport>? _reportSub;
  StreamSubscription<SyncConflictEvent>? _conflictSub;

  // ── Getters ────────────────────────────────────────────────────────────────

  List<Todo> get todos => _todos;
  int get pendingCount => _pendingCount;
  bool get isSyncing => _isSyncing;
  String? get lastSyncError => _lastSyncError;
  DateTime? get lastSyncAt => _lastSyncAt;
  int get lastSyncSucceeded => _lastSyncSucceeded;
  int get lastSyncFailed => _lastSyncFailed;
  int get lastSyncDeadLettered => _lastSyncDeadLettered;
  bool get lastSyncSkippedOffline => _lastSyncSkippedOffline;
  bool get engineRunning => _engine?.isRunning ?? false;

  // ── Engine wiring ──────────────────────────────────────────────────────────

  /// Attaches a started [SyncEngine] and subscribes to its streams.
  ///
  /// The engine must already have had [SyncEngine.start] called.
  /// Call this once after `AppInitializer` starts the engine.
  void attachEngine(SyncEngine engine, Stream<int> pendingCountStream) {
    _engine = engine;

    _pendingCountSub = pendingCountStream.listen((count) {
      _pendingCount = count;
      notifyListeners();
    });

    _reportSub = engine.reports.listen(_onReport);
    _conflictSub = engine.conflicts.listen(_onConflict);

    notifyListeners();
  }

  void _onReport(SyncCycleReport report) {
    _isSyncing = false;
    _lastSyncAt = report.completedAt;
    _lastSyncSucceeded = report.succeeded;
    _lastSyncFailed = report.failed;
    _lastSyncDeadLettered = report.deadLettered;
    _lastSyncSkippedOffline = report.skippedOffline;

    final err = report.cycleError;
    if (err != null) {
      _lastSyncError = err.message;
    } else if (report.skippedOffline) {
      _lastSyncError = 'Skipped — device is offline.';
    } else {
      _lastSyncError = null;
    }

    notifyListeners();
  }

  void _onConflict(SyncConflictEvent event) {
    // Conflict events are ephemeral notifications. The operation has been
    // dead-lettered durably. Surface a user-visible message.
    _lastSyncError = 'Conflict detected for '
        '${event.operation.entityType}:${event.operation.entityId}. '
        'The operation was dead-lettered. Manual intervention may be required.';
    notifyListeners();
  }

  // ── Data loading ───────────────────────────────────────────────────────────

  /// Loads the current todo list from local storage.
  ///
  /// Call once at startup and after any mutation that the UI needs to
  /// reflect immediately.
  Future<void> loadTodos() async {
    _todos = await _repo.getAll();
    notifyListeners();
  }

  // ── Mutations ──────────────────────────────────────────────────────────────

  /// Creates a new todo with [title].
  ///
  /// Persists the domain row and enqueues the outbox operation atomically.
  /// Never swallows exceptions: the UI should catch and display them.
  Future<void> createTodo(String title, String id) async {
    final todo = Todo(id: id, title: title.trim());
    await _repo.create(todo);
    _todos = [..._todos, todo];
    notifyListeners();
  }

  /// Toggles the `completed` flag of [todo].
  Future<void> toggleTodo(Todo todo) async {
    final updated = todo.copyWith(completed: !todo.completed);
    await _repo.update(updated);
    _todos = _todos.map((t) => t.id == todo.id ? updated : t).toList();
    notifyListeners();
  }

  /// Deletes `todo` from local storage. The local row is removed
  /// immediately; the remote delete will be pushed by the sync engine.
  ///
  /// **Important**: deletion from local storage and remote synchronisation
  /// are separate concerns. A remote failure does not undo the local delete.
  Future<void> deleteTodo(Todo todo) async {
    await _repo.delete(todo.id);
    _todos = _todos.where((t) => t.id != todo.id).toList();
    notifyListeners();
  }

  // ── Sync ───────────────────────────────────────────────────────────────────

  /// Manually triggers a sync cycle.
  ///
  /// Throws [StateError] if the engine is not yet running.
  Future<void> syncNow() async {
    final engine = _engine;
    if (engine == null || !engine.isRunning) {
      throw StateError('SyncEngine is not running.');
    }
    _isSyncing = true;
    _lastSyncError = null;
    notifyListeners();
    try {
      await engine.syncNow();
    } finally {
      if (_isSyncing) {
        _isSyncing = false;
        notifyListeners();
      }
    }
  }

  // ── Disposal ───────────────────────────────────────────────────────────────

  @override
  void dispose() {
    _pendingCountSub?.cancel();
    _reportSub?.cancel();
    _conflictSub?.cancel();
    super.dispose();
  }
}
