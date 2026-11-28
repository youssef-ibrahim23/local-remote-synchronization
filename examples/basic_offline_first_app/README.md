# basic_offline_first_app

A reference Flutter application demonstrating how to build a production-quality
**Offline-First** app using the `local_remote_synchronization` ecosystem.

> **Status**: Phase 4.1 — fully implemented on branch `feature/phase-4-reference-app`.

---

## Purpose

This example shows how to wire the synchronisation ecosystem into a minimal but
realistic Flutter application.  It uses a **Todo** domain and demonstrates:

- Creating, updating, and deleting Todos while offline.
- Displaying data exclusively from local SQLite storage.
- Tracking pending outbox operations and sync status.
- Manually triggering synchronisation via a button.
- Showing sync progress and errors in the UI.
- Automatic sync on connectivity restoration.
- Retry behaviour using `ExponentialBackoffRetryPolicy`.
- Conflict event subscription (surfaced as a UI error message).
- Proper disposal of subscriptions, engine, and app-owned resources.

This is an **architectural reference application**, not a UI showcase.  The UI
is intentionally simple.

---

## Architecture

```
┌────────────────────────────────────────────────────────────────────┐
│                        Flutter Application                         │
│                                                                    │
│  main.dart                                                         │
│    └── AppInitializer                                              │
│          ├── opens SQLite database                                 │
│          ├── SqfliteLocalStorageAdapter  (local_remote_sqflite)    │
│          ├── TodoRepository              (app-owned)               │
│          ├── ConnectivityPlusProvider    (app-owned adapter)       │
│          ├── RestRemoteSyncAdapter       (local_remote_rest)       │
│          └── SyncEngine                 (local_remote_synchron…)   │
│                                                                    │
│  TodoAppState  (ChangeNotifier)                                    │
│    ├── reads from TodoRepository (local only)                      │
│    ├── subscribes to SyncEngine.reports                            │
│    ├── subscribes to SyncEngine.conflicts                          │
│    └── subscribes to SqfliteLocalStorageAdapter.watchPendingCount  │
│                                                                    │
│  TodoListScreen                                                    │
│    └── listens to TodoAppState → rebuilds on change               │
└────────────────────────────────────────────────────────────────────┘
```

**Dependency direction**: Application → Core + selected adapters.
No adapter-to-adapter dependencies.  Core remains Pure Dart with no Flutter
dependency.

---

## Project Layout

```
lib/
  main.dart                       Entry point
  app/
    app_initializer.dart          Lifecycle owner: DB, adapters, engine
  domain/
    todo.dart                     Todo model (app-owned)
  data/
    todo_repository.dart          CRUD + atomic outbox enqueue
  sync/
    connectivity_plus_provider.dart  ConnectivityProvider adapter
    todo_rest_config.dart            REST endpoint configuration
  state/
    todo_app_state.dart           ChangeNotifier state container
  ui/
    todo_list_screen.dart         Screen widget

test/
  integration/
    todo_integration_test.dart    Integration tests (sqflite_common_ffi)
```

---

## Local-First Persistence

The application owns its `todos` table and domain model.  
The Sqflite adapter owns only its durable `sync_outbox` table.

Every local mutation atomically:
1. Writes the domain row to `todos`.
2. Enqueues the corresponding `SyncOperation` into `sync_outbox`.

Both writes use the same native Sqflite `Transaction` via
`SqfliteLocalStorageAdapter.runWithTransaction`.  If either write fails, the
transaction rolls back and neither write is persisted.

The UI always reads from local storage.  It never makes remote calls directly.

---

## Remote REST Backend Configuration

The REST adapter is configured via `lib/sync/todo_rest_config.dart`.

| Operation | HTTP Method | Path               |
|-----------|-------------|--------------------|
| create    | `POST`      | `{baseUrl}/todos`  |
| update    | `PATCH`     | `{baseUrl}/todos/{id}` |
| delete    | `DELETE`    | `{baseUrl}/todos/{id}` |

### Configuring the base URL

Override at build time:

```sh
flutter run --dart-define=TODO_API_URL=https://api.example.com
```

The app defaults to `http://localhost:8080` and **fully works offline** without
a real backend (local mutations persist; sync is retried when connectivity
returns).

### Authentication

Supply tokens via `RestRemoteSyncAdapter.defaultHeaders` in
`buildRestAdapter`.  Never store real credentials in this repository.

### Idempotency

Each request includes an `Idempotency-Key` header containing the stable
`SyncOperation.idempotencyKey`.  The adapter delivers at-least-once — the
backend **must** persist and honour this key to deduplicate retries.  The
adapter alone does not guarantee exactly-once delivery.

> **Important**: this adapter never generates a new key and never claims
> exactly-once delivery.  Exactly-once requires backend enforcement.

### Conflict detection

If your backend enforces optimistic concurrency (ETag / `If-Match`), include
`baseVersion` in `SyncOperation.metadata`.  `409`/`412` responses are reported
as `SyncConflict` and dead-lettered durably.  The Core contract provides no
automatic conflict-resolution workflow.

---

## Running the App

```sh
cd examples/basic_offline_first_app
flutter pub get
flutter run
```

For a custom backend:

```sh
flutter run --dart-define=TODO_API_URL=https://your-backend.example.com
```

---

## Running the Tests

The integration tests use `sqflite_common_ffi` (real SQLite in process).  
No device or real backend is required.

```sh
cd examples/basic_offline_first_app
flutter test
```

---

## Sync Engine Lifecycle

```
AppInitializer.start()
  ├── opens SQLite database
  ├── SqfliteLocalStorageAdapter.ensureInitialized()
  ├── builds RestRemoteSyncAdapter (retains reference for disposal)
  ├── creates SyncEngine(localStorage, remoteSync, connectivity, ...)
  ├── TodoAppState.attachEngine(engine, watchPendingCount())
  │     ├── subscribes SyncEngine.reports   → updates UI sync summary
  │     ├── subscribes SyncEngine.conflicts → surfaces conflict messages
  │     └── subscribes watchPendingCount    → shows outbox badge
  └── SyncEngine.start()         ← recovers stale inFlight ops
                                   subscribes connectivity
                                   triggers initial sync if online

AppInitializer.dispose()          ← idempotent; also runs on partial start failure
  ├── TodoAppState.dispose()      ← cancels stream subscriptions
  ├── SyncEngine.dispose()        ← waits for active cycle; closes streams
  ├── RestRemoteSyncAdapter.close() ← closes owned HTTP client (if not injected)
  ├── SqfliteLocalStorageAdapter.dispose()
  └── Database.close()
```

> **Lifecycle note:** In `main.dart`, `AppInitializer.dispose()` is wired to the
> root widget's `State.dispose()`. Flutter widget disposal is **not** a
> guaranteed process-shutdown callback (the OS can terminate the process without
> running `dispose()`). Crash recovery relies on the durable SQLite outbox and
> `SyncEngine.start()` recovering any stale `inFlight` operations on next launch.

---

## Known Limitations

- **Pull synchronisation** is not implemented.  This is a push-only outbox.
- **Conflict resolution** is not implemented.  Conflicts are dead-lettered and
  require manual intervention.
- **Background execution** is platform-controlled.  The app does not register
  WorkManager or BGTaskScheduler jobs.  Sync runs while the app is in the
  foreground or the process is alive.
- **Exactly-once delivery** requires backend idempotency enforcement.  The
  adapter alone is at-least-once.
- **Hive consistency**: Hive does not support ACID transactions.  Domain writes
  and outbox enqueues cannot be made fully atomic.  Use Sqflite or Drift for
  production reliability.
