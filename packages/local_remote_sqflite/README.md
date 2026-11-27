# local_remote_sqflite

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](../../LICENSE)

**Sqflite** implementation of `LocalStorageAdapter` for the
[local_remote_synchronization](../local_remote_synchronization) ecosystem.

> **Architectural Principle:** Each local storage adapter persists its own
> Outbox using the same database technology selected by the application.
> Sqflite is **not** required by the Core or by applications using other
> adapters (such as Drift or Hive).

---

## Installation

```yaml
dependencies:
  local_remote_synchronization: ^0.1.0
  local_remote_sqflite: ^0.1.0
  sqflite: ^2.4.2
```

---

## Initialization

The application owns its SQLite `Database` instance and domain tables. The
adapter owns only its outbox table (defaulting to `sync_outbox`).

You can create the outbox schema in `onCreate` / `onUpgrade`, or call
`ensureInitialized()` (idempotent):

```dart
final db = await openDatabase(
  'app.db',
  version: 1,
  onCreate: (db, version) async {
    await db.execute(
      'CREATE TABLE tasks (id TEXT PRIMARY KEY, title TEXT NOT NULL)',
    );
    await SqfliteOutboxSchema.createSchema(db);
  },
);

final localStorage = SqfliteLocalStorageAdapter(database: db);
await localStorage.ensureInitialized();
```

---

## Usage & Typed Transaction API

To guarantee atomicity between your application's domain writes and outbox
enqueues, use `runWithTransaction`, which provides both the native Sqflite
`Transaction` and an `OutboxWriter` bound to that exact same transaction:

```dart
await localStorage.runWithTransaction((txn, outbox) async {
  await txn.insert('tasks', {'id': task.id, 'title': task.title});

  await outbox.enqueue(
    LocalMutation(
      entityType: 'tasks',
      entityId: task.id,
      type: SyncOperationType.create,
      payload: task.toJson(),
    ),
  );
});
```

If you already opened a `db.transaction((txn) ...)` block, use
`localStorage.outboxFor(txn)` to obtain an `OutboxWriter` bound to `txn`, and
call `await localStorage.refreshPendingCount()` after the transaction commits
to notify `watchPendingCount()` listeners.

> **Note:** `runWithTransaction` and `outboxFor` are specific to the Sqflite
> adapter. Core remains database-agnostic and does not expose Sqflite types.

---

## Outbox Schema & Indexes

Table `sync_outbox` (configurable via `tableName`, validated as a safe SQLite
identifier):

| Column | Type | Notes |
|---|---|---|
| `sequence_number` | `INTEGER PRIMARY KEY AUTOINCREMENT` | Monotonic insertion order |
| `operation_id` | `TEXT NOT NULL UNIQUE` | Stable UUID v4 |
| `idempotency_key` | `TEXT NOT NULL` | Immutable across retries |
| `entity_type` | `TEXT NOT NULL` | Logical entity/table name |
| `entity_id` | `TEXT NOT NULL` | Entity identifier |
| `operation_type` | `TEXT NOT NULL` | `create`, `update`, `delete` |
| `payload_json` | `TEXT` | Nullable JSON snapshot/patch |
| `metadata_json` | `TEXT NOT NULL` | JSON metadata |
| `status` | `TEXT NOT NULL` | `pending`, `inFlight`, `failed`, `deadLetter` |
| `attempt_count` | `INTEGER NOT NULL DEFAULT 0` | Push attempts |
| `created_at_ms` | `INTEGER NOT NULL` | UTC epoch milliseconds |
| `occurred_at_ms` | `INTEGER` | Optional UTC epoch milliseconds |
| `updated_at_ms` | `INTEGER NOT NULL` | Last transition UTC epoch ms |
| `next_retry_at_ms` | `INTEGER` | Nullable UTC epoch milliseconds |
| `last_error_json` | `TEXT` | Nullable serialized `SyncError` |

Indexes:
- `idx_sync_outbox_status_retry` on `(status, next_retry_at_ms, created_at_ms, sequence_number)`
- `idx_sync_outbox_entity_seq` on `(entity_type, entity_id, sequence_number)`

---

## Per-Entity FIFO Behavior

When `fetchEligibleOperations(..., preservePerEntityOrder: true)` is called:
- Operations are ordered deterministically by `(created_at_ms ASC, sequence_number ASC)`.
- An operation is excluded if **any** earlier operation for the same
  `(entity_type, entity_id)` has a status other than `succeeded` (`pending`,
  `inFlight`, `failed`, or `deadLetter`).
- Unrelated entities remain independent and are never head-of-line blocked.
- A `deadLetter` operation continues blocking newer operations for the same
  entity until an explicit resolution/discard mechanism is introduced.

---

## Crash Recovery & Transaction Guarantees

- **Atomicity**: `runWithTransaction` and `runInTransaction` execute all work
  inside a single Sqflite `db.transaction(...)`. If either the domain write or
  `outbox.enqueue` throws, SQLite rolls back both.
- **Crash recovery**: Calling `recoverInFlightOperations()` on startup resets
  persisted `inFlight` rows back to `pending` so interrupted operations are
  retried safely.
- **Idempotency**: `operation_id` and `idempotency_key` are persisted and never
  regenerated across retries or restarts.

---

## Known Limitations

- **No exactly-once remote delivery**: Durable local storage and stable
  idempotency keys provide at-least-once local durability; exactly-once remote
  effects require idempotency support on the remote backend.
- **Dead-letter blocking**: When `preservePerEntityOrder` is `true`, a
  `deadLetter` operation blocks subsequent operations for the same entity until
  explicit dead-letter resolution APIs are added in a future phase.
- **No background isolate synchronization**: This package provides local
  persistence only; foreground/background scheduling is outside its scope.

---

## License

MIT — see [LICENSE](../../LICENSE).
