# local_remote_drift

A durable outbox adapter for the **local_remote_synchronization** core package, built on **Drift** (formerly Moor).

## What it provides

- Implements the `LocalStorageAdapter` and `OutboxWriter` contracts defined by the core package.
- Persists outbox operations in the host application's existing Drift database, allowing seamless integration with any existing local schema.
- Supports:
  - Transaction-scoped enqueuing via `runWithTransaction` / `runInTransaction`.
  - FIFO per-entity ordering.
  - Stable operation IDs and idempotency keys (auto-generated).
  - Reactive pending-operation count stream (`watchPendingCount`).
  - Manual `refreshPendingCount()` for external-transaction scenarios.

## Installation

```yaml
dependencies:
  local_remote_drift:
    path: packages/local_remote_drift  # or from pub.dev when released
```

The core package is added transitively via the path dependency.

## Usage

### 1. Define an application-owned Drift database

The application owns its domain tables and the lifecycle of the database.
The adapter only requires a `GeneratedDatabase` instance; it does not care
about domain-specific tables.

```dart
import 'dart:io';

import 'package:drift/drift.dart';
import 'package:drift/native.dart';
import 'package:local_remote_drift/local_remote_drift.dart';
import 'package:local_remote_synchronization/local_remote_synchronization.dart';

// Application database that owns domain tables and runs migrations.
class AppDatabase extends GeneratedDatabase {
  AppDatabase(super.e);

  @override
  int get schemaVersion => 2;

  @override
  Iterable<TableInfo<Table, Object?>> get allTables => const [];

  @override
  MigrationStrategy get migration => MigrationStrategy(
        onCreate: (m) async {
          // Create your domain tables here.
          await customStatement('''
            CREATE TABLE tasks (
              id   TEXT PRIMARY KEY,
              title TEXT NOT NULL
            )
          ''');
          // Create the outbox table inside the same migration so domain tables
          // and the outbox are always created together.
          await DriftOutboxSchema.createSchema(this);
        },
        onUpgrade: (m, from, to) async {
          // For databases that already exist, call createSchema in onUpgrade
          // when introducing the outbox for the first time:
          if (from < 2) {
            await DriftOutboxSchema.createSchema(this);
          }
        },
      );
}
```

### 2. Create the adapter

```dart
final db = AppDatabase(NativeDatabase(File('app.db')));
final adapter = DriftLocalStorageAdapter(database: db);
```

If you are **not** running migrations via `MigrationStrategy`, or you want to
ensure the outbox table exists before first use, call:

```dart
await adapter.ensureInitialized();
```

This is idempotent — it creates the table only if it does not already exist.

### 3. Write domain data and enqueue a mutation atomically

```dart
await adapter.runWithTransaction((outbox) async {
  // Any DAO or custom SQL on the same Drift database automatically
  // participates in the same underlying transaction.
  await db.customInsert(
    'INSERT INTO tasks (id, title) VALUES (?, ?)',
    variables: [Variable.withString('t-1'), Variable.withString('First task')],
  );

  // Enqueue a LocalMutation. The adapter generates the operation ID.
  await outbox.enqueue(
    LocalMutation(
      entityType: 'tasks',
      entityId: 't-1',
      type: SyncOperationType.create,
      payload: const {'title': 'First task'},
    ),
  );
});
```

Both the domain insert and the outbox insert are either committed together or
rolled back together.

### 4. External-transaction approach

When you need to perform multiple domain writes across several DAO calls in an
existing `db.transaction` block, use `adapter.outbox` to enqueue without
creating a nested transaction:

```dart
await db.transaction(() async {
  await db.customInsert(
    'INSERT INTO tasks (id, title) VALUES (?, ?)',
    variables: [Variable.withString('t-2'), Variable.withString('Second task')],
  );

  // adapter.outbox is zone-aware: the insert joins the ambient transaction.
  await adapter.outbox.enqueue(
    LocalMutation(
      entityType: 'tasks',
      entityId: 't-2',
      type: SyncOperationType.create,
      payload: const {'title': 'Second task'},
    ),
  );
  // The pending-count stream is NOT updated here — the transaction has not
  // committed yet. Call refreshPendingCount() after the block returns.
});
await adapter.refreshPendingCount();
```

### 5. Listening for pending-operation count changes

```dart
final subscription = adapter.watchPendingCount().listen((count) {
  print('Pending outbox items: $count');
});
```

### 6. Dispose the adapter

```dart
await adapter.dispose();
```

This closes internal stream controllers but does **not** close the
application-owned `GeneratedDatabase`. The app remains responsible for the
database lifecycle.

## Schema initialization

| Scenario | Recommended approach |
|---|---|
| New database | Call `DriftOutboxSchema.createSchema(this)` inside `MigrationStrategy.onCreate`. |
| Adding to an existing database | Call `DriftOutboxSchema.createSchema(this)` inside `MigrationStrategy.onUpgrade` when migrating from the version before the outbox was introduced. |
| Standalone / test | Call `await adapter.ensureInitialized()` before first use. It is idempotent. |

Do **not** rely on `ensureInitialized()` as the sole initialization path in
production code that uses `MigrationStrategy` — integrate the schema creation
into your migration so the outbox version is tracked alongside your schema
version.

## When to call `refreshPendingCount()`

Call `refreshPendingCount()` **only** when you use `adapter.outbox` inside a
transaction that you manage yourself (the external-transaction pattern in step
4). The writer cannot safely emit a count before the surrounding transaction
commits, so you trigger the refresh manually after the outer `db.transaction`
block returns.

`runWithTransaction` and `runInTransaction` call `refreshPendingCount()`
automatically after the transaction commits — no manual call needed.

## Nested-transaction behavior (Drift 2.31+)

**Note:** The nested transaction semantics described below require Drift 2.31 or later.

When `runWithTransaction` is called inside an outer `db.transaction`, Drift
creates a **child transaction** (backed by a SQLite SAVEPOINT). The behavior is:

- Writes inside the child transaction become visible to the outer transaction
  when the child commits, but are **not persisted to the database** until the
  outer transaction commits.
- If the **child** transaction throws, only its writes are rolled back; the
  outer transaction can continue.
- If the **outer** transaction throws after the child has committed, **all**
  writes — including those from the child — are rolled back.
- **`watchPendingCount` will emit a count after the child commits**, even
  though the data is not yet durably persisted. This is a known limitation of
  the current API: a premature count can be emitted if the outer transaction
  later aborts. The count self-corrects once the outer transaction rolls back
  and `refreshPendingCount()` is called.

For this reason, avoid calling `runWithTransaction` inside an outer
`db.transaction` when you require the pending-count stream to be strictly
accurate. Prefer `adapter.outbox` for the external-transaction pattern and call
`refreshPendingCount()` after the outer transaction commits.

## Testing

```bash
melos test -p local_remote_drift
# or directly:
dart test
```

## Contributing

Follow the contribution guidelines in the root `CONTRIBUTING.md`.

---

*This adapter is part of the **offline-first synchronization** ecosystem. See
the core package README for the overall architecture.*
