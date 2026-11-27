# local_remote_drift

A durable outbox adapter for the **local_remote_synchronization** core package, built on **Drift** (formerly Moor).

## What it provides
- Implements the `LocalStorageAdapter` and `OutboxWriter` contracts defined by the core package.
- Persists outbox operations in the host application's existing Drift database, allowing seamless integration with any existing local schema.
- Supports:
  - Transaction‑scoped enqueuing (`runWithTransaction`/`runInTransaction`).
  - FIFO per‑entity ordering.
  - Stable operation IDs and idempotency keys (generated automatically).
  - Reactive pending‑operation count stream (`watchPendingCount`).
  - Manual `refreshPendingCount()` for external transaction scenarios.

## Installation
```yaml
dependencies:
  local_remote_drift:
    path: packages/local_remote_drift # or from pub.dev when released
```

Make sure the core package is also a dependency (it is added automatically via the path reference).

## Usage example
```dart
import 'package:local_remote_drift/local_remote_drift.dart';
import 'package:drift/drift.dart';
import 'package:local_remote_synchronization/local_remote_synchronization.dart';

// 1️⃣ Create the application‑owned Drift database.
// The app owns its domain tables and the lifecycle of the database.
final db = LazyDatabase(() async {
  // For example, an in‑memory database for tests or a file‑based one in production.
  return NativeDatabase.memory();
});

// 2️⃣ Initialise the adapter (the outbox schema).
final adapter = DriftLocalStorageAdapter(database: db);
await adapter.ensureInitialized(); // safe to call repeatedly

// 3️⃣ Atomically write domain data and enqueue a mutation.
await adapter.runWithTransaction((outbox) async {
  // Domain write – any DAO or custom SQL on the same Drift database.
  await db.customInsert(
    'INSERT INTO tasks (id, title) VALUES (?, ?)',
    variables: [Variable.withString('t-1'), Variable.withString('First task')],
  );

  // Enqueue a local mutation. The adapter generates the operation ID.
  await outbox.enqueue(
    LocalMutation(
      entityType: 'tasks',
      entityId: 't-1',
      type: SyncOperationType.create,
      payload: const { 'title': 'First task' },
    ),
  );
});

// 4️⃣ Using an external (already‑open) transaction.
await db.transaction(() async {
  // Domain write inside the external transaction.
  await db.customInsert(
    'INSERT INTO tasks (id, title) VALUES (?, ?)',
    variables: [Variable.withString('t-2'), Variable.withString('Second task')],
  );

  // Enqueue via the adapter’s `outbox` writer (no new transaction is created).
  await adapter.outbox.enqueue(
    LocalMutation(
      entityType: 'tasks',
      entityId: 't-2',
      type: SyncOperationType.create,
      payload: const { 'title': 'Second task' },
    ),
  );
  // The pending‑count stream is not updated yet because the transaction
  // hasn't committed. Call `refreshPendingCount()` after the `transaction`
  // block finishes.
});
await adapter.refreshPendingCount(); // Emit the correct pending count.

// 5️⃣ Listening for pending‑operation count changes.
final subscription = adapter.watchPendingCount().listen((count) {
  print('Pending outbox items: $count');
});

// 6️⃣ Dispose the adapter when the app shuts down.
// This closes internal streams but does **not** close the application‑owned
// Drift database; the app should close the DB itself when appropriate.
await adapter.dispose();
await subscription.cancel();
```

## Schema initialization
- **New databases** – Call `await adapter.ensureInitialized();` before any outbox work, or integrate `DriftOutboxSchema.createSchema` into your `MigrationStrategy.onCreate`.
- **Existing databases** – `ensureInitialized()` is safe to call; it creates the outbox table only if it does not already exist, making it suitable for migrations.

## When to call `refreshPendingCount()`
`refreshPendingCount()` is required **only** when you use the `adapter.outbox` writer inside a transaction that you manage yourself (see step 4). The writer cannot emit a count before the surrounding transaction commits, so you manually trigger the refresh after the transaction finishes.

## Disposal
The adapter owns only the outbox‑related resources (stream controllers, etc.). It does **not** close the supplied Drift database – the application remains responsible for the database lifecycle.

---
*This adapter is part of the **offline‑first synchronization** ecosystem. See the core package README for the overall architecture.*
