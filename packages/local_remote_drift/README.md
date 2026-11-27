# local_remote_drift

A durable outbox adapter for the **local_remote_synchronization** core package, built on **Drift** (formerly Moor).

## What it provides
- Implements the `LocalStorageAdapter` and `OutboxWriter` contracts defined by the core package.
- Persists outbox operations in the host application's existing Drift database, enabling seamless integration with any existing local schema.
- Supports:
  - Transaction‑scoped enqueuing (`outboxFor(transaction)`).
  - FIFO per‑entity ordering.
  - Stable operation IDs and idempotency keys.
  - Automatic pending‑operation count stream (`watchPendingCount`).
  - Refresh of pending count after external transactions (`refreshPendingCount`).
- No dependency on Flutter SDK (pure‑Dart) apart from the optional `sqlite3_flutter_libs` required by Drift on mobile.

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

// Assuming you already have a DriftDatabase `db`.
final adapter = DriftLocalStorageAdapter(database: db);

await adapter.runWithTransaction((txn) async {
  // Perform your domain writes using the same transaction.
  await db.customWrite(...);

  // Enqueue a sync operation atomically.
  final writer = txn.outbox;
  await writer.enqueue(SyncOperation(
    id: const Uuid().v4(),
    entityType: 'note',
    entityId: '123',
    payload: {'title': 'Hello'},
    // other required fields …
  ));
});

// Listen for pending‑operation count updates.
adapter.watchPendingCount().listen((count) {
  print('Pending outbox items: $count');
});
```

## API surface
- `DriftLocalStorageAdapter`
  - `runWithTransaction`, `runInTransaction`
  - `outboxFor(Transaction txn)` – returns a writer bound to an existing Drift transaction.
  - `refreshPendingCount()` – call after an external transaction commits to emit the correct count.
  - `watchPendingCount()` – broadcast stream of pending operation counts.
- `DriftOutboxWriter`
  - `enqueue(SyncOperation operation)` – inserts a new operation and returns the stored instance with a generated sequence number.

## Testing
The package ships with a full Dart test suite that runs against an in‑memory Drift database. Run the tests with:
```bash
melos test -p local_remote_drift
```

## Contributing
Feel free to open PRs or issues on the main repository. Follow the contribution guidelines in the root `CONTRIBUTING.md`.

---
*This adapter is part of the **offline‑first synchronization** ecosystem. See the core package README for the overall architecture.*