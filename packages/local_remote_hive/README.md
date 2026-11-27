# local_remote_hive

[![pub.dev](https://img.shields.io/pub/v/local_remote_hive.svg)](https://pub.dev/packages/local_remote_hive)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](../../LICENSE)

A durable outbox adapter for the **local_remote_synchronization** core package, built on **Hive**.

## What it provides

- Implements the `LocalStorageAdapter` and `OutboxWriter` contracts defined by the core package.
- Persists outbox operations in application-owned Hive boxes, allowing seamless integration with any existing local schema.
- Supports:
  - Transaction-scoped enqueuing via `runWithTransaction` / `runInTransaction`.
  - FIFO per-entity ordering.
  - Stable operation IDs and idempotency keys (auto-generated).
  - Reactive pending-operation count stream (`watchPendingCount`).
  - Manual `refreshPendingCount()` for external-write scenarios.
  - Crash recovery of in-flight operations (`recoverInFlightOperations`).

## Installation

```yaml
dependencies:
  local_remote_hive:
    path: packages/local_remote_hive  # or from pub.dev when released
```

The core package is added transitively via the path dependency.

## Usage

### 1. Initialize Hive and open boxes

The application owns its Hive boxes and lifecycle. Open the outbox box and
the metadata box before creating the adapter.

```dart
import 'dart:io';

import 'package:hive/hive.dart';
import 'package:local_remote_hive/local_remote_hive.dart';
import 'package:local_remote_synchronization/local_remote_synchronization.dart';

// Initialize Hive with a storage directory.
await Hive.initFlutter(); // or Hive.init(path) for pure Dart

// Open the outbox box and metadata box.
final outboxBox = await Hive.openBox<String>('sync_outbox');
final metadataBox = await Hive.openBox<int>('sync_outbox_meta');
```

### 2. Create the adapter

```dart
final adapter = HiveLocalStorageAdapter(
  box: outboxBox,
  metadataBox: metadataBox,
);
```

The adapter does not open or close boxes — the application owns their
lifecycle. Call `ensureInitialized()` for API consistency; it is a no-op
because Hive boxes are already open.

### 3. Write domain data and enqueue a mutation atomically

```dart
await adapter.runWithTransaction((outbox) async {
  // Domain writes on other Hive boxes are NOT covered by this transaction.
  // Hive does not provide multi-key transactions.
  // For atomic domain + outbox writes, use a single Hive box or
  // application-level coordination.

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

Outbox writes through the provided `OutboxWriter` are buffered and flushed
atomically on success. If the action throws, all buffered writes are
discarded.

### 4. External-write approach

When you need to write directly through `adapter.outbox` outside of a
transaction, call `refreshPendingCount()` after the write to notify
`watchPendingCount` listeners:

```dart
await adapter.outbox.enqueue(
  LocalMutation(
    entityType: 'tasks',
    entityId: 't-2',
    type: SyncOperationType.create,
    payload: const {'title': 'Second task'},
  ),
);
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
application-owned Hive boxes. The app remains responsible for the box
lifecycle.

## Transaction semantics

Hive does not provide multi-key transactions. `runWithTransaction` and
`runInTransaction` buffer outbox writes and flush them atomically on
success, providing atomicity for **outbox operations only**. Domain writes
on other Hive boxes are **not** covered by this transaction.

For atomic domain + outbox writes, consider:
- Using a single Hive box for related data.
- Application-level coordination (e.g., write-ahead logging).
- Migrating to a database with transaction support (Drift, Sqflite).

## When to call `refreshPendingCount()`

Call `refreshPendingCount()` **only** when you use `adapter.outbox` outside
of `runWithTransaction` / `runInTransaction`. The writer cannot safely emit
a count before the surrounding operation completes, so you trigger the
refresh manually after the write.

`runWithTransaction` and `runInTransaction` call `refreshPendingCount()`
automatically after the buffered writes are flushed — no manual call needed.

## Testing

```bash
melos test -p local_remote_hive
# or directly:
dart test
```

Tests use a temporary directory for Hive storage and do not require
`hive_flutter` or platform channels.

## Contributing

Follow the contribution guidelines in the root `CONTRIBUTING.md`.

---

*This adapter is part of the **offline-first synchronization** ecosystem. See
the core package README for the overall architecture.*
