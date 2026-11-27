# local_remote_hive

[![pub.dev](https://img.shields.io/pub/v/local_remote_hive.svg)](https://pub.dev/packages/local_remote_hive)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](../../LICENSE)

A durable outbox adapter for the **local_remote_synchronization** core package, built on **Hive**.

## What it provides

- Implements the `LocalStorageAdapter` and `OutboxWriter` contracts defined by the core package.
- Persists all outbox state in a **single serialized snapshot** within one Hive box.
- Supports:
  - Transaction-scoped enqueuing via `runWithTransaction` / `runInTransaction`.
  - FIFO per-entity ordering.
  - Stable operation IDs and idempotency keys (auto-generated).
  - Reactive pending-operation count stream (`watchPendingCount`).
  - Manual `refreshPendingCount()` for external-write scenarios.
  - Crash recovery of in-flight operations (`recoverInFlightOperations`).
  - Serialized mutations to prevent race conditions.
  - Snapshot versioning with automatic migration.
  - Corruption detection with actionable exceptions.

## Installation

```yaml
dependencies:
  local_remote_hive:
    path: packages/local_remote_hive  # or from pub.dev when released
```

The core package is added transitively via the path dependency.

## Usage

### 1. Initialize Hive and open the outbox box

The application owns its Hive box and lifecycle. Initialize Hive and open
the outbox box before creating the adapter.

```dart
import 'dart:io';

import 'package:hive/hive.dart';
import 'package:local_remote_hive/local_remote_hive.dart';
import 'package:local_remote_synchronization/local_remote_synchronization.dart';

// For pure Dart (no Flutter):
await Hive.init('/path/to/hive/storage');

// For Flutter apps, use hive_flutter instead:
// await Hive.initFlutter();

// Open the outbox box.
final outboxBox = await Hive.openBox<String>('sync_outbox');
```

### 2. Create the adapter

```dart
final adapter = HiveLocalStorageAdapter(box: outboxBox);
```

The adapter does not open or close boxes — the application owns their
lifecycle. Call `ensureInitialized()` for API consistency; it is a no-op
because Hive boxes are already open.

### 3. Write domain data and enqueue a mutation with staged outbox writes

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

Outbox writes through the provided `OutboxWriter` are buffered in memory.
On success, the resulting snapshot is persisted using one `Box.put` call.
If the action throws, all buffered writes are discarded.

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
application-owned Hive box. The app remains responsible for the box
lifecycle.

## Persistence design

### Single-snapshot model

All outbox state is stored in **one JSON snapshot** under the key
`outbox_snapshot` in the Hive box. The snapshot contains:

- `schemaVersion` — snapshot format version (currently `2`)
- `lastSequenceNumber` — monotonically increasing sequence counter
- `operations` — array of all active `SyncOperation` records

Each mutation loads the snapshot, applies changes in memory, and writes the
complete snapshot back using a **single `Box.put` call**. This reduces
multi-key consistency problems by keeping all outbox state in one key.

**Durability limitation:** Hive does NOT provide an ACID transaction or an
unconditional crash-atomic durability guarantee. A successful `Box.put`
must not be presented as an absolute guarantee against data loss after a
process or system crash. Hive may buffer writes in memory before flushing
to disk. For stronger durability guarantees, use a database with explicit
transaction support (Drift, Sqflite).

### Transaction semantics

`runWithTransaction` and `runInTransaction` work as follows:

1. Acquire the adapter's internal mutex (serializes all mutations).
2. Load the current snapshot from the box.
3. Create a working copy of the snapshot.
4. Run the action with a buffered `OutboxWriter`.
5. On success: flush buffered operations to the working copy and persist
   the complete snapshot in one write.
6. On failure: discard the working copy — no changes are persisted.

Hive does not provide multi-key transactions or an ACID guarantee. This
method stages all outbox changes in memory and persists the resulting
snapshot in a single write, which reduces multi-key consistency problems.
It does not cover domain writes on other Hive boxes.

For atomic domain + outbox writes, consider:
- Using a single Hive box for related data.
- Application-level coordination (e.g., write-ahead logging).
- Migrating to a database with transaction support (Drift, Sqflite).

## Serialized mutation model

All mutations are serialized through an internal async mutex. This prevents
race conditions in sequence number allocation and ensures read-modify-write
cycles are consistent within the adapter instance.

The following operations are serialized:
- `runInTransaction` / `runWithTransaction`
- `outbox.enqueue` (direct writes)
- `markInFlight`, `markSucceeded`, `markFailed`, `markDeadLetter`
- `recoverInFlightOperations`

**Note:** The mutex is per-adapter-instance. If multiple adapter instances
share the same Hive box, they can still race. Use a single adapter instance
per box.

## Corruption handling

If the persisted snapshot is corrupted or contains invalid data, the adapter
throws a `HiveOutboxCorruptionException` with details about the corruption.

The original persisted data is **preserved** — the adapter never silently
deletes or overwrites corrupted data. This allows manual inspection and
recovery.

Corruption scenarios that throw exceptions:
- Malformed JSON
- Snapshot is not a JSON object
- Missing or invalid `schemaVersion`, `lastSequenceNumber`, or `operations`
- Invalid operation records within the snapshot

## Snapshot versioning and migration

The snapshot includes a `schemaVersion` field. The adapter validates this
version when loading persisted state:

- **Current version** (`2`): Loaded directly.
- **Supported older versions** (`1`): Automatically migrated to current.
  - v1 → v2: `lastSequenceNumber` is computed from the maximum sequence
    number in operations if missing.
- **Unsupported future versions** (`> 2`): Throws
  `HiveOutboxVersionException` with a clear message.
- **Unsupported old versions** (`< 1`): Throws `HiveOutboxVersionException`.

The adapter never silently resets or overwrites an unsupported snapshot.

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
