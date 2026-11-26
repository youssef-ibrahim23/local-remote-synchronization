# Adapter Development Guide

This guide explains how to implement a custom **local storage adapter** or
**remote sync adapter** for the `local_remote_synchronization` ecosystem.

---

## Prerequisite Reading

Before implementing an adapter, read:

- [architecture.md](architecture.md) — dependency direction and data flow
- [package_structure.md](package_structure.md) — where adapters live
- The Core package README at `packages/local_remote_synchronization/README.md`

---

## 1. Building a Local Storage Adapter

A local storage adapter teaches the Core how to persist data and the sync queue
to a specific database technology.

### Contract to Implement

```
LocalStorageAdapter  (defined in local_remote_synchronization/src/contracts/)
```

### Responsibilities

The adapter must:

1. **Store domain records** — persist the app's entities (e.g., `Todo`, `User`).
2. **Atomically enqueue operations** — when a mutation is applied, a
   corresponding `SyncOperation` must be written **in the same transaction**.
   If the app crashes after the write but before the enqueue, data consistency
   is broken.
3. **Provide a durable queue** — the queue must survive application restarts.
   Operations must be retrievable on the next cold start.
4. **Support idempotent dequeue** — dequeuing an already-dequeued operation
   must be a no-op, not an error.

### Package Structure

```
local_remote_<technology>/
├── lib/
│   ├── local_remote_<technology>.dart      # Public barrel export
│   └── src/
│       ├── <technology>_storage_adapter.dart   # LocalStorageAdapter impl
│       ├── <technology>_sync_queue.dart         # SyncQueue impl (optional)
│       └── ...
├── test/
│   └── ...
├── pubspec.yaml
└── README.md
```

### pubspec.yaml Pattern

```yaml
name: local_remote_<technology>

dependencies:
  local_remote_synchronization:
    path: ../local_remote_synchronization   # local path during dev
  <technology_package>: ^x.y.z

dev_dependencies:
  test: ^1.x.x
  mocktail: ^1.x.x
```

> **Never** add `local_remote_supabase`, `local_remote_firestore`, or any other
> adapter package as a dependency. Local adapters must only know about the Core.

### Implementation Notes

- Operations in the queue must be **ordered by creation time** (FIFO).
- Each `SyncOperation` has a stable `operationId` (UUID). Store it and use it
  to prevent duplicate enqueues.
- Expose reactive streams where the database natively supports them
  (Drift `watch()`, Hive `watchBoxLazy()`).
- Schema migrations are your responsibility — the Core does not manage them.

---

## 2. Building a Remote Sync Adapter

A remote sync adapter teaches the Core how to push local mutations to a specific
backend service and optionally pull remote changes.

### Contract to Implement

```
RemoteSyncAdapter  (defined in local_remote_synchronization/src/contracts/)
```

### Responsibilities

The adapter must:

1. **Apply a `SyncOperation` to the remote** — translate the operation type
   (insert / update / delete) and payload into the backend's protocol.
2. **Return a `SyncResult`** — indicating success, failure, or conflict.
3. **Never know which local database is used** — the adapter only receives a
   serialised `SyncOperation` and must not import any local adapter package.
4. **Use `operationId` as an idempotency key** — submit it to the backend
   (e.g., as an HTTP `Idempotency-Key` header or a Firestore document field)
   so that retried operations are not applied twice.

### Package Structure

```
local_remote_<backend>/
├── lib/
│   ├── local_remote_<backend>.dart      # Public barrel export
│   └── src/
│       ├── <backend>_remote_adapter.dart   # RemoteSyncAdapter impl
│       └── ...
├── test/
│   └── ...
├── pubspec.yaml
└── README.md
```

### pubspec.yaml Pattern

```yaml
name: local_remote_<backend>

dependencies:
  local_remote_synchronization:
    path: ../local_remote_synchronization
  <backend_sdk_package>: ^x.y.z
```

> **Never** add sqflite, Drift, Hive, or any local adapter as a dependency.
> Remote adapters must only know about the Core and their backend SDK.

### Conflict Handling

When the backend signals a conflict (e.g., HTTP 409, Firestore precondition
failed), the adapter must return a `SyncResult` with status
`SyncResultStatus.conflict` and include the remote snapshot in the result
payload. The Core's `ConflictResolver` will then decide how to proceed.

### Error Mapping

Map backend-specific errors to `SyncError` with an appropriate `SyncErrorCode`.
Do not let raw backend exceptions leak out of the adapter.

---

## 3. Registering Your Adapter

Once implemented, wire your adapter into the `SyncEngine` during app startup:

```dart
// Pseudocode — API not yet finalised
final engine = SyncEngine(
  localAdapter: MySqfliteStorageAdapter(database: db),
  remoteAdapter: MySupabaseRemoteAdapter(client: supabase),
  connectivityProvider: MyConnectivityProvider(),
  retryPolicy: ExponentialBackoffRetryPolicy(),
  conflictResolver: LastWriteWinsConflictResolver(),
);
await engine.start();
```

---

## 4. Testing Your Adapter

### Local Adapter Testing

- Use an in-memory or temporary database for unit tests.
- Test atomicity: simulate a crash between write and enqueue (manual
  transaction abort) and verify the queue state.
- Test queue ordering: enqueue multiple operations and verify FIFO dequeue.
- Test idempotency: enqueue the same `operationId` twice and verify it appears
  only once.

### Remote Adapter Testing

- Use a mock or fake backend client (e.g., `mocktail`, `fake_cloud_firestore`).
- Test all `SyncOperationType` values.
- Test conflict response handling — fake a 409 / precondition-failed response
  and verify the returned `SyncResult.status == SyncResultStatus.conflict`.
- Test error mapping — fake a network error and verify `SyncError` is returned
  with the correct `SyncErrorCode`.

---

## 5. Publishing Your Adapter

When your adapter is ready for publication:

1. Run `dart pub publish --dry-run` and fix all issues.
2. Bump the version in `pubspec.yaml` following Semantic Versioning.
3. Update the Core path dependency to a pub.dev version constraint:

   ```yaml
   local_remote_synchronization: ^x.y.z
   ```

4. Add a comprehensive `CHANGELOG.md` entry.
5. Submit a pull request if contributing to the official ecosystem.
   Or publish independently if building a community adapter.
