# local_remote_firestore

[![pub.dev](https://img.shields.io/pub/v/local_remote_firestore.svg)](https://pub.dev/packages/local_remote_firestore)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](../../LICENSE)

**Cloud Firestore** implementation of `RemoteSyncAdapter` for the
[local_remote_synchronization](../local_remote_synchronization) ecosystem.

---

## Responsibility

This package bridges the Core contract `RemoteSyncAdapter` with the
[cloud_firestore](https://pub.dev/packages/cloud_firestore) SDK. It pushes local
mutations to Firestore collections inside atomic transactions with a dedicated
idempotency ledger.

**It does not know which local database is being used.**

---

## Scope

The adapter implements `RemoteSyncAdapter.push` **only**. It does not pull
remote changes, listen to Firestore snapshots, resolve conflicts, schedule
retries, or manage Firebase initialization or authentication.

---

## Initialization

The **application owns** Firebase initialization and the `FirebaseFirestore`
instance. The adapter never initializes Firebase, never authenticates, and
never disposes the Firestore instance.

```dart
import 'package:cloud_firestore/cloud_firestore.dart';
import 'package:firebase_core/firebase_core.dart';
import 'package:local_remote_firestore/local_remote_firestore.dart';

await Firebase.initializeApp(options: DefaultFirebaseOptions.currentPlatform);

final adapter = FirestoreRemoteSyncAdapter(
  firestore: FirebaseFirestore.instance,
  entityMapping: {
    'note': FirestoreEntityConfig(
      collectionPath: 'notes',
      versionField: 'sync_version',
    ),
    'todo': FirestoreEntityConfig(
      collectionPath: 'users/user_1/todos',
    ),
  },
  idempotencyCollectionPath: '_sync_operations',
  expectedVersionKey: 'baseVersion',
);
```

## Public API

- `FirestoreRemoteSyncAdapter({required FirebaseFirestore firestore,
  required Map<String, FirestoreEntityConfig> entityMapping,
  String idempotencyCollectionPath = '_sync_operations',
  String expectedVersionKey = 'baseVersion'})`
- `FirestoreEntityConfig({required String collectionPath,
  String? versionField})`

Configuration is snapshotted at construction time. The entity mapping keys are
**logical entity types** (e.g. `'note'`), not Firestore collection names.

### `FirestoreEntityConfig`

- `collectionPath` — required collection path. Nested paths are supported
  (`'users/user_1/todos'`). Odd-length segment counts are required (a
  collection reference ends on a collection, not a document). Leading/trailing
  slashes are normalized; empty, dot, and dot-dot segments are rejected.
- `versionField` — optional application-managed integer version field name.
  When set, the adapter injects and increments this field and returns the
  version in `SyncSuccess.remoteVersion` / `SyncConflict.remoteVersion`.

### Constructor validation

The adapter throws `ArgumentError` at construction for: empty or blank entity
mapping, blank entity type keys, invalid collection paths, invalid
`idempotencyCollectionPath` (must be a valid collection path), and blank
`expectedVersionKey`.

---

## CRUD semantics

### Create

- `operation.entityId` becomes the Firestore **document ID**.
- The document is created only if it does not exist. An existing document
  without a matching ledger entry produces `SyncConflict` with the current
  remote snapshot — the adapter never silently overwrites.
- On success, `SyncSuccess.syncedData` contains the acknowledged document data
  and `remoteVersion` is `'1'` when versioning is enabled.

### Update

- Payload fields are **merged** (PATCH semantics); unrelated document fields
  are preserved. Payloads containing the reserved `versionField` are rejected
  with a non-retryable validation failure before any Firestore access.
- A missing document returns non-retryable `notFound`.
- When `versionField` is configured, the version is incremented **inside the
  transaction** and returned as `remoteVersion`.

### Delete

- The target document is deleted. A missing document is treated as idempotent
  success.
- The operation is recorded in the ledger even when the document was already
  absent, so retries of the original operation are safe.

Entity IDs containing `/`, `.`/`..`, or reserved sentinel values are rejected
with a non-retryable validation failure before any Firestore access. IDs are
otherwise used verbatim as Firestore document IDs (Firestore document IDs have
no `/`; all other characters are legal).

---

## Transactional idempotency

Every `push` runs inside a single `FirebaseFirestore.runTransaction`:

1. Read the idempotency ledger document.
2. Read the target entity document.
3. Validate the operation identity against the ledger.
4. Apply the entity mutation.
5. Write the ledger entry with the result.

All reads happen before all writes, satisfying Firestore transaction rules.
Transaction callbacks may execute more than once; the callback performs no
external side effects, emits no logs, and mutates no shared state.

### Ledger document ID

The ledger document ID is `sha256(idempotencyKey)` (hex-encoded) inside
`idempotencyCollectionPath`. It is deterministic and collision-safe.

### Ledger schema

| Field | Type | Description |
|---|---|---|
| `idempotencyKey` | string | Original operation key |
| `operationId` | string | Original `SyncOperation.operationId` |
| `entityType` | string | Logical entity type |
| `entityId` | string | Target document ID |
| `operationType` | string | `create` / `update` / `delete` |
| `payloadFingerprint` | string | SHA-256 of canonical JSON payload |
| `status` | string | `applied` |
| `remoteVersion` | string? | Version returned to the client |
| `syncedData` | map? | Acknowledged entity data |
| `createdAt` | timestamp | Server timestamp — written via `FieldValue.serverTimestamp()` |

**Replay:** a retry with the identical operation identity and payload returns
the recorded `SyncSuccess` without re-applying the mutation. This protects
against stale delete retries removing a recreated document with a reused
entity ID.

**Mismatch:** reusing the same key with a different `operationId`,
`entityType`, `entityId`, `operationType`, or payload fingerprint returns a
non-retryable validation `SyncFailure`. The adapter never treats a different
operation as the original.

No credentials or tokens are stored in the ledger.

### Retention / TTL

The adapter never deletes ledger records. You should configure a Firestore TTL
policy on the `createdAt` timestamp field (the exact field written by the
adapter) in the ledger collection so entries expire automatically. Ledger
retention **must exceed the maximum plausible retry horizon** — the longest
window in which the `SyncEngine` or application could still re-deliver the
same operation (e.g. 7–30 days, matching your retry/backoff policy). Entries
must outlive that horizon; deleting them early re-opens the
duplicate-application window.

---

## Security Rules

Your rules must permit the authenticated client to read/write:

- Every configured entity collection path (including nested segments).
- The ledger collection (`idempotencyCollectionPath`), which stores hashed
  operation metadata — treat it as sensitive sync state.

```javascript
match /notes/{docId} {
  allow read, write: if request.auth != null;
}
match /_sync_operations/{docId} {
  allow read, write: if request.auth != null;
}
// Nested paths need their own matches, e.g.:
match /users/{userId}/todos/{docId} {
  allow read, write: if request.auth != null && userId == request.auth.uid;
}
```

Firestore Security Rules evaluate per-document inside transactions. If rules
deny access, the SDK surfaces `permission-denied`, which the adapter maps to a
non-retryable `forbidden` failure. Authentication is entirely the application's
responsibility (`FirebaseAuth` sign-in before pushing); the adapter does not
refresh tokens.

---

## Optional application-managed versioning

Set `FirestoreEntityConfig.versionField` to enable optimistic concurrency:

```dart
FirestoreEntityConfig(collectionPath: 'notes', versionField: 'sync_version')
```

When `operation.metadata[expectedVersionKey]` (default `'baseVersion'`)
contains an `int`:

1. The current document's version field is read inside the transaction.
2. A mismatch returns `SyncConflict` with the current remote snapshot and the
   current version as `remoteVersion`. The mutation is not applied.
3. A match applies the mutation and increments the version.

Rules:

- Version values are strictly validated — strings, doubles, and nulls are
  rejected as non-retryable validation failures. No silent coercion.
- New documents created through `create` start at version `1`.
- If no base version is provided, **no optimistic-concurrency claim is made** —
  the update applies unconditionally (version still increments).
- Without `versionField`, no version fields are injected and no version is
  reported.
- This is **application-managed** versioning stored in your document data. It
  is not Firestore `updateTime`-based concurrency and does not detect writes
  that bypass the adapter.

---

## Error mapping

`FirebaseException` codes map to Core `SyncErrorCode`:

| Firestore code | SyncErrorCode | Retryable |
|---|---|---|
| `unauthenticated` | `unauthorized` | no |
| `permission-denied` | `forbidden` | no |
| `not-found` | `notFound` | no |
| `already-exists` | `conflict` | no |
| `aborted` | `conflict` | no |
| `failed-precondition` | `conflict` | no |
| `invalid-argument` | `validation` | no |
| `resource-exhausted` | `rateLimited` | yes |
| `unavailable` | `network` | yes |
| `deadline-exceeded` | `timeout` | yes |
| `internal` | `serverError` | yes |
| `cancelled` | `cancelled` | no |
| other | `unknown` | no |

`TimeoutException` → `timeout` (retryable), `SocketException` → `network`
(retryable). Internal validation failures → `validation` (non-retryable).
Unexpected programming errors propagate unchanged — they are never disguised
as network failures. The original exception and stack trace are preserved in
`SyncError.cause` / `stackTrace`, and every result carries the original
`operation.operationId`.

There is no automatic retry inside the adapter; the `SyncEngine` decides when
to re-push. `aborted`/`failed-precondition` mapping to `conflict` reflects
Firestore transaction contention — a retry with a fresh base version is the
correct response.

---

## Limitations

- **At-least-once delivery.** The adapter does not guarantee exactly-once
  application. The idempotency ledger makes *retries of the same
  `SyncOperation`* safe; it cannot deduplicate two operations that share an
  entity but have different idempotency keys.
- **Ledger scope.** Idempotency is per-Firestore-database. Reusing an
  idempotency key against a different backend or after ledger expiry is
  undefined behavior.
- **Transaction semantics.** Firestore transactions may retry their callback;
  contention surfaces as `aborted`/`failed-precondition` → `conflict`. Very
  hot documents increase contention.
- **Push only.** No pull, no listeners, no conflict resolution UI.
- **fake_cloud_firestore.** The in-memory fake implements `runTransaction`
  semantics faithfully for single-instance tests, but it does not simulate
  real backend contention, latency, or security-rule evaluation. Integration
  behavior against a real Firestore instance should be verified separately.
- **Document ID reuse.** Deleting a document frees its ID for reuse. The
  ledger protects retries of the *original* delete operation, but a *new*
  operation creating a document with a previously deleted ID is a distinct
  operation and is allowed.

---

## Testing

```bash
flutter test packages/local_remote_firestore
```

Tests use `fake_cloud_firestore` — no Firebase project, credentials, or
network access required.

---

## License

MIT — see [LICENSE](../../LICENSE).
