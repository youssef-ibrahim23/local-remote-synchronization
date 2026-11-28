# local_remote_rest

[![pub.dev](https://img.shields.io/pub/v/local_remote_rest.svg)](https://pub.dev/packages/local_remote_rest)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](../../LICENSE)

**HTTP REST** implementation of `RemoteSyncAdapter` for the
[local_remote_synchronization](../local_remote_synchronization) ecosystem.

---

## Responsibility

This package bridges the Core contract `RemoteSyncAdapter` with any HTTP REST
backend via the `http` package. It translates `SyncOperation` payloads into
standard HTTP verbs (POST / PATCH-or-PUT / DELETE) and maps HTTP responses
back into Core `SyncResult` values.

**It does not know which local database is being used.**

---

## Status

> ✅ **Implemented.** Push-only REST adapter.

---

## Installation

```yaml
dependencies:
  local_remote_rest:
    path: ../local_remote_rest # or your pubspec path/git source
  local_remote_synchronization:
    path: ../local_remote_synchronization
```

---

## Usage

The application owns authentication, token refresh, and the HTTP client
lifecycle. Pass credentials through `defaultHeaders` or an injected
`http.Client`.

```dart
import 'package:http/http.dart' as http;
import 'package:local_remote_rest/local_remote_rest.dart';
import 'package:local_remote_synchronization/local_remote_synchronization.dart';

final adapter = RestRemoteSyncAdapter(
  baseUrl: 'https://api.example.com/v1/',
  entityMapping: {
    'note': RestEntityConfig(collectionPath: 'notes'),
    'todo': RestEntityConfig(
      collectionPath: 'todos',
      updateMethod: RestUpdateMethod.put,
      idField: 'todo_id', // APIs that expect the ID in the body
    ),
  },
  defaultHeaders: {
    'Authorization': 'Bearer $accessToken',
  },
  // Optional: inject your own client (e.g. with interceptors).
  // httpClient: myClient,
  requestTimeout: const Duration(seconds: 15),
);

// Later, when the adapter-owned client should be released:
adapter.close(); // no-op for injected clients
```

### Entity mapping

`entityType` is **never** treated as a URL. Each `SyncOperation.entityType`
must have an explicit `RestEntityConfig`:

| Field | Default | Purpose |
|---|---|---|
| `collectionPath` | — (required) | POST target for creates, e.g. `'notes'` |
| `itemPathTemplate` | `'{collectionPath}/{id}'` | PATCH/PUT/DELETE target; `{id}` is URI-encoded |
| `updateMethod` | `patch` | `PATCH` or `PUT` |
| `idField` | `null` | Body field that carries `entityId` |

An unmapped `entityType` returns a non-retryable `SyncErrorCode.validation`
`SyncFailure` — the adapter never guesses endpoints.

---

## Request mapping

| Core operation | HTTP request |
|---|---|
| `create` | `POST {collectionPath}` with JSON body |
| `update` | `PATCH`/`PUT {itemPath}` with JSON body |
| `delete` | `DELETE {itemPath}`, **no body** |

- Bodies are JSON (`Content-Type: application/json`).
- `SyncOperation.idempotencyKey` is sent on **every** request through the
  configured idempotency header (default `Idempotency-Key`). The adapter
  never generates a new key.
- When `idField` is configured, `entityId` is injected into the body and a
  conflicting caller-supplied value is rejected as a validation failure.
- `204 No Content`, `202 Accepted`, empty bodies, and JSON-object bodies
  are all handled. A non-empty success body must be a JSON object; anything
  else maps to a non-retryable `serialization` failure.

---

## Idempotency

Delivery is **at-least-once**. Sending an `Idempotency-Key` header does
**not** by itself deduplicate requests — actual deduplication requires the
backend to implement idempotent request handling (e.g. store-and-replay of
responses keyed by the header). Without backend support, a retried create
may produce a duplicate resource; the backend's own conflict responses
(typically `409`) are surfaced as `SyncConflict`.

No local idempotency ledger is implemented.

---

## Optimistic concurrency (ETag / If-Match)

Opt-in per operation:

```dart
SyncOperation(
  // ...
  metadata: {'baseVersion': '"v7"'}, // expectedVersionKey, default 'baseVersion'
)
```

- The value is sent verbatim in the configured request header
  (default `If-Match`).
- The configured response header (default `ETag`) is surfaced as
  `remoteVersion` on `SyncSuccess`/`SyncConflict`.
- `412 Precondition Failed` and `409 Conflict` produce `SyncConflict`.
  When the response body is a JSON object it becomes `remoteSnapshot`;
  otherwise the snapshot is empty because generic REST responses do not
  guarantee entity state.
- No version in metadata → no conditional header is sent and no
  concurrency is claimed.

**The backend must implement conditional-request semantics** (`If-Match`
evaluation against its current ETag/version). The adapter cannot detect
stale writes against backends that ignore the header.

---

## Delete semantics

`DELETE` sends no body. A `404` response is treated as an **idempotent
success** (the target is already absent), matching the repeated-delete
contract. Note the inherent ambiguity: a generic REST API cannot
distinguish "resource absent" from "resource hidden by authorization" —
a `404` caused by missing permission will also be reported as success.
Backends that need stricter semantics should return `403` for
unauthorized deletes.

Entity IDs are URI-encoded as path segments; never reused assumptions are
made beyond standard REST semantics.

---

## Error mapping

| HTTP / error | `SyncErrorCode` | Retryable |
|---|---|---|
| `400`, `422` | `validation` | No |
| `401` | `unauthorized` | No |
| `403` | `forbidden` | No |
| `404` (update/create) | `notFound` | No |
| `404` (delete) | — | Idempotent `SyncSuccess` |
| `409`, `412` | `SyncConflict` | No |
| `408` | `timeout` | Yes |
| `429` | `rateLimited` | Yes (`Retry-After` honored when present) |
| `5xx` | `serverError` | Yes |
| Other statuses | `unknown` | No |
| `http.ClientException` | `network` | Yes |
| `TimeoutException` / request timeout | `timeout` | Yes |
| Malformed/non-object JSON response | `serialization` | No |

Error messages prefer the backend's `message`/`error`/`detail`/`title`
JSON field, then a truncated raw body, then the status line. **Headers —
including `Authorization` — are never included in error messages.** `cause`
and `stackTrace` are preserved for diagnostics. Unexpected programming
errors propagate rather than being disguised as network failures.

---

## HTTP client lifecycle

- Inject `httpClient` to supply an instrumented/auth-aware client — the
  adapter **never closes** an injected client.
- When omitted, the adapter creates a default `http.Client` and closes it
  on `adapter.close()`.
- Each request is bounded by `requestTimeout` (default 30s).

---

## Delivery & scheduling guarantees

- **At-least-once** delivery; retry orchestration belongs to Core
  `SyncEngine` — the adapter performs no retries itself.
- **Push-only**: no pull synchronization, no background scheduling, no
  Flutter lifecycle integration, no conflict resolution.
- Run **one sync engine per outbox**; Core provides no cross-instance
  atomic claiming.

---

## License

MIT — see [LICENSE](../../LICENSE).
