# local_remote_supabase

[![pub.dev](https://img.shields.io/pub/v/local_remote_supabase.svg)](https://pub.dev/packages/local_remote_supabase)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](../../LICENSE)

**Supabase** implementation of `RemoteSyncAdapter` for the
[local_remote_synchronization](../local_remote_synchronization) ecosystem.

---

## Responsibility

This package bridges the Core contract `RemoteSyncAdapter` with the
[supabase_flutter](https://pub.dev/packages/supabase_flutter) client. It
applies local mutations (`create` / `update` / `delete`) to Supabase
PostgreSQL tables through PostgREST.

**It does not know which local database is being used**, does not own the
outbox, and does not initialize Supabase — your application owns the
`SupabaseClient` (including `Supabase.initialize`) and passes it in.

---

## Installation

```yaml
dependencies:
  local_remote_supabase:
    path: ../local_remote_supabase # or hosted version
```

```dart
import 'package:local_remote_supabase/local_remote_supabase.dart';
```

## Usage

```dart
final adapter = SupabaseRemoteSyncAdapter(
  client: Supabase.instance.client, // your existing client
  tableMapping: {
    'note': SupabaseTableConfig(
      tableName: 'notes',
      // primaryKeyColumn: 'id',          // default
      // idempotencyColumn: '_sync_operation_id', // default
      versionColumn: 'version',          // optional optimistic concurrency
      // expectedVersionKey: 'baseVersion',      // default metadata key
    ),
    'todo': SupabaseTableConfig(tableName: 'todos'),
  },
);

final engine = SyncEngine(
  storage: myLocalAdapter,
  remote: adapter,
  connectivity: myConnectivity,
  // ...
);
```

`entityType` on each `SyncOperation` selects the table. An unmapped
`entityType` produces a **non-retryable validation `SyncFailure`** — there
is no fallback to arbitrary table names.

## Required schema

Every mapped table must have:

- A **primary key column** (default `id`) matching `SyncOperation
  .entityId`.
- An **idempotency column** (default `_sync_operation_id`) that stores the
  `SyncOperation.idempotencyKey` of the last applied operation. It must be
  readable and writable under your RLS policies.

Example:

```sql
alter table public.notes
  add column if not exists _sync_operation_id text;

create index if not exists notes_sync_op_idx
  on public.notes (_sync_operation_id);

-- Optional, for optimistic concurrency:
alter table public.notes
  add column if not exists version bigint not null default 1;
```

The adapter never alters your schema. Add columns via your own
migrations.

## Operation semantics

### Create

- `entityId` is injected into the primary-key column. If the payload
  already contains the PK it must equal `entityId` — a mismatch is a
  non-retryable validation failure, never a silent overwrite.
- `idempotencyKey` is written to the idempotency column.
- On a `23505` unique violation the adapter fetches the row by primary
  key: matching idempotency key → `SyncSuccess` (already-applied retry);
  different key → `SyncConflict` with the remote snapshot; violation on
  another constraint or an RLS-hidden row → non-retryable conflict
  failure. Unique violations are **never** blindly treated as success.

### Update

- Requires a non-null payload. Payload keys matching the primary-key or
  idempotency columns must equal the operation's values (mismatch →
  validation failure); the server-managed version column is stripped.
- The current row is fetched first: missing → `notFound` (non-retryable);
  idempotency key already matches → `SyncSuccess` without reapplying.
- With `versionColumn` configured **and** `metadata['baseVersion']`
  supplied, the remote version must match or a `SyncConflict` is
  returned. The update itself is issued as a conditional
  `WHERE version = expected` write, so a race between read and write
  cannot silently overwrite — an empty result re-reads the row and
  reports `SyncConflict` (or `notFound` if it vanished).
- Without a configured/supplied version, no optimistic-concurrency claim
  is made.

### Delete

- Filters only on the primary key — payload is never used as a filter.
- An already-absent row is an idempotent `SyncSuccess`.
- With a supplied expected version, the delete is conditional on it;
  mismatch → `SyncConflict` with the remote snapshot.

> **RLS ambiguity:** PostgREST cannot distinguish "row absent" from
> "row hidden by RLS" — both produce an empty filtered response. A
> successful delete means *no row matched under the caller's visibility*.
> If your RLS policies hide rows the user shouldn't delete, ensure delete
> policies are correct; the adapter cannot detect the difference.

## Conflicts

Detected conflicts return Core `SyncConflict` with the latest remote row
as `remoteSnapshot`, `remoteVersion` populated from `versionColumn` when
configured, and `serverTimestamp` left `null` (PostgREST does not expose a
trusted server timestamp). Core dead-letters the operation and emits its
conflict event — **no conflict resolution is performed here**.

## Error mapping

| Condition                                   | `SyncErrorCode`   | Retryable |
|---------------------------------------------|-------------------|-----------|
| Socket/DNS/`ClientException`, `PGRST000`    | `network`         | yes       |
| `TimeoutException`                          | `timeout`         | yes       |
| HTTP 401, `PGRST301/302`, `AuthException`   | `unauthorized`    | no        |
| HTTP 403, SQLSTATE `42501`                  | `forbidden`       | no        |
| HTTP 404, `PGRST116`, missing update target | `notFound`        | no        |
| HTTP 409, SQLSTATE `23505`                  | `conflict`        | no        |
| HTTP 400/406/422, SQLSTATE 22/23/42, PGRST* | `validation`      | no        |
| HTTP 429                                    | `rateLimited`     | yes       |
| HTTP 5xx, `40001`/`40P01`, `PGRST003`       | `serverError`     | yes       |
| Invalid JSON / parse failure                | `serialization`   | no        |
| Request aborted                             | `cancelled`       | no        |
| Anything else                               | propagates        | —         |

`SyncError` preserves the backend message, the HTTP status code when the
SDK exposes one, the original exception as `cause`, and the stack trace
as a diagnostic-only field. `retryAfter` is always `null` — the Supabase
SDK does not surface a reliable retry-after value and none is invented.
No tokens or headers are included in errors.

## Idempotency & delivery guarantees

- `idempotencyKey` is stable across all retries (generated by Core); the
  adapter never generates a new one.
- The idempotency column records the **last applied operation key** per
  row: retried creates/updates are recognised and skipped.
- Delivery is **at-least-once**, not exactly-once. Duplicate remote
  effects remain possible when `markSucceeded` fails after a successful
  push, or via triggers/functions/concurrent clients outside the row.
- Run **one engine per outbox** — there is no cross-instance atomic
  claiming.

## Authentication & RLS

The adapter uses whatever session the supplied `SupabaseClient` already
has (anonymous, authenticated user, etc.). It works under your RLS
policies; it never embeds or requires a service-role key. Ensure RLS
permits the operations your app performs, including read access to the
idempotency and version columns.

## Limitations

- Push-only: no pull synchronization, no background scheduling, no
  Flutter lifecycle integration.
- Optimistic concurrency is opt-in per table via `versionColumn` +
  `baseVersion` metadata; tables without it get no version protection.
- Delete success under RLS is ambiguous (see above).
- `serverTimestamp` is never populated — PostgREST provides no reliable
  per-row server timestamp unless you add one yourself.

## Testing

Unit tests run against a fake `http.Client` injected through
`SupabaseClient(httpClient:)` — no live Supabase project required.

## License

MIT — see [LICENSE](../../LICENSE).
