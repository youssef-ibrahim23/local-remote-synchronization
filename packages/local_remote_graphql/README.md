# local_remote_graphql

[![pub.dev](https://img.shields.io/pub/v/local_remote_graphql.svg)](https://pub.dev/packages/local_remote_graphql)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](../../LICENSE)

**GraphQL** implementation of `RemoteSyncAdapter` for the
[local_remote_synchronization](../local_remote_synchronization) ecosystem.

---

## Responsibility

This package bridges the Core contract `RemoteSyncAdapter` with any GraphQL
backend through an application-owned `GraphQLClient`. It executes
application-configured mutation documents, forwards the stable
`SyncOperation.idempotencyKey` through configured variables builders,
interprets GraphQL transport/execution errors, and converts mutation
outcomes into Core `SyncResult` values.

**It does not know which local database is being used.**

---

## Scope

Push-only. The adapter does **not** pull remote changes, run subscriptions,
resolve conflicts, schedule retries, manage authentication, or own the
GraphQL client lifecycle.

---

## Dependencies

Pure Dart — no Flutter SDK dependency. Runtime dependencies:

- `local_remote_synchronization` (Core contracts)
- `graphql` (`GraphQLClient`, `MutationOptions`, `QueryResult`, error types)
- `gql` (`DocumentNode` AST)
- `http` (transport error classification)

Tests use `package:test` — no Flutter test runner required.

---

## Setup

The application owns the `GraphQLClient`: its `Link` chain, endpoint URL,
authentication headers, token refresh, and transport. The adapter never
initializes an endpoint, never manages credentials, and never disposes the
injected client.

```dart
import 'package:gql/language.dart';
import 'package:graphql/client.dart';
import 'package:local_remote_graphql/local_remote_graphql.dart';

final httpLink = HttpLink('https://api.example.com/graphql');

// Application-owned link chain: auth, retry, logging, etc.
final link = AuthLink(
  getToken: () async => 'Bearer $token',
  child: httpLink,
);

final client = GraphQLClient(
  link: link,
  cache: GraphQLCache(),
);

final adapter = GraphQLRemoteSyncAdapter(
  client: client,
  entityMapping: {
    'note': GraphQLEntityConfig(
      create: GraphQLOperationConfig(
        document: parseString('''
          mutation CreateNote($input: NoteInput!) {
            createNote(input: $input) { id version }
          }
        '''),
        variables: (op) => {
          'input': {
            'id': op.entityId,
            'idempotencyKey': op.idempotencyKey,
            'data': op.payload,
          },
        },
      ),
      update: GraphQLOperationConfig(
        document: parseString('''
          mutation UpdateNote($id: ID!, $patch: NotePatch!, $baseVersion: Int) {
            updateNote(id: $id, patch: $patch, baseVersion: $baseVersion) {
              id version
            }
          }
        '''),
        variables: (op) => {
          'id': op.entityId,
          'patch': op.payload,
          // Optional optimistic concurrency — application-defined.
          'baseVersion': op.metadata['baseVersion'],
          'idempotencyKey': op.idempotencyKey,
        },
      ),
      delete: GraphQLOperationConfig(
        document: parseString('''
          mutation DeleteNote($id: ID!, $key: String!) {
            deleteNote(id: $id, idempotencyKey: $key)
          }
        '''),
        variables: (op) => {
          'id': op.entityId,
          'key': op.idempotencyKey,
        },
      ),
    ),
  },
  responseParser: parseNoteResponse,
);
```

### Entity mapping

`entityMapping` keys are Core `entityType` values — they are **not**
assumed to match GraphQL type names, collection names, or mutation names.
An unmapped entity type returns a non-retryable `validation` failure.

`GraphQLEntityConfig` declares which operations an entity supports
(`create`, `update`, `delete`). Each is a `GraphQLOperationConfig` with:

- `document` — a parsed `DocumentNode` for the mutation.
- `variables` — a `GraphQLVariablesBuilder` callback receiving the original
  `SyncOperation` and returning the variables map.

A missing operation configuration produces a non-retryable `validation`
failure rather than an exception.

### Variables builders

The builder receives the **original** `SyncOperation` — `entityId`,
`payload`, `metadata`, `entityType`, `type`, `createdAt`, and the stable
`idempotencyKey`. Map them into whatever variable names the backend expects.
The adapter never invents variable names. A builder that throws produces a
non-retryable `validation` failure before any request is sent.

### Response parser

GraphQL response shapes are backend-specific. `responseParser` receives the
original `SyncOperation` and the top-level response `data` map, and returns
a typed `GraphQLMutationOutcome`:

```dart
GraphQLMutationOutcome parseNoteResponse(
  SyncOperation op,
  Map<String, dynamic> data,
) {
  final result = data['updateNote'] as Map<String, dynamic>;
  if (result['conflict'] == true) {
    return GraphQLMutationConflict(
      remoteSnapshot: result['remote'] as Map<String, dynamic>,
      remoteVersion: result['version']?.toString(),
    );
  }
  return GraphQLMutationSuccess(
    remoteVersion: result['version']?.toString(),
    syncedData: result,
  );
}
```

- `GraphQLMutationSuccess` → `SyncSuccess` (`remoteVersion`,
  `serverTimestamp`, `syncedData` all optional).
- `GraphQLMutationConflict` → `SyncConflict` (remote snapshot + optional
  version for conflict resolution).
- `GraphQLMutationFailure` → `SyncFailure` wrapping a fully-formed
  `SyncError` you construct.

When `responseParser` is omitted, every interpretable response becomes a
plain `SyncSuccess` with no version or data claims.

A parser that throws is reported as a non-retryable `serialization`
failure with the original error preserved as `cause` — the adapter never
fabricates success from an uninterpretable response.

---

## CRUD semantics

- **Create / update / delete** each execute their configured mutation with
  variables from the builder. Delete operations may have a `null` payload.
- Mutations run with `FetchPolicy.noCache` — sync writes never read from or
  pollute the application's GraphQL cache.
- GraphQL errors and link exceptions take precedence over response data: a
  mutation reporting errors is never treated as a success, even when `data`
  is also present.
- `null` or empty `data` without GraphQL errors is a non-retryable
  `serialization` failure.

---

## Idempotency and delivery guarantees

- The adapter always forwards `operation.idempotencyKey` — through the
  variables builder — and **never generates a new key**. Retries of the
  same `SyncOperation` reuse the same key.
- Sending a key does **not** guarantee deduplication. Actual deduplication
  requires backend-side idempotency support keyed on that value.
- Delivery is **at-least-once**: Core retries may execute the same mutation
  more than once. There is no adapter-owned ledger or exactly-once claim.

---

## Error mapping

### GraphQL execution errors (`errors[].extensions.code`)

| Code | SyncErrorCode | Retryable |
|---|---|---|
| `UNAUTHENTICATED`, `UNAUTHORIZED` | `unauthorized` | no |
| `FORBIDDEN`, `PERMISSION_DENIED` | `forbidden` | no |
| `NOT_FOUND` | `notFound` | no |
| `CONFLICT`, `VERSION_CONFLICT` | `conflict`* | no |
| `BAD_USER_INPUT`, `VALIDATION_ERROR`, `INVALID_ARGUMENT`, `GRAPHQL_VALIDATION_FAILED` | `validation` | no |
| `RATE_LIMITED`, `TOO_MANY_REQUESTS`, `THROTTLED` | `rateLimited` | yes |
| `SERVICE_UNAVAILABLE`, `UNAVAILABLE` | `network` | yes |
| `DEADLINE_EXCEEDED`, `TIMEOUT`, `TIMED_OUT` | `timeout` | yes |
| `INTERNAL_SERVER_ERROR`, `INTERNAL` | `serverError` | yes |
| `CANCELLED`, `CANCELED` | `cancelled` | no |
| other / missing | `unknown` | no |

\* Conflict-coded errors become `SyncConflict` when
`extensions.remoteSnapshot` (a map) is present; `extensions.remoteVersion`
is used as the version when available. Otherwise they become a
non-retryable `conflict` `SyncFailure`.

Codes are matched case-insensitively (uppercased before lookup).

### Transport errors

- `ServerException` / `HttpLinkServerException` HTTP status mapping:
  `401`→unauthorized, `403`→forbidden, `404`→notFound,
  `409`/`412`→conflict, `429`→rateLimited (honors `Retry-After` seconds),
  `5xx`→serverError — all non-retryable except 429/5xx. `statusCode` is
  preserved on `SyncError.statusCode`.
- `SocketException`, `HandshakeException`, `http.ClientException` →
  `network`, retryable.
- `TimeoutException` → `timeout`, retryable.
- Other `LinkException`s unwrap `originalException` and re-map; unmapped
  ones → `unknown`, non-retryable.
- Dart `Error` subtypes (`StateError`, `TypeError`, …) signal programming
  bugs — they propagate rather than becoming sync failures.
- Every result preserves the exact `operation.operationId`; original
  exceptions and stack traces are kept in `SyncError.cause`/`stackTrace`.
  Error messages never include authorization headers or tokens.

---

## Optional optimistic concurrency

There is no built-in version field. Applications implement concurrency by
mapping `operation.metadata` entries into mutation variables (e.g.
`baseVersion`) inside the variables builder, and reporting conflicts via
the response parser (`GraphQLMutationConflict` or a conflict-coded
GraphQL error). The adapter itself makes no concurrency claims.

---

## Client ownership and lifecycle

- The adapter holds the injected `GraphQLClient` read-only; it never
  closes links, clears the cache, or disposes the client.
- Authentication, header injection, token refresh, retries at the link
  level, and endpoint configuration are entirely application-owned via
  the `Link` chain.
- `entityMapping` is snapshotted (unmodifiable copy) at construction.

---

## Testing

```bash
dart test packages/local_remote_graphql
```

Tests use a deterministic fake `Link` (`test/support/fake_graphql_link.dart`)
that records requests and replays queued responses/errors — no server,
credentials, or network required. `package:test` is used; Flutter is not
required to run them.

---

## Limitations

- **At-least-once delivery.** No exactly-once guarantee without backend
  idempotency keyed on the forwarded key.
- **Push only.** No pull, subscriptions, listeners, or conflict-resolution
  workflow.
- **No schema awareness.** Mutation documents, variable names, response
  shapes, and conflict representations are entirely application-defined.
- **GraphQL errors win over data.** A response containing both `data` and
  `errors` is treated as a failure, never a success.
- **Cache bypass.** `FetchPolicy.noCache` means mutation results are not
  written to the app's `GraphQLCache`; applications needing cache updates
  must handle them through their own link/cache layer.
- **No automatic retries.** Retry policy belongs to the `SyncEngine`.

---

## License

MIT — see [LICENSE](../../LICENSE).
