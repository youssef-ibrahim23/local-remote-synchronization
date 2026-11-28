# Changelog — local_remote_graphql

## [Unreleased]

### Added

- `GraphQLRemoteSyncAdapter` implementing `RemoteSyncAdapter.push` for any
  GraphQL backend through an application-owned `GraphQLClient`.
- `GraphQLEntityConfig` / `GraphQLOperationConfig`: per-entity create,
  update, and delete mutation documents plus variables-builder callbacks.
- `GraphQLResponseParser` and typed `GraphQLMutationOutcome` values
  (success / conflict / failure) for backend-specific response
  interpretation.
- Stable `idempotencyKey` forwarding through variables builders;
  at-least-once delivery semantics documented.
- GraphQL execution-error mapping via `extensions.code`, HTTP status
  mapping via `ServerException`, transport error classification, and
  `Retry-After` support.
- Mutations execute with `FetchPolicy.noCache`.
- Switched from `graphql_flutter`/Flutter SDK to the pure-Dart `graphql`
  package; tests use `package:test` with a deterministic fake `Link`.

### Fixed

- HTTP status mapping now covers `400`/`422`→validation and `408`→
  retryable timeout; `ServerException`s carrying parsed GraphQL errors
  prefer a recognized `extensions.code` over the generic HTTP status.
- Idempotency documentation corrected: `operation.idempotencyKey` is
  available to variables builders but is not injected automatically —
  applications must include it explicitly when backend idempotency is
  required.
