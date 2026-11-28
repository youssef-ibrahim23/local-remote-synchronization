# Changelog — local_remote_rest

## [Unreleased]

### Added

- `RestRemoteSyncAdapter` implementing Core `RemoteSyncAdapter` over HTTP:
  POST creates, PATCH/PUT updates, DELETE deletes.
- `RestEntityConfig` with collection path, item path template (`{id}`
  placeholder, URI-encoded), PATCH/PUT selection, and optional `idField`.
- Configurable idempotency header (default `Idempotency-Key`), ETag-style
  conditional requests (`If-Match`/`ETag`, `baseVersion` metadata),
  default headers, request timeout, and optional `http.Client` injection
  with correct ownership semantics.
- Structured error mapping for HTTP statuses, `ClientException`,
  `TimeoutException`, and malformed JSON responses.
- Idempotent delete-on-404 semantics (documented authorization ambiguity).
- Deterministic unit tests with a fake `http.Client` (no network).
- URI safety hardening: endpoint paths reject absolute URLs, schemes,
  authorities, query/fragment delimiters, and dot segments (including
  percent-encoded forms); resolved request URIs are verified to stay on
  the configured origin; dot-only entity IDs are rejected safely.
- `requestTimeout` now covers the entire HTTP operation including body
  consumption, is validated as a positive duration at construction, and
  cancels stalled body subscriptions on timeout.
