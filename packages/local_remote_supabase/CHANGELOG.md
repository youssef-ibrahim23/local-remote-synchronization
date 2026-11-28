# Changelog — local_remote_supabase

## [Unreleased]

### Added

- `SupabaseRemoteSyncAdapter` implementing Core `RemoteSyncAdapter` with
  create/update/delete via PostgREST, per-row idempotency column dedup,
  optional optimistic concurrency, structured error mapping, and
  deterministic fake-HTTP tests.
- `SupabaseTableConfig` entity-type → table mapping with validated
  primary-key, idempotency, version, and metadata-key columns.
- README documenting required schema (idempotency column, version
  increment trigger), RLS requirements, error mapping, at-least-once
  delivery semantics, delete entity-ID reuse rules, and RLS ambiguity.

### Fixed

- Optimistic concurrency: documented that version progression requires a
  server-side increment trigger; `remoteVersion` is read from the server
  row, never computed client-side.
- `AuthException`/`AuthApiException` mapping now uses explicit HTTP status
  mapping (401/403/429/5xx/400/422) instead of classifying everything
  except 403 as `unauthorized`.
