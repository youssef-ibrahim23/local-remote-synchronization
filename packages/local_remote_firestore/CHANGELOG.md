# Changelog — local_remote_firestore

## [Unreleased]

### Added

- `FirestoreRemoteSyncAdapter` implementing `RemoteSyncAdapter.push`:
  - Create / update / delete mapped to Firestore document operations.
  - Entity ID used as the Firestore document ID; creates never overwrite
    existing documents without a ledger match.
  - PATCH-style updates that preserve unrelated fields.
- Transactional idempotency ledger (`idempotencyCollectionPath`,
  SHA-256-keyed document IDs) storing operation identity, payload
  fingerprint, and result for safe at-least-once retries.
- Optional application-managed optimistic concurrency via
  `FirestoreEntityConfig.versionField` and `expectedVersionKey` metadata.
- `FirestoreEntityConfig` for entity-type → collection-path mapping with
  nested path support and strict validation.
- Dedicated Firestore/Firebase error mapping to Core `SyncErrorCode` with
  precise retryability.
- Comprehensive unit tests using `fake_cloud_firestore` (CRUD, idempotency,
  versioning, error mapping, validation).

### Fixed

- Ledger timestamp field renamed to `createdAt` (written via
  `FieldValue.serverTimestamp()`) so the schema matches the documented TTL
  instructions. **Compatibility:** ledgers written by earlier builds used
  `appliedAt`; any TTL policy or tooling keyed on the old field name must be
  updated. This package has not been released, so no migration path is
  provided.
- `expectedVersionKey` metadata is now strictly validated: an absent key
  still means "no concurrency check", but a present key with an explicit
  `null`, `String`, `double`, or other non-`int` value is rejected with a
  non-retryable `validation` failure instead of being treated as absent.
- Versioned deletes now return `SyncConflict` when the stored document
  version is missing or malformed, matching update behavior — malformed
  version data can no longer silently bypass the versioning contract.
