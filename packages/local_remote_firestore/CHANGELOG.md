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
