# Local Remote Synchronization

> **Database-agnostic · Backend-agnostic · Offline-First · Durable Outbox**

A modular, open-source Dart and Flutter package ecosystem for Offline-First
architectures based on the Outbox Pattern.

---

## Vision

Modern mobile apps must work reliably without an internet connection and
persist local mutations durably until they can be synchronized. **Local Remote
Synchronization** provides database-agnostic and backend-agnostic building
blocks without locking you into a specific database or backend.

The ecosystem is designed around strict **dependency inversion**: the Pure Dart
Core package defines contracts (interfaces) and immutable models, and each
concrete technology plugs in as an independent adapter package that depends on
the Core — never the other way around.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                Your Application (Flutter / Dart)            │
│   owns local domain tables & DAOs · writes locally first    │
└───────────────────┬─────────────────────────────────────────┘
                    │ depends on
┌───────────────────▼─────────────────────────────────────────┐
│          local_remote_synchronization  (Core)               │
│                 (Pure Dart — Implemented)                   │
│  Engine:    SyncEngine, SyncEngineConfig, RetryPolicy,      │
│             ExponentialBackoffRetryPolicy,                  │
│             SyncCycleReport, SyncConflictEvent              │
│  Contracts: LocalStorageAdapter, OutboxWriter,              │
│             RemoteSyncAdapter, ConnectivityProvider         │
│  Models:    LocalMutation, SyncOperation, SyncResult,       │
│             SyncError                                       │
└──────┬─────────────────────────────────────────────┬────────┘
       │ implements                                  │ implements
┌──────▼──────────┐  ┌──────────────┐    ┌──────────▼──────────────────────┐
│ local_remote_   │  │local_remote_ │    │ local_remote_supabase           │
│ sqflite         │  │drift         │    │   (Implemented)                 │
│ (Implemented)   │  │(Implemented) │    │ local_remote_firestore          │
│                 │  │local_remote_ │    │   (Implemented)                 │
│                 │  │hive          │    │ local_remote_rest               │
│                 │  │(Implemented) │    │   (Implemented)                 │
│                 │  │              │    │ local_remote_graphql            │
│                 │  │              │    │   (Implemented)                 │
└─────────────────┘  └──────────────┘    └─────────────────────────────────┘
  LocalStorageAdapter                      RemoteSyncAdapter
  (Each adapter persists its own Outbox in its chosen DB)
```

---

## Current Status

### Implemented
- **Core models, contracts & engine** (`local_remote_synchronization`):
  - Models & contracts: `LocalMutation`, `SyncOperation`, `SyncResult` (`SyncSuccess`, `SyncConflict`, `SyncFailure`), `SyncError`, `OutboxWriter`, `LocalStorageAdapter`, `RemoteSyncAdapter`, and `ConnectivityProvider`.
  - Push-only orchestration: `SyncEngine`, `SyncEngineConfig`, `SyncCycleReport`, `SyncConflictEvent`, `RetryPolicy`, and `ExponentialBackoffRetryPolicy` (with startup `inFlight` recovery, per-entity FIFO ordering, single-flight cycle deduplication, connectivity-restore triggers, and durable `deadLetter` persistence on conflicts or exhausted retries).
- **Sqflite durable outbox adapter** (`local_remote_sqflite`):
  - Transaction-safe domain writes + outbox enqueue via `runWithTransaction` and `outboxFor(txn)` + `refreshPendingCount()`.
  - Durable SQLite outbox persistence (`sync_outbox`) with UTC timestamps, JSON payload/metadata, and indexes.
  - Crash recovery (`recoverInFlightOperations`) for `inFlight` operations.
  - Per-entity FIFO eligibility (`fetchEligibleOperations`) and reactive pending counts (`watchPendingCount`).

- **Drift durable outbox adapter** (`local_remote_drift`):
  - Pure Dart implementation using the application's existing Drift `GeneratedDatabase`.
  - Zone-based transaction atomicity via `runWithTransaction` and `adapter.outbox` + `refreshPendingCount()`.
  - Durable SQLite outbox persistence (`sync_outbox`), crash recovery, per-entity FIFO, and race-free `watchPendingCount`.

- **Hive durable outbox adapter** (`local_remote_hive`):
  - Pure Dart implementation using application-owned Hive boxes.
  - Single-snapshot persistence with serialized mutations via `runWithTransaction` and `adapter.outbox` + `refreshPendingCount()`.
  - Snapshot versioning with automatic migration, corruption detection, crash recovery, per-entity FIFO, and reactive `watchPendingCount`.
  - **Consistency limitation:** A single Hive box write is not an unconditional crash-atomic or ACID guarantee. Hive may buffer writes in memory before flushing to disk.

- **Remote sync adapters**:
  - `local_remote_rest`: Generic HTTP REST push adapter (`RestRemoteSyncAdapter`, `RestEntityConfig`), sending `Idempotency-Key` and optional `If-Match` headers.
  - `local_remote_supabase`: Supabase PostgREST push adapter (`SupabaseRemoteSyncAdapter`, `SupabaseTableConfig`).
  - `local_remote_firestore`: Cloud Firestore push adapter (`FirestoreRemoteSyncAdapter`, `FirestoreEntityConfig`).
  - `local_remote_graphql`: Generic GraphQL mutation push adapter (`GraphqlRemoteSyncAdapter`, `GraphqlEntityConfig`).

- **Reference Flutter application** (`examples/basic_offline_first_app`):
  - Demonstrates local-first Todo CRUD with atomic Sqflite transactions (`TodoRepository`), `SyncEngine` lifecycle management (`AppInitializer`), `ConnectivityPlusProvider`, `RestRemoteSyncAdapter` configuration, manual `syncNow()`, and reactive sync status/error reporting.

### Out of Core Scope / Planned
- Pull synchronization.
- Automatic conflict resolution strategies (conflicts are persisted as `deadLetter` and surfaced via `SyncEngine.conflicts`).
- Platform background scheduling integrations (e.g., WorkManager, BGTaskScheduler — background execution is platform-controlled).

> **Delivery & Idempotency Note:** Retrying a failed push provides **at-least-once** delivery, not guaranteed exactly-once remote execution. Remote backends must enforce idempotency (for example, by persisting and deduplicating on `SyncOperation.idempotencyKey`) to prevent duplicate side effects.

---

## Package Ecosystem

| Package | Type | Responsibility | Status |
|---|---|---|---|
| `local_remote_synchronization` | Core (Pure Dart) | Contracts, models, enums, `SyncEngine`, `RetryPolicy` | ✅ Implemented |
| `local_remote_sqflite` | Local adapter | `LocalStorageAdapter` via Sqflite | ✅ Implemented |
| `local_remote_drift` | Local adapter | `LocalStorageAdapter` via Drift | ✅ Implemented |
| `local_remote_hive` | Local adapter | `LocalStorageAdapter` via Hive | ✅ Implemented |
| `local_remote_supabase` | Remote adapter | `RemoteSyncAdapter` via Supabase | ✅ Implemented |
| `local_remote_firestore` | Remote adapter | `RemoteSyncAdapter` via Firestore | ✅ Implemented |
| `local_remote_rest` | Remote adapter | `RemoteSyncAdapter` via REST HTTP | ✅ Implemented |
| `local_remote_graphql` | Remote adapter | `RemoteSyncAdapter` via GraphQL | ✅ Implemented |
| `basic_offline_first_app` | Reference app | End-to-end Flutter Todo example | ✅ Implemented |

---

## Project Structure

```
local-remote-synchronization/
├── packages/
│   ├── local_remote_synchronization/   # Core package
│   ├── local_remote_sqflite/           # sqflite local adapter
│   ├── local_remote_drift/             # Drift local adapter
│   ├── local_remote_hive/              # Hive local adapter
│   ├── local_remote_supabase/          # Supabase remote adapter
│   ├── local_remote_firestore/         # Firestore remote adapter
│   ├── local_remote_rest/              # REST remote adapter
│   └── local_remote_graphql/           # GraphQL remote adapter
├── examples/
│   └── basic_offline_first_app/        # Reference Flutter app
├── docs/
│   ├── architecture.md
│   ├── package_structure.md
│   └── adapter_development.md
├── scripts/                            # Utility scripts
├── melos.yaml                          # Monorepo management
└── analysis_options.yaml               # Shared lint rules
```

---

## Development Setup

### Prerequisites

- Flutter SDK ≥ 3.27.0 (Dart ≥ 3.6.0)
- [Melos](https://melos.invertase.dev/) — monorepo manager

### Install Melos

```bash
dart pub global activate melos
```

### Bootstrap the monorepo

```bash
melos bootstrap
```

This resolves dependencies across all packages and links local path overrides.

### Run analysis

```bash
melos analyze
```

### Run tests

```bash
melos test
```

### Format code

```bash
melos format:fix
```

---

## Contributing

Contributions are welcome! Please read the [Architecture Documentation](docs/architecture.md)
and the [Adapter Development Guide](docs/adapter_development.md) before opening a pull request.

---

## License

MIT — see [LICENSE](LICENSE).
