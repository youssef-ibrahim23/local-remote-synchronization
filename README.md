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
│  Contracts: LocalStorageAdapter, OutboxWriter,              │
│             RemoteSyncAdapter, ConnectivityProvider         │
│  Models:    LocalMutation, SyncOperation, SyncResult,       │
│             SyncError                                       │
└──────┬─────────────────────────────────────────────┬────────┘
       │ implements                                  │ implements
┌──────▼──────────┐  ┌──────────────┐    ┌──────────▼──────────────────────┐
│ local_remote_   │  │local_remote_ │    │ local_remote_supabase (Planned) │
│ sqflite         │  │drift         │    │ local_remote_firestore (Planned)│
│ (Implemented)   │  │(Implemented) │    │ local_remote_rest (Planned)     │
│                 │  │local_remote_ │    │ local_remote_graphql (Planned)  │
│                 │  │hive (Planned)│    │                                 │
└─────────────────┘  └──────────────┘    └─────────────────────────────────┘
  LocalStorageAdapter                      RemoteSyncAdapter
  (Each adapter persists its own Outbox in its chosen DB)
```

---

## Current Status

### Implemented
- **Core models & contracts** (`local_remote_synchronization`): `LocalMutation`, `SyncOperation`, `SyncResult` (`SyncSuccess`, `SyncConflict`, `SyncFailure`), `SyncError`, `OutboxWriter`, `LocalStorageAdapter`, `RemoteSyncAdapter`, and `ConnectivityProvider`.
- **Sqflite durable outbox adapter** (`local_remote_sqflite`):
  - Transaction-safe domain writes + outbox enqueue via `runWithTransaction` and `outboxFor(txn)` + `refreshPendingCount()`.
  - Durable SQLite outbox persistence (`sync_outbox`) with UTC timestamps, JSON payload/metadata, and indexes.
  - Crash recovery (`recoverInFlightOperations`) for `inFlight` operations.
  - Per-entity FIFO eligibility (`fetchEligibleOperations`) and reactive pending counts (`watchPendingCount`).

- **Drift durable outbox adapter** (`local_remote_drift`):
  - Pure Dart implementation using the application's existing Drift `GeneratedDatabase`.
  - Zone-based transaction atomicity via `runWithTransaction` and `adapter.outbox` + `refreshPendingCount()`.
  - Durable SQLite outbox persistence (`sync_outbox`), crash recovery, per-entity FIFO, and race-free `watchPendingCount`.

### Planned
- `SyncEngine` and push orchestration.
- `RetryPolicy` implementations and retry orchestration.
- `ConflictResolver` implementations.
- `local_remote_hive` local adapter.
- `local_remote_supabase`, `local_remote_firestore`, `local_remote_rest`, and `local_remote_graphql` remote adapters.
- Optional platform background scheduling integrations (out of Core scope).

---

## Package Ecosystem

| Package | Type | Responsibility | Status |
|---|---|---|---|
| `local_remote_synchronization` | Core (Pure Dart) | Contracts, models, enums | ✅ Phase 1 Implemented |
| `local_remote_sqflite` | Local adapter | `LocalStorageAdapter` via Sqflite | ✅ Phase 2.1 Implemented |
| `local_remote_drift` | Local adapter | `LocalStorageAdapter` via Drift | ✅ Phase 2.2 Implemented |
| `local_remote_hive` | Local adapter | `LocalStorageAdapter` via Hive | 🗓 Planned |
| `local_remote_supabase` | Remote adapter | `RemoteSyncAdapter` via Supabase | 🗓 Planned |
| `local_remote_firestore` | Remote adapter | `RemoteSyncAdapter` via Firestore | 🗓 Planned |
| `local_remote_rest` | Remote adapter | `RemoteSyncAdapter` via REST HTTP | 🗓 Planned |
| `local_remote_graphql` | Remote adapter | `RemoteSyncAdapter` via GraphQL | 🗓 Planned |

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

- Flutter SDK ≥ 3.19.0 (Dart ≥ 3.3.0)
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
