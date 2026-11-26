# local_remote_synchronization

[![pub.dev](https://img.shields.io/pub/v/local_remote_synchronization.svg)](https://pub.dev/packages/local_remote_synchronization)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](../../LICENSE)

The **Core** package of the Local Remote Synchronization ecosystem.

This package contains **only** framework-agnostic abstractions:

- Contracts (interfaces) that adapter packages implement.
- Typed models shared across the ecosystem.
- Engine, queue, retry, and conflict-resolution skeletons.

It has **zero** dependency on sqflite, Drift, Hive, Firebase, Supabase, REST clients,
GraphQL clients, or any connectivity library. All integrations depend on this
package, never the other way around.

---

## Package Layout

```
lib/src/
├── core/           Shared base types and internal utilities
├── contracts/      Abstract interfaces: LocalStorageAdapter, RemoteSyncAdapter,
│                   ConnectivityProvider, ConflictResolver, RetryPolicy
├── models/         SyncOperation, LocalMutation, SyncResult, SyncStatus,
│                   SyncConfiguration, SyncError
├── enums/          SyncOperationType, SyncResultStatus, ConflictStrategy, …
├── exceptions/     SyncException hierarchy
├── engine/         SyncEngine and SyncProcessor skeletons (not yet implemented)
├── queue/          SyncQueue abstraction (durable, FIFO, survives restarts)
├── scheduler/      SyncScheduler skeleton
├── retry/          RetryPolicy abstraction
└── conflict/       ConflictResolver abstraction
```

---

## Status

> 🚧 **In active development.** The public API is not yet stable.

| Module | Status |
|---|---|
| Contracts | 🚧 Scaffolded |
| Models | 🚧 Scaffolded |
| Enums | 🚧 Scaffolded |
| Exceptions | 🚧 Scaffolded |
| Engine | 🗓 Planned |
| Queue | 🗓 Planned |
| Scheduler | 🗓 Planned |
| Retry | 🗓 Planned |
| Conflict | 🗓 Planned |

---

## Installation

This package is not yet published on pub.dev.
Add it via local path dependency during development:

```yaml
dependencies:
  local_remote_synchronization:
    path: ../local_remote_synchronization
```

---

## Architectural Constraints

1. The UI **must** read from local storage, not directly from the remote.
2. Local writes **must** be persisted immediately before sync is attempted.
3. A local mutation and its pending sync operation **must** be persisted atomically.
4. The remote adapter **must not** know which local database is in use.
5. The Core **must** remain independent of all concrete adapters.
6. All operations **must** be designed for idempotency.

---

## License

MIT — see [LICENSE](../../LICENSE).
