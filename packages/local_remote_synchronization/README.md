# local_remote_synchronization

[![pub.dev](https://img.shields.io/pub/v/local_remote_synchronization.svg)](https://pub.dev/packages/local_remote_synchronization)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](../../LICENSE)

The **Core** package of the Local Remote Synchronization ecosystem.

This package contains **only** framework-agnostic abstractions and the
push-only sync engine:

- Contracts (interfaces) that adapter packages implement.
- Typed models shared across the ecosystem.
- The `SyncEngine` orchestration foundation (Phase 3.1 scope — see below).

It has **zero** dependency on sqflite, Drift, Hive, Firebase, Supabase, REST clients,
GraphQL clients, or any connectivity library. All integrations depend on this
package, never the other way around.

---

## Package Layout

```
lib/src/
├── contracts/      LocalStorageAdapter, RemoteSyncAdapter,
│                   ConnectivityProvider, OutboxWriter
├── models/         SyncOperation, LocalMutation, SyncResult
│                   (SyncSuccess / SyncFailure / SyncConflict), SyncError
├── enums/          SyncOperationType, SyncOperationStatus, SyncErrorCode
├── engine/         SyncEngine, SyncEngineConfig, SyncCycleReport
│                   (Phase 3.1 foundation — see below)
├── utils/          Internal helpers (deep immutability)
└── core/, queue/, scheduler/, retry/, conflict/, exceptions/
                   Reserved for later phases — currently empty
```

---

## Status

> 🚧 **In active development.** The public API is not yet stable.

| Module | Status |
|---|---|
| Contracts | ✅ Implemented |
| Models | ✅ Implemented |
| Enums | ✅ Implemented |
| Exceptions | 🗓 Planned |
| Engine | 🚧 Phase 3.1 foundation |
| Queue | 🗓 Planned |
| Scheduler | 🗓 Planned |
| Retry | 🗓 Planned |
| Conflict | 🗓 Planned |

---

## SyncEngine (Phase 3.1)

`SyncEngine` orchestrates a **push-only** sync cycle over the durable
outbox: it recovers stale `inFlight` operations on `start()`, monitors
connectivity, fetches eligible operations in batches, claims them via
`markInFlight`, pushes them through `RemoteSyncAdapter`, and applies
`markSucceeded` for confirmed results. Each cycle produces exactly one
immutable `SyncCycleReport`, available via `syncNow()` and the
`reports` stream.

Phase 3.1 provides the engine foundation only. It does **not** provide
retry scheduling or backoff, dead-letter handling, conflict detection or
resolution, per-entity ordering guarantees beyond what the storage
adapter's `preservePerEntityOrder` fetch option offers, pull
synchronization, or background/OS-level scheduling.

```dart
final engine = SyncEngine(
  localStorage: myLocalStorageAdapter,
  remoteSync: myRemoteSyncAdapter,
  connectivity: myConnectivityProvider,
  config: SyncEngineConfig(batchSize: 50, maxConcurrency: 5),
);

await engine.start();          // recovers inFlight ops, subscribes to connectivity
final report = await engine.syncNow();
await engine.dispose();
```

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
