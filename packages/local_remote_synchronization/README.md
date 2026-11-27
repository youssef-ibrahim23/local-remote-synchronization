# local_remote_synchronization

[![pub.dev](https://img.shields.io/pub/v/local_remote_synchronization.svg)](https://pub.dev/packages/local_remote_synchronization)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](../../LICENSE)

The **Core** package of the Local Remote Synchronization ecosystem.

This package contains **only** framework-agnostic abstractions and the
push-only sync engine:

- Contracts (interfaces) that adapter packages implement.
- Typed models shared across the ecosystem.
- The `SyncEngine` orchestration with retry and failure handling
  (Phase 3.2 scope — see below).

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
├── engine/         SyncEngine, SyncEngineConfig, SyncCycleReport,
│                   RetryPolicy, ExponentialBackoffRetryPolicy
│                   (Phase 3.2 scope — see below)
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
| Engine | 🚧 Phase 3.2 (retry & failure handling) |
| Queue | 🗓 Planned |
| Scheduler | 🗓 Planned |
| Retry | ✅ Phase 3.2 (`RetryPolicy`) |
| Conflict | 🗓 Planned |

---

## SyncEngine (Phase 3.2)

`SyncEngine` orchestrates a **push-only** sync cycle over the durable
outbox: it recovers stale `inFlight` operations on `start()`, monitors
connectivity, fetches eligible operations in batches, claims them via
`markInFlight`, pushes them through `RemoteSyncAdapter`, and applies the
appropriate storage transition for each result. Each cycle produces
exactly one immutable `SyncCycleReport`, available via `syncNow()` and
the `reports` stream.

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

### Retry policy

Failed pushes (`SyncFailure`, including unexpected push exceptions,
which are wrapped as retryable failures) are handled by a `RetryPolicy`:

- `shouldDeadLetter(attemptCount, error)` decides whether the operation
  is dead-lettered. `attemptCount` is the number of **completed** push
  attempts (`operation.attemptCount + 1`).
- `computeDelay(attemptCount, error)` returns the delay before the next
  attempt; the engine converts it to an absolute `nextRetryAt` using its
  own clock and calls `markFailed`.

When `retryPolicy` is omitted, `ExponentialBackoffRetryPolicy` is used:
delay doubles per attempt from `initialDelay` (default 1s) up to
`maxDelay` (default 5m) with ±`jitterFactor` (default 0.2) jitter, and an
operation is dead-lettered when the error is non-retryable or
`attemptCount >= maxAttempts` (default 10). `SyncError.retryAfter` is
honored as a **minimum** delay and is never capped by `maxDelay`.

Retries happen on **future sync cycles** — there is no timer or
background scheduler; an operation becomes eligible again only when its
`nextRetryAt` is due and a new cycle runs. Each cycle also uses a single
fixed eligibility cutoff captured at cycle start, so an operation whose
retry becomes due mid-cycle is not retried in that same cycle, and an
operation is never pushed twice within one cycle.

A mismatched `result.operationId` is treated as a non-retryable protocol
violation and dead-lettered. `SyncConflict` results still abort the
cycle — conflict handling arrives in a later phase.

### Storage errors

`markSucceeded`/`markFailed`/`markDeadLetter` failures are
operation-level: they increment `SyncCycleReport.storageErrors`, do not
increment the corresponding outcome counter, and do not abort the cycle.
Persisted state is unknown after such a failure. Cycle-level storage
failures (`fetchEligibleOperations`, `markInFlight`) still set
`cycleError` and abort. `SyncCycleReport.isSuccess` is `false` whenever
`storageErrors > 0`.

Phase 3.2 does **not** provide conflict detection or resolution,
per-entity ordering guarantees beyond what the storage adapter's
`preservePerEntityOrder` fetch option offers, pull synchronization, or
background/OS-level scheduling.

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
