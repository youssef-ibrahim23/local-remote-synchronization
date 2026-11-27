# Phase 3 — Sync Engine Architecture

> **Status:** Design proposal — revised after architecture review.
>
> **Scope:** Push-only synchronization orchestration. No pull, no conflict
> resolution strategies, no background scheduling, no Flutter lifecycle.

---

## 1. Proposed Architecture

The `SyncEngine` is a pure-Dart orchestration layer that sits between the
existing Core contracts and the application's adapters. It depends only on
`LocalStorageAdapter`, `RemoteSyncAdapter`, `ConnectivityProvider`, and Core
models — never on any concrete database or backend.

```
┌─────────────────────────────────────────────────────────────┐
│                    Application Layer                        │
│  Enqueues LocalMutations via OutboxWriter / runInTransaction│
│  Subscribes to SyncEngine.conflicts for conflict handling   │
│  Calls SyncEngine.syncNow() for manual synchronization      │
└──────────────────────────┬──────────────────────────────────┘
                           │ uses
┌──────────────────────────▼──────────────────────────────────┐
│                      SyncEngine (Core)                      │
│                                                             │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────┐  │
│  │ Connectivity │  │   SyncCycle  │  │   RetryPolicy    │  │
│  │   Monitor    │  │   Executor   │  │  (Exponential    │  │
│  │              │  │              │  │   Backoff+Jitter)│  │
│  └──────────────┘  └──────────────┘  └──────────────────┘  │
│                                                             │
│  Lifecycle: start() → syncNow() / auto-trigger → dispose()  │
└──────┬──────────────────┬──────────────────┬────────────────┘
       │ uses             │ uses             │ uses
┌──────▼──────────┐ ┌─────▼──────────────┐ ┌▼───────────────────┐
│ LocalStorage    │ │ RemoteSyncAdapter  │ │ Connectivity       │
│ Adapter         │ │ (push-only)        │ │ Provider           │
│ (contract)      │ │ (contract)         │ │ (contract)         │
└──────┬──────────┘ └─────┬──────────────┘ └┬──────────────────┘
       │ implemented by   │ implemented by   │ implemented by
┌──────▼──────────┐ ┌─────▼──────────────┐ ┌▼───────────────────┐
│ Sqflite/Drift/  │ │ Supabase/Firestore/│ │ connectivity_plus  │
│ Hive adapters   │ │ REST/GraphQL       │ │ or custom          │
│ (future)        │ │ (future)           │ │                    │
└─────────────────┘ └────────────────────┘ └────────────────────┘
```

**Dependency direction:** The `SyncEngine` **uses** the three contracts
(`LocalStorageAdapter`, `RemoteSyncAdapter`, `ConnectivityProvider`).
Concrete adapter packages **implement** those contracts. The engine does
not implement any adapter contract and does not depend on any concrete
adapter package.

### Placement

`SyncEngine` and its supporting types live in the Core package
(`local_remote_synchronization`) under `lib/src/engine/`. This is consistent
with the architecture document which already lists `SyncEngine` and
`RetryPolicy` as planned Core components. No new package is created.

**Rationale:** The engine has zero dependencies beyond `dart:async` and
`dart:math`. It introduces no platform or database coupling. Keeping it in
Core avoids forcing consumers to add another package for basic sync
orchestration.

---

## 2. Component Responsibilities

### `SyncEngine`

The central orchestrator. Responsibilities:

- Recover stale `inFlight` operations **once** during successful `start()`.
- Trigger sync cycles on connectivity restoration (configurable).
- Execute manual sync cycles via `syncNow()`.
- Prevent overlapping sync cycles within the engine instance via a
  single-flight reservation assigned before the first asynchronous gap.
- Fetch eligible operations, push them through `RemoteSyncAdapter`, and
  persist outcomes through `LocalStorageAdapter`.
- Validate `SyncResult.operationId` against the pushed operation before
  applying any state transition.
- Emit conflict events **after** the durable `deadLetter` transition
  succeeds.
- Emit sync cycle reports for observability, including cycle-level errors.
- Stop accepting new sync requests, cancel connectivity subscriptions,
  and wait for the active cycle to finish on `dispose()`.

### `SyncEngineConfig`

Immutable configuration value object. Controls batch size, concurrency,
connectivity behavior, and offline sync policy. Has sensible defaults so
most applications need no configuration. Constructor validates that
`batchSize > 0` and `maxConcurrency > 0`.

### `RetryPolicy`

Abstract interface for computing retry delays and dead-letter decisions.
Keeps the engine decoupled from the specific backoff algorithm. The default
implementation is `ExponentialBackoffRetryPolicy`.

### `ExponentialBackoffRetryPolicy`

Concrete `RetryPolicy` implementation providing:

- Exponential backoff: `initialDelay * 2^(attemptCount - 1)`.
- Configurable `initialDelay`, `maxDelay`, `maxAttempts`, and `jitterFactor`.
- Jitter to prevent thundering-herd retries.
- Honors `SyncError.retryAfter` as a **minimum** delay that local backoff
  and jitter must not shorten.
- Dead-letters on non-retryable errors or when `maxAttempts` is reached.
- Owns its random source for jitter; `SyncEngine` does not accept a
  separate `Random` parameter.
- Validates constructor arguments: `initialDelay > 0`,
  `maxDelay >= initialDelay`, `maxAttempts > 0`,
  `jitterFactor` in `[0.0, 1.0]`.

### `SyncCycleReport`

Immutable value object summarizing a single sync cycle. Returned by
`syncNow()` and emitted on the `reports` stream. Contains counts of
fetched/succeeded/failed/conflicted/dead-lettered operations, cycle-level
error state, and timing information. `completedAt` is non-null for all
emitted reports.

### `SyncConflictEvent`

Immutable value object emitted on the `conflicts` stream **after** the
operation has been durably marked `deadLetter`. Contains the full
`SyncOperation` and the `SyncConflict` result (including `remoteSnapshot`)
so the application can attempt conflict resolution. The event is an
in-memory notification, not durable storage.

---

## 3. Proposed Public API

All types are `final class` or `abstract interface class` in Core. No Flutter
dependencies. No platform channels.

### `SyncEngine`

```dart
/// Push-only synchronization engine that orchestrates the durable outbox.
///
/// Depends only on Core contracts. Does not know about specific databases
/// or remote backends.
final class SyncEngine {
  /// Creates a [SyncEngine] with the given adapters and configuration.
  ///
  /// [localStorage] — durable outbox adapter (Sqflite, Drift, Hive, ...).
  /// [remoteSync] — remote push adapter (Supabase, REST, ...).
  /// [connectivity] — connectivity provider for auto-sync triggers.
  /// [config] — engine configuration (defaults provided, validated).
  /// [retryPolicy] — retry strategy (defaults to
  ///   [ExponentialBackoffRetryPolicy]).
  SyncEngine({
    required LocalStorageAdapter localStorage,
    required RemoteSyncAdapter remoteSync,
    required ConnectivityProvider connectivity,
    SyncEngineConfig config = const SyncEngineConfig(),
    RetryPolicy? retryPolicy,
  });

  /// Whether [start] has completed successfully and [dispose] has not
  /// been called.
  bool get isRunning;

  /// Stream of [SyncCycleReport]s emitted after each completed cycle.
  /// Reports are emitted only after the cycle has fully finished, including
  /// all storage transitions. Useful for observability, logging, and UI
  /// status indicators.
  Stream<SyncCycleReport> get reports;

  /// Stream of [SyncConflictEvent]s emitted after a conflict has been
  /// durably persisted as `deadLetter`. The event is an in-memory
  /// notification broadcast to current listeners; if no listener is
  /// attached when the event is emitted, the event is lost. The
  /// `remoteSnapshot` is not persisted separately and may be lost if the
  /// process crashes before the application handles the event.
  ///
  /// The application must resolve conflicts and call the appropriate
  /// [LocalStorageAdapter] method to finalize the operation.
  Stream<SyncConflictEvent> get conflicts;

  /// Starts the engine.
  ///
  /// 1. Recovers stale `inFlight` operations via
  ///    [LocalStorageAdapter.recoverInFlightOperations] **once**.
  /// 2. Subscribes to [ConnectivityProvider.onConnectivityChanged].
  /// 3. If [SyncEngineConfig.syncOnStart] is `true` and the device is
  ///    online, schedules an initial sync cycle (does not await it).
  ///
  /// Concurrent calls to [start] share one startup operation — the second
  /// caller awaits the same [Future].
  ///
  /// If recovery fails, [start] throws, `_started` remains `false`, and a
  /// subsequent [start] call will retry recovery. No sync cycle can begin
  /// before recovery completes successfully.
  ///
  /// Throws [StateError] if called after [dispose].
  Future<void> start();

  /// Manually triggers a sync cycle.
  ///
  /// Requires [start] to have completed successfully. Throws [StateError]
  /// if called before [start] or after [dispose].
  ///
  /// If a cycle is already in progress, returns the in-flight cycle's
  /// [SyncCycleReport] future (deduplication — no overlapping cycles).
  /// The cycle is reserved synchronously before the first asynchronous
  /// gap, so concurrent callers cannot both start separate cycles.
  ///
  /// If [SyncEngineConfig.syncWhenOffline] is `false` (default) and the
  /// device is offline, returns a report with `skippedOffline: true`
  /// without fetching operations.
  Future<SyncCycleReport> syncNow();

  /// Releases all resources.
  ///
  /// 1. Sets `_disposed = true` so no new sync cycles can start.
  /// 2. Cancels the connectivity subscription.
  /// 3. Waits for the active sync cycle to complete (if any).
  ///    Does **not** impose a timeout — remote calls are not cancellable,
  ///    so shutdown time is bounded only by the remote adapter's own
  ///    timeouts.
  /// 4. Closes internal stream controllers **after** the active cycle
  ///    has completed and no further events can be emitted.
  ///
  /// Does NOT close the [LocalStorageAdapter], [RemoteSyncAdapter], or
  /// [ConnectivityProvider] — the application owns their lifecycles.
  ///
  /// Operations currently `inFlight` remain `inFlight` and will be
  /// recovered to `pending` on the next [start] call.
  Future<void> dispose();
}
```

### `SyncEngineConfig`

```dart
/// Immutable configuration for [SyncEngine].
final class SyncEngineConfig {
  /// Creates a [SyncEngineConfig] with the given values.
  ///
  /// Throws [ArgumentError] if [batchSize] or [maxConcurrency] is not
  /// positive.
  SyncEngineConfig({
    this.batchSize = 50,
    this.maxConcurrency = 5,
    this.syncOnStart = true,
    this.syncOnConnectivityRestore = true,
    this.syncWhenOffline = false,
  }) {
    if (batchSize <= 0) {
      throw ArgumentError.value(batchSize, 'batchSize', 'Must be positive.');
    }
    if (maxConcurrency <= 0) {
      throw ArgumentError.value(
        maxConcurrency,
        'maxConcurrency',
        'Must be positive.',
      );
    }
  }

  /// Maximum number of operations fetched per batch.
  /// Default: 50. Must be > 0.
  final int batchSize;

  /// Maximum number of concurrent pushes within a batch.
  /// Default: 5. Must be > 0.
  final int maxConcurrency;

  /// Whether to trigger a sync cycle automatically on [SyncEngine.start]
  /// when the device is online.
  /// Default: true.
  final bool syncOnStart;

  /// Whether to trigger a sync cycle when connectivity transitions from
  /// offline to online.
  /// Default: true.
  final bool syncOnConnectivityRestore;

  /// Whether [SyncEngine.syncNow] should attempt a sync even when the
  /// device is offline. When `false` (default), offline manual syncs
  /// return a skipped report. When `true`, the engine attempts the push
  /// anyway — the remote adapter will likely return a network error,
  /// which is handled by the retry policy.
  /// Default: false.
  final bool syncWhenOffline;
}
```

### `RetryPolicy`

```dart
/// Strategy for computing retry delays and dead-letter decisions.
///
/// Implementations must be stateless and thread-safe (within a single
/// isolate). The engine calls [computeDelay] and [shouldDeadLetter]
/// for each failed operation.
abstract interface class RetryPolicy {
  /// Computes the delay before the next retry attempt.
  ///
  /// [attemptCount] is the number of completed push attempts (1 after the
  /// first failure, 2 after the second, etc.).
  ///
  /// [error] is the [SyncError] that caused the failure. If
  /// [SyncError.retryAfter] is non-null, implementations **must** honor it
  /// as a minimum delay — local backoff and jitter must not produce a
  /// delay shorter than `retryAfter`.
  Duration computeDelay(int attemptCount, SyncError error);

  /// Returns `true` if the operation should be moved to `deadLetter`
  /// instead of being scheduled for another retry.
  ///
  /// [attemptCount] is the number of completed push attempts.
  /// [error] is the [SyncError] that caused the failure.
  bool shouldDeadLetter(int attemptCount, SyncError error);
}
```

### `ExponentialBackoffRetryPolicy`

```dart
/// Default [RetryPolicy] using exponential backoff with jitter.
///
/// Delay formula:
///   1. base = min(initialDelay * 2^(attemptCount - 1), maxDelay)
///   2. jittered = base * (1 - jitterFactor + random() * 2 * jitterFactor)
///   3. if error.retryAfter != null:
///        return max(jittered, error.retryAfter)
///      else:
///        return jittered
///
/// `retryAfter` is a **minimum** — it is never shortened by local backoff
/// or jitter, but a longer local backoff is preserved.
///
/// Example with `initialDelay = 1s`, `maxDelay = 5m`, `jitterFactor = 0.2`:
///   attempt 1 → ~1s   (0.8s – 1.2s)
///   attempt 2 → ~2s   (1.6s – 2.4s)
///   attempt 3 → ~4s   (3.2s – 4.8s)
///   attempt 4 → ~8s   (6.4s – 9.6s)
///   ...
///   attempt 10 → ~5m  (4m – 6m, capped)
///
/// Numeric overflow: `2^(attemptCount - 1)` is computed using saturating
/// arithmetic — once `base` would exceed [maxDelay], the exponent is
/// capped and [maxDelay] is returned directly, avoiding integer overflow
/// for large attempt counts.
final class ExponentialBackoffRetryPolicy implements RetryPolicy {
  /// Creates an [ExponentialBackoffRetryPolicy].
  ///
  /// [initialDelay] — delay before the first retry. Default: 1 second.
  ///   Must be > 0.
  /// [maxDelay] — maximum delay between retries. Default: 5 minutes.
  ///   Must be >= [initialDelay].
  /// [maxAttempts] — maximum push attempts before dead-lettering.
  ///   Default: 10. Must be > 0.
  /// [jitterFactor] — fractional jitter applied to each delay.
  ///   `0.0` = no jitter, `0.2` = ±20%. Default: 0.2.
  ///   Must be in `[0.0, 1.0]`.
  /// [random] — injectable RNG for jitter. Defaults to [Random.secure].
  ///   Only this class owns a [Random]; [SyncEngine] does not accept one.
  ///
  /// Throws [ArgumentError] if any argument violates the constraints above.
  ExponentialBackoffRetryPolicy({
    Duration initialDelay = const Duration(seconds: 1),
    Duration maxDelay = const Duration(minutes: 5),
    this.maxAttempts = 10,
    this.jitterFactor = 0.2,
    Random? random,
  });

  /// Maximum number of push attempts before dead-lettering. Must be > 0.
  final int maxAttempts;

  /// Fractional jitter applied to computed delays. Must be in `[0.0, 1.0]`.
  final double jitterFactor;

  @override
  Duration computeDelay(int attemptCount, SyncError error);

  @override
  bool shouldDeadLetter(int attemptCount, SyncError error);
}
```

### `SyncCycleReport`

```dart
/// Immutable summary of a single sync cycle.
final class SyncCycleReport {
  const SyncCycleReport._({
    required this.startedAt,
    required this.completedAt,
    required this.fetched,
    required this.succeeded,
    required this.failed,
    required this.conflicted,
    required this.deadLettered,
    required this.cycleError,
    required this.skippedOffline,
  });

  /// When the cycle started.
  final DateTime startedAt;

  /// When the cycle completed. Non-null for all emitted reports — a report
  /// is only created after the cycle has fully finished.
  final DateTime completedAt;

  /// Number of operations fetched from the outbox.
  final int fetched;

  /// Number of operations successfully pushed and removed.
  final int succeeded;

  /// Number of operations that failed and were scheduled for retry.
  final int failed;

  /// Number of operations that encountered a [SyncConflict].
  final int conflicted;

  /// Number of operations moved to `deadLetter`.
  final int deadLettered;

  /// Cycle-level error that prevented the cycle from completing normally
  /// (e.g., `fetchEligibleOperations` threw, or a storage transition
  /// failed at the cycle level). Null if the cycle completed without
  /// cycle-level errors. Individual operation failures are counted in
  /// [failed] and [deadLettered], not here.
  final SyncError? cycleError;

  /// Whether the cycle was skipped because the device is offline.
  final bool skippedOffline;

  /// Duration of the sync cycle. Safe to call on any emitted report —
  /// [completedAt] is always non-null.
  Duration get duration => completedAt.difference(startedAt);

  /// Whether the cycle completed without errors.
  ///
  /// Returns `false` if:
  /// - [failed] > 0 (operations scheduled for retry)
  /// - [conflicted] > 0 (operations parked as deadLetter due to conflict)
  /// - [deadLettered] > 0 (operations exhausted retries or non-retryable)
  /// - [cycleError] != null (cycle-level failure)
  ///
  /// Returns `true` if:
  /// - [skippedOffline] is `true` (no work was attempted — this is a
  ///   skipped outcome, not a failure)
  /// - All operations succeeded and no cycle-level error occurred
  bool get isSuccess =>
      cycleError == null &&
      failed == 0 &&
      conflicted == 0 &&
      deadLettered == 0;

  @override
  String toString() =>
      'SyncCycleReport(fetched: $fetched, succeeded: $succeeded, '
      'failed: $failed, conflicted: $conflicted, '
      'deadLettered: $deadLettered, skippedOffline: $skippedOffline, '
      'cycleError: $cycleError, duration: $duration)';
}
```

### `SyncConflictEvent`

```dart
/// Emitted on [SyncEngine.conflicts] after a conflict has been durably
/// persisted as `deadLetter`.
///
/// The event is emitted only after [LocalStorageAdapter.markDeadLetter]
/// has succeeded. If the storage transition fails, no event is emitted
/// and the operation remains in its previous state.
///
/// Delivery semantics: the event is added to a broadcast stream. If no
/// listener is attached at the time of emission, the event is lost. The
/// `remoteSnapshot` is not persisted separately — if the process crashes
/// before the application handles the event, the snapshot may be lost.
///
/// The application must resolve conflicts and call the appropriate
/// [LocalStorageAdapter] method. The Core contract does not provide a
/// dedicated conflict-resolution or discard operation.
final class SyncConflictEvent {
  const SyncConflictEvent({
    required this.operation,
    required this.conflict,
  });

  /// The operation that encountered the conflict. Its status is now
  /// `deadLetter` in the outbox.
  final SyncOperation operation;

  /// The conflict result from the remote adapter, including
  /// [SyncConflict.remoteSnapshot].
  final SyncConflict conflict;
}
```

### Public API Summary

| Type | Purpose | Constructor params |
|------|---------|-------------------|
| `SyncEngine` | Orchestrator | `localStorage`, `remoteSync`, `connectivity`, `config?`, `retryPolicy?` |
| `SyncEngineConfig` | Configuration | `batchSize`, `maxConcurrency`, `syncOnStart`, `syncOnConnectivityRestore`, `syncWhenOffline` |
| `RetryPolicy` | Interface | — |
| `ExponentialBackoffRetryPolicy` | Default retry | `initialDelay`, `maxDelay`, `maxAttempts`, `jitterFactor`, `random?` |
| `SyncCycleReport` | Cycle result | internal |
| `SyncConflictEvent` | Conflict notification | internal |

**No Core API changes required.** All types use existing contracts.

---

## 4. Sync Cycle Sequence

### Startup (`start()`)

```
start()
  ├─ if _disposed → throw StateError
  ├─ if _startupFuture != null → return _startupFuture  (share one startup)
  ├─ _startupFuture = _performStartup()
  ├─ return _startupFuture
  │
  └─ _performStartup():
       ├─ await localStorage.recoverInFlightOperations()  ← once
       │   └─ if throws → _startupFuture = null, rethrow
       ├─ _connectivitySub = connectivity.onConnectivityChanged.listen(...)
       ├─ _isConnected = await connectivity.isConnected
       ├─ if (config.syncOnStart && _isConnected) {
       │     unawaited(_triggerAutoSync())  ← scheduled, not awaited
       │   }
       ├─ _started = true
       └─ _startupFuture = null
```

**Failed startup:** If `recoverInFlightOperations` throws, `_startupFuture`
is cleared, `_started` remains `false`, and the exception propagates to
the caller. A subsequent `start()` call will create a new `_startupFuture`
and retry recovery. No sync cycle can begin before recovery completes
because `_started` is only set to `true` after recovery succeeds, and
`syncNow()` checks `_started`.

### Sync cycle (`syncNow()` or auto-trigger)

```
syncNow()
  ├─ if _disposed → throw StateError
  ├─ if !_started → throw StateError("SyncEngine.start() must complete first")
  ├─ if _activeCycle != null → return _activeCycle  (share active cycle)
  ├─ _cycleReservation = Completer<SyncCycleReport>()  ← reserve NOW
  ├─ _activeCycle = _runCycle(_cycleReservation)
  ├─ return _activeCycle
  │
  └─ _runCycle():
       ├─ if (!config.syncWhenOffline && !_isConnected) {
       │     report = SyncCycleReport(skippedOffline: true, ...)
       │     _cycleReservation.complete(report)
       │     return report
       │   }
       ├─ report = SyncCycleReport(startedAt: now, ...)
       ├─ try {
       │     loop:
       │       a. fetchEligibleOperations(now, batchSize,
       │          preservePerEntityOrder: true)
       │          └─ if throws → report.cycleError = SyncError(...), break
       │       b. if empty → break
       │       c. for each chunk of maxConcurrency:
       │          i.   markInFlight(op.operationId)
       │               └─ if throws → log, skip this op, continue
       │          ii.  result = await remoteSync.push(op)
       │               └─ if throws → treat as SyncFailure(unknown)
       │          iii. validate result.operationId == op.operationId
       │               └─ if mismatch → log, mark op as failed (protocol
       │                  violation), continue
       │          iv.  handle result:
       │               SyncSuccess → markSucceeded
       │               SyncFailure → markFailed/markDeadLetter
       │               SyncConflict → markDeadLetter THEN emit event
       │          v.   update report counters
       │       d. if fetched < batchSize → break
       │     } catch (e) {
       │       report.cycleError = SyncError(unknown, ...)
       │     }
       │     report.completedAt = now
       │     reports.add(report)
       │     _cycleReservation.complete(report)
       │     return report
       └─ finally {
            _activeCycle = null
            _cycleReservation = null
          }
```

**Single-flight reservation:** `_activeCycle` is assigned synchronously
(immediately after the null check, before any `await`). This means two
concurrent callers cannot both pass the check — the first caller assigns
`_activeCycle` before yielding to the event loop, so the second caller
sees the non-null value and returns the same future. No async gap exists
between the check and the assignment.

**Manual and connectivity-triggered sharing:** Both `syncNow()` and
`_triggerAutoSync()` call the same internal `_requestSyncCycle()` method
which performs the single-flight check and reservation. They share the
same `_activeCycle` future.

### Key properties

- **Recovery once:** `recoverInFlightOperations()` runs only during
  `start()`, not during sync cycles. This prevents resetting `inFlight`
  operations belonging to another active engine instance during manual
  syncs.
- **Single-flight:** `_activeCycle` is reserved synchronously before the
  first `await`, eliminating the race condition.
- **Batch loop:** Continues fetching until `fetchEligibleOperations`
  returns fewer than `batchSize` or empty. This drains the outbox in one
  cycle when possible.
- **Chunked concurrency:** Operations within a chunk are pushed
  concurrently. Chunks are processed sequentially.
- **Result validation:** Every `SyncResult` is checked against the pushed
  operation's `operationId` before any state transition.
- **Conflict ordering:** `markDeadLetter` succeeds **before** the conflict
  event is emitted. If storage fails, no event is emitted.

---

## 5. Concurrency and FIFO Strategy

### How batch fetching and concurrency interact

`fetchEligibleOperations(preservePerEntityOrder: true)` is documented in
the Core contract as returning operations where "an operation is not
eligible if any earlier operation for the same `(entityType, entityId)`
has a status other than `succeeded`." The contract does **not** explicitly
document that at most one operation per entity is returned, but the
adapter implementations (Sqflite, Drift, Hive) enforce this by selecting
the earliest eligible operation per entity group.

**Engine assumption:** The engine relies on this behavior but does not
claim it as a guaranteed contract property. If a future adapter returns
multiple operations for the same entity, the engine will process them in
fetch order (which is `createdAt` then `sequenceNumber` ascending), which
preserves FIFO within the batch. Cross-batch FIFO is preserved because
the first batch's operations are marked `inFlight` (a blocking status)
before the second batch is fetched.

```
Outbox state:
  Entity A: op-A1 (seq 1, pending), op-A2 (seq 2, pending), op-A3 (seq 3, pending)
  Entity B: op-B1 (seq 4, pending), op-B2 (seq 5, pending)
  Entity C: op-C1 (seq 6, pending)

fetchEligibleOperations(batchSize: 10, preservePerEntityOrder: true)
  → returns [op-A1, op-B1, op-C1]  (earliest per entity, per adapter impl)

Process chunk [op-A1, op-B1, op-C1] concurrently:
  op-A1 → inFlight → push → succeeded
  op-B1 → inFlight → push → failed (retry in 30s)
  op-C1 → inFlight → push → succeeded

Next fetchEligibleOperations:
  → op-A2 now eligible (op-A1 succeeded)
  → op-B2 NOT eligible (op-B1 is failed, blocks entity B)
  → op-C2 eligible if exists (op-C1 succeeded)
  → returns [op-A2, op-C2, ...]
```

### FIFO guarantees and limitations

| Aspect | Behavior | Source |
|--------|----------|--------|
| At most one eligible op per entity | Enforced by adapter implementations; contract documents the blocking rule but not the "one per entity" limit explicitly | Adapter implementations |
| Ordering within batch | `createdAt` ascending, then `sequenceNumber` ascending (null sequenceNumbers sort last) | Contract + adapter implementations |
| Ordering when timestamps equal | `sequenceNumber` ascending as tiebreaker; if both null, order is adapter-defined (typically insertion order) | Contract does not specify; adapter-dependent |
| `sequenceNumber` null | Operation is eligible but sorts after operations with non-null sequenceNumbers for the same entity | Contract does not explicitly document; adapter-dependent |
| `failed` blocks entity | Yes — `preservePerEntityOrder` checks for any non-`succeeded` earlier operation | Contract explicitly documents |
| `deadLetter` blocks entity | Yes — same rule; contract explicitly notes this blocks until explicit resolution | Contract explicitly documents |
| `inFlight` blocks entity | Yes — same rule; prevents concurrent pushes for same entity | Contract explicitly documents |
| Unrelated entities proceed | Yes — blocking is per `(entityType, entityId)`, not global | Contract explicitly documents |

### Is `fetchEligibleOperations` sufficient for FIFO under concurrency?

**Within a single engine instance: yes, with documented assumptions.**

The sequence is:
1. `fetchEligibleOperations` returns eligible operations.
2. Engine calls `markInFlight` on each before pushing.
3. `markInFlight` changes status to `inFlight`, which is a blocking
   status for FIFO purposes (per the contract).
4. Even if the push fails, the operation remains `inFlight` or `failed`,
   still blocking later ops for the same entity.

**The gap between fetch and markInFlight:** Another writer could
modify the operation in that window. Within one engine instance, the
single-flight cycle mutex prevents concurrent cycles. The adapter-level
mutex (Hive) or database transaction isolation (Sqflite/Drift) provides
additional safety. However, the engine does not claim atomic claiming —
this is a documented limitation.

### Cross-instance limitation

**Multiple `SyncEngine` instances sharing the same outbox can race.**

Scenario:
```
Engine-1: fetchEligibleOperations → [op-A1, op-B1]
Engine-2: fetchEligibleOperations → [op-A1, op-B1]  (same ops!)
Engine-1: markInFlight(op-A1) → succeeds
Engine-2: markInFlight(op-A1) → also succeeds (idempotent, no check)
Engine-1: push(op-A1) → remote applies
Engine-2: push(op-A1) → remote applies AGAIN?
```

The remote adapter's `idempotencyKey` prevents duplicate application on
the backend, but:
- Both engines push the same operation (wasted bandwidth).
- `attemptCount` may be incremented twice for the same logical attempt.
- Results may be inconsistent (one succeeds, one fails).

**Recommendation:** Run one `SyncEngine` per outbox. If multiple engines
are needed (e.g., different remote targets), use separate outboxes or
accept the duplicate-push risk mitigated by idempotency keys.

The current `LocalStorageAdapter` contract does not provide atomic
"claim" semantics (e.g., `UPDATE ... WHERE status='pending'` returning
affected rows). Adding this would require a Core API change, which is
out of scope for Phase 3.

### Concurrency model summary

| Aspect | Behavior |
|--------|----------|
| Batch size | `config.batchSize` ops per fetch |
| Concurrency | `config.maxConcurrency` parallel pushes per chunk |
| FIFO | Per-entity via `preservePerEntityOrder: true` |
| Cross-entity | Full parallelism |
| Overlapping cycles | Prevented by synchronous `_activeCycle` reservation |
| Cross-instance | Not protected — rely on idempotency keys |

---

## 6. Retry Algorithm and Examples

### `ExponentialBackoffRetryPolicy` algorithm

```
computeDelay(attemptCount, error):
  1. base = min(initialDelay * 2^(attemptCount - 1), maxDelay)
     └─ uses saturating exponentiation: if 2^(attemptCount-1) would
        overflow, base = maxDelay directly
  2. jitterRange = jitterFactor
     jitter = 1 - jitterRange + random() * 2 * jitterRange
     jittered = base * jitter
  3. if error.retryAfter != null:
        return max(jittered, error.retryAfter)  ← retryAfter is minimum
     else:
        return jittered

shouldDeadLetter(attemptCount, error):
  1. If !error.isRetryable → return true
  2. If attemptCount >= maxAttempts → return true
  3. Return false
```

### `retryAfter` semantics

`SyncError.retryAfter` is treated as a **minimum delay**:

- If `retryAfter` is **longer** than the computed local backoff → use
  `retryAfter`.
- If `retryAfter` is **shorter** than the computed local backoff → use
  the local backoff (do not shorten).
- Jitter is applied to the local backoff **before** comparing with
  `retryAfter`, so jitter cannot reduce the delay below `retryAfter`.

Edge cases:
- `retryAfter = Duration.zero` → no effect; local backoff applies.
- `retryAfter` very large (e.g., 1 hour) → delay is 1 hour regardless of
  local backoff.
- `retryAfter` combined with `maxDelay` → `retryAfter` takes precedence
  over `maxDelay` (server knows best).

### Numeric overflow prevention

`2^(attemptCount - 1)` can overflow for large `attemptCount`. The
implementation uses saturating exponentiation:

```
if (attemptCount - 1 >= 63) → base = maxDelay  (2^63 overflows int64)
else → base = min(initialDelay * (1 << (attemptCount - 1)), maxDelay)
```

Since `maxDelay` is always finite and `initialDelay * 2^n` is capped at
`maxDelay` well before overflow, the shift amount is bounded by
`log2(maxDelay / initialDelay)`, which is small for practical configs.

### Example: network failure with default config

```
Config: initialDelay=1s, maxDelay=5m, maxAttempts=10, jitterFactor=0.2

Push attempt 1 → SyncFailure(network, isRetryable=true)
  → markFailed(attemptCount: 1, nextRetryAt: now + ~1s)

[1s later] Push attempt 2 → SyncFailure(network, isRetryable=true)
  → markFailed(attemptCount: 2, nextRetryAt: now + ~2s)

[2s later] Push attempt 3 → SyncFailure(network, isRetryable=true)
  → markFailed(attemptCount: 3, nextRetryAt: now + ~4s)

... continues with exponential backoff ...

Push attempt 10 → SyncFailure(network, isRetryable=true)
  → shouldDeadLetter(10, error) → true (attemptCount >= maxAttempts)
  → markDeadLetter(attemptCount: 10, error: error)
```

### Example: non-retryable validation error

```
Push attempt 1 → SyncFailure(validation, isRetryable=false)
  → shouldDeadLetter(1, error) → true (!isRetryable)
  → markDeadLetter(attemptCount: 1, error: error)
```

Immediately dead-letters — no retry scheduled.

### Example: server-suggested retry delay

```
Push attempt 1 → SyncFailure(rateLimited, isRetryable=true, retryAfter=30s)
  → computeDelay: local backoff = ~1s, jittered = ~1s
                    max(~1s, 30s) = 30s
  → markFailed(attemptCount: 1, nextRetryAt: now + 30s)

Push attempt 5 → SyncFailure(rateLimited, isRetryable=true, retryAfter=5s)
  → computeDelay: local backoff = ~16s, jittered = ~16s
                    max(~16s, 5s) = ~16s  (local backoff wins)
  → markFailed(attemptCount: 5, nextRetryAt: now + ~16s)
```

### Example: conflict

```
Push attempt 1 → SyncConflict(remoteSnapshot: {...})
  → markDeadLetter(attemptCount: 1, error: SyncError(conflict, ...))
     └─ MUST succeed before event is emitted
  → Emit SyncConflictEvent on conflicts stream (after markDeadLetter)
  → Operation parked — blocks later ops for same entity
  → Application resolves via conflicts stream
```

### `attemptCount` semantics

`SyncOperation.attemptCount` starts at `0`. The engine increments it on
each failure:

- `markFailed(attemptCount: op.attemptCount + 1, ...)`
- `markDeadLetter(attemptCount: op.attemptCount + 1, ...)`

This means `attemptCount` represents the number of **completed** push
attempts, not the current attempt number. After the first failure,
`attemptCount = 1`.

---

## 7. Conflict Handling Decision

### The problem

`RemoteSyncAdapter.push` can return `SyncConflict` containing a
`remoteSnapshot`. The `LocalStorageAdapter` contract has no method to
persist conflict state or the remote snapshot. The engine must handle
this without:

- Silently treating conflict as success (data loss).
- Retrying forever (infinite loop).
- Losing the `remoteSnapshot` (needed for resolution).

### Decision: Park + Notify (ordered)

The engine, upon receiving `SyncConflict`:

1. Calls `markDeadLetter` with a `SyncError(code: SyncErrorCode.conflict)`.
   **This storage transition must succeed before proceeding.**
2. **After** `markDeadLetter` succeeds, emits a `SyncConflictEvent` on the
   `conflicts` stream containing the full `SyncOperation` and
   `SyncConflict` (including `remoteSnapshot`).
3. If `markDeadLetter` throws, the operation remains in its previous
   state (`inFlight`), no event is emitted, and the error is logged. The
   operation will be recovered to `pending` on the next `start()` and
   retried — this is safe because the remote adapter's idempotency key
   prevents duplicate application.

### Why `deadLetter` and not `failed`?

- `deadLetter` blocks later operations for the same entity — correct,
  you don't want to push B2 before resolving B1's conflict.
- `failed` with `nextRetryAt` would eventually retry — wrong, conflicts
  need manual resolution.
- `deadLetter` is visible in the outbox for inspection and manual
  recovery.

### Conflict stream delivery semantics

- The `conflicts` stream is a **broadcast stream**.
- If no listener is attached when an event is emitted, the event is
  **lost**. The `remoteSnapshot` is not persisted separately.
- If the process crashes after `markDeadLetter` succeeds but before the
  application handles the event, the `remoteSnapshot` is **lost**. The
  operation remains `deadLetter` in the outbox, but the conflict details
  are gone. The application must re-fetch from remote or treat the
  dead-lettered operation as needing manual review.
- **Conflict recovery is NOT guaranteed.** The engine provides best-effort
  notification; durable conflict resolution requires application-level
  handling or a future Core API addition.

### Application resolution flow — semantic mismatch warning

The Core contract has no dedicated conflict-resolution or discard
operation. The available methods are:

- `markSucceeded(operationId)` — removes the operation from the outbox.
  Semantically means "the remote accepted this operation." Using it to
  discard a conflicted operation is a **semantic mismatch** — the remote
  did not accept the local change.
- `markDeadLetter(operationId, ...)` — already called by the engine.
  There is no way to "un-dead-letter" or transition to a resolved state.

**Known limitation:** The application cannot cleanly resolve a conflict
using existing Core methods. Options:

1. **Accept remote state:** Call `markSucceeded` to remove the
   dead-lettered operation. This is semantically incorrect (remote did
   not accept local change) but practically removes the blocker. The
   application must separately apply `remoteSnapshot` to local domain
   state.
2. **Discard local change:** The operation is already `deadLetter`.
   The application must manually clean up local domain data if needed.
   The dead-lettered operation remains in the outbox indefinitely.
3. **Merge and re-enqueue:** Call `markSucceeded` (semantically incorrect)
   then `runInTransaction` to enqueue a new `LocalMutation` with merged
   data. **This is NOT atomic** — if the app crashes between
   `markSucceeded` and `enqueue`, the original operation is lost and the
   merged mutation is never created. This workflow must not be advertised
   as guaranteed.

### Smallest future API change

The smallest justified Core addition would be:

```dart
/// Marks the operation as resolved after conflict handling.
/// Removes it from the active outbox without implying remote acceptance.
Future<void> markResolved(String operationId);
```

Or alternatively, a dedicated conflict state:

```dart
/// Marks the operation as conflicted, persisting the remote snapshot.
Future<void> markConflict(
  String operationId, {
  required Map<String, dynamic> remoteSnapshot,
  String? remoteVersion,
  DateTime? serverTimestamp,
});
```

Both are breaking changes to `LocalStorageAdapter` and require adapter
updates. They are documented as future enhancements, not Phase 3 scope.

---

## 8. Error Handling Matrix

| Scenario | Engine Behavior | Operation State After |
|----------|----------------|----------------------|
| `SyncSuccess` | Validate `operationId`, then `markSucceeded` | Removed from outbox |
| `SyncFailure` (retryable, attempts left) | Validate `operationId`, then `markFailed` with computed `nextRetryAt` | `failed`, eligible at `nextRetryAt` |
| `SyncFailure` (non-retryable or max attempts) | Validate `operationId`, then `markDeadLetter` | `deadLetter`, blocks entity |
| `SyncConflict` | Validate `operationId`, `markDeadLetter`, then emit event | `deadLetter`, blocks entity |
| `SyncResult.operationId` mismatch | Log protocol violation, `markFailed` with `SyncErrorCode.unknown`, `isRetryable: false` | `failed` → `deadLetter` on next cycle (non-retryable) |
| `RemoteSyncAdapter.push` throws | Treat as `SyncFailure(unknown, isRetryable: true)` | `failed` or `deadLetter` per retry policy |
| `markInFlight` throws | Log warning, skip operation this cycle | Unchanged (still `pending`/`failed`) |
| `markSucceeded` throws | Log warning, continue cycle | `inFlight` — recovered on next `start()` |
| `markFailed` throws | Log warning, continue cycle | `inFlight` — recovered on next `start()` |
| `markDeadLetter` throws | Log warning, continue cycle, no conflict event emitted | `inFlight` — recovered on next `start()` |
| `fetchEligibleOperations` throws | Set `report.cycleError`, abort cycle | Unchanged |
| `recoverInFlightOperations` throws (in `start`) | `start` throws, `_started` remains `false` | `inFlight` ops remain; retry on next `start` |
| Connectivity lost mid-push | Push fails with network error → retry policy | `failed` with backoff |
| App terminated mid-push | OS kills process | `inFlight` → recovered on next `start()` |
| `dispose()` during cycle | Wait for cycle to complete (no timeout), then close streams | `inFlight` ops recovered on next `start()` |
| `syncNow()` before `start()` | Throws `StateError` | — |
| `syncNow()` after `dispose()` | Throws `StateError` | — |
| `start()` after `dispose()` | Throws `StateError` | — |
| `start()` concurrent calls | Share one `_startupFuture` | — |
| `syncNow()` concurrent calls | Share one `_activeCycle` future | — |

### Key invariants

- **No operation disappears** due to an unhandled exception. Every code
  path either marks the operation or leaves it in a recoverable state.
- **`inFlight` is recoverable on `start()` only.** Recovery does not run
  during sync cycles, preventing interference with other engine instances.
- **Result validation prevents cross-operation contamination.** A
  mismatched `SyncResult` never causes the wrong operation to be marked
  succeeded.
- **Conflict events are emitted only after durable persistence.** If
  `markDeadLetter` fails, no event is emitted.
- **Storage failures are non-fatal.** The engine logs and continues —
  a failed `markSucceeded` means the operation stays `inFlight` and
  will be retried on next `start()` (idempotency key prevents duplicate
  remote application).
- **Unexpected exceptions are retryable.** Conservative default — better
  to retry than to lose an operation.

---

## 9. Lifecycle and Disposal Behavior

### `start()`

```
start()
  ├─ if _disposed → throw StateError
  ├─ if _startupFuture != null → return _startupFuture
  ├─ _startupFuture = _performStartup()
  └─ return _startupFuture

_performStartup():
  ├─ await localStorage.recoverInFlightOperations()  ← once, before _started
  │   └─ if throws → _startupFuture = null, rethrow
  ├─ _connectivitySub = connectivity.onConnectivityChanged.listen((online) {
  │     _isConnected = online
  │     if (online && !_wasConnected && config.syncOnConnectivityRestore) {
  │       _wasConnected = true
  │       unawaited(_triggerAutoSync())
  │     } else if (!online) {
  │       _wasConnected = false
  │     }
  │   }, onError: (e) { log; })
  ├─ _isConnected = await connectivity.isConnected  ← sample initial state
  ├─ _wasConnected = _isConnected
  ├─ if (config.syncOnStart && _isConnected) {
  │     unawaited(_triggerAutoSync())  ← scheduled, not awaited
  │   }
  ├─ _started = true
  ├─ _startupFuture = null
  └─ return
```

**`start()` does not await the initial auto-sync.** It schedules it via
`unawaited(_triggerAutoSync())` and returns immediately after setting
`_started = true`. The caller can `await engine.start()` and then
`await engine.syncNow()` if they need to wait for the first cycle.

### `syncNow()`

```
syncNow()
  ├─ if _disposed → throw StateError
  ├─ if !_started → throw StateError("SyncEngine.start() must complete first")
  ├─ if _activeCycle != null → return _activeCycle
  ├─ _activeCycle = _requestSyncCycle()  ← synchronous assignment
  └─ return _activeCycle

_requestSyncCycle():
  ├─ report = SyncCycleReport(startedAt: now, ...)
  ├─ if (!config.syncWhenOffline && !_isConnected) {
  │     report.skippedOffline = true
  │     report.completedAt = now
  │     reports.add(report)
  │     return report
  │   }
  ├─ _runCycle(report)  ← async, updates report in place
  └─ return report
```

**Single-flight:** `_activeCycle` is assigned synchronously in `syncNow()`
before any `await`. The `_requestSyncCycle()` method is called
synchronously and returns a `Future` that is immediately assigned to
`_activeCycle`. Two concurrent callers cannot both pass the null check
because Dart's single-threaded event loop ensures the assignment happens
before the second caller's continuation runs.

### `_triggerAutoSync()`

```
_triggerAutoSync()
  ├─ if _disposed || !_started → return
  ├─ try {
  │     await syncNow()
  │   } catch (e) {
  │     log warning  ← prevent unhandled async error
  │   }
```

Auto-triggers call `syncNow()` which handles deduplication. Errors are
caught and logged to prevent unhandled asynchronous errors.

### `dispose()`

```
dispose()
  ├─ if _disposed → return (idempotent)
  ├─ _disposed = true
  ├─ await _connectivitySub?.cancel()
  ├─ if (_activeCycle != null) {
  │     await _activeCycle  ← wait for cycle to finish, NO timeout
  │   }
  ├─ await _reportController.close()
  ├─ await _conflictController.close()
  └─ return
```

**No timeout on dispose:** The `RemoteSyncAdapter` contract has no
cancellation support. Imposing a timeout would risk closing stream
controllers while the active cycle can still emit events (reports,
conflicts). Instead, `dispose()` waits indefinitely for the active cycle
to complete. Shutdown time is bounded only by the remote adapter's own
timeouts and the storage adapter's responsiveness.

### Disposal during an active cycle

- `_disposed` is set to `true` immediately, preventing new `syncNow()`
  calls from starting new cycles.
- The active cycle continues to completion — in-flight pushes finish,
  storage transitions complete, and the report is emitted.
- After the cycle completes, `_activeCycle` is set to `null`.
- Stream controllers are closed **after** `_activeCycle` completes,
  ensuring no events are emitted after close.
- Operations currently `inFlight` remain `inFlight` and are recovered on
  the next `start()` call.

### Connectivity behavior

| Event | Action |
|-------|--------|
| `start()` + online + `syncOnStart` | Schedule `syncNow()` via `_triggerAutoSync()` (not awaited) |
| `start()` + offline | No sync; wait for connectivity transition |
| Connectivity `false → true` + `syncOnConnectivityRestore` | Schedule `syncNow()` via `_triggerAutoSync()` |
| Connectivity `true → true` (duplicate) | Ignored — `_wasConnected` flag prevents re-trigger |
| Connectivity `true → false` | Update `_isConnected = false`; let current cycle finish |
| Manual `syncNow()` + offline + `!syncWhenOffline` | Return skipped report |
| Manual `syncNow()` + offline + `syncWhenOffline` | Attempt push anyway |

### Initial connectivity sampling

1. `start()` subscribes to `onConnectivityChanged` **before** sampling
   `isConnected`. This ensures no transition is missed between sampling
   and subscription.
2. `isConnected` is awaited to get the current state.
3. `_wasConnected` is initialized to the sampled value, so the first
   `true` event from the stream (if any) is treated as a transition only
   if the previous state was `false`.

### Avoiding infinite retry loops

- **Connectivity triggers** are edge-triggered (only on `false → true`
  transition). The `_wasConnected` flag deduplicates repeated `true`
  events.
- **Retry delays** ensure failed operations aren't retried immediately.
  `nextRetryAt` gates eligibility.
- **Dead-letter** stops automatic retries for non-retryable errors and
  max-attempt exhaustion.
- **Cycle mutex** prevents overlapping cycles from stacking up.
- **Auto-trigger errors** are caught and logged, preventing unhandled
  async errors from crashing the app.

---

## 10. Identified Limitations and Trade-offs

### 1. No atomic claiming across engine instances

`fetchEligibleOperations` + `markInFlight` is not atomic. Two engine
instances can fetch and push the same operations. Mitigated by:
- Remote adapter idempotency keys (prevents duplicate application).
- Recommendation: one engine per outbox.

**Future enhancement:** Add `claimOperations` or atomic status transition
to `LocalStorageAdapter` contract.

### 2. Conflict `remoteSnapshot` not persisted

The `remoteSnapshot` from `SyncConflict` is delivered via broadcast
stream but not stored durably. If the app crashes before resolving, the
snapshot is lost. Conflict recovery is **not guaranteed**.

**Mitigation:** App should handle conflicts promptly or re-fetch from
remote. Documented as acceptable for Phase 3.

**Future enhancement:** Add `markConflict` to `LocalStorageAdapter` to
persist conflict state and remote snapshot.

### 3. No dedicated conflict-resolution or discard API

The Core contract has no `markResolved` or `markConflict` method.
`markSucceeded` is semantically incorrect for conflict resolution.
Merge-and-re-enqueue is not atomic.

**Future enhancement:** Add `markResolved` or `markConflict` to
`LocalStorageAdapter`.

### 4. No pull synchronization

Engine is push-only. Remote changes are not fetched. This is by design —
pull requires a different contract (`RemoteSyncAdapter.pull` or similar)
and conflict resolution strategy.

### 5. No background scheduling

Engine runs only when the app is in the foreground (or when manually
triggered). No WorkManager/BGTaskScheduler integration. Documented in
architecture.md as a platform-level concern.

### 6. Chunked concurrency, not sliding window

Operations are processed in chunks of `maxConcurrency`. A slow operation
in a chunk delays the next chunk even if other slots are free. A sliding
window (start next op as soon as one completes) would improve throughput
but adds complexity.

**Trade-off:** Simplicity and predictability over maximum throughput.
Most sync workloads don't need sliding-window optimization.

### 7. `watchPendingCount` not used for auto-sync

Engine does not listen to `watchPendingCount` to trigger syncs on new
mutations. Syncs are triggered by connectivity changes, manual calls, or
`syncOnStart`. Auto-sync on enqueue could be added in Phase 3.2+.

**Trade-off:** Avoids complexity and potential for sync storms. Apps
that need immediate sync can call `syncNow()` after enqueueing.

### 8. Retry policy is per-operation, not global

Each operation's retry delay is computed independently. There's no
global rate limiting or circuit breaker. If many operations fail
simultaneously, they all retry around the same time (mitigated by
jitter).

**Future enhancement:** Add a global rate limiter or token bucket.

### 9. Dispose has no timeout

`dispose()` waits indefinitely for the active cycle to complete. If the
remote adapter hangs, `dispose()` hangs. This is a safe first-version
policy — no cancellation support exists in the contracts.

**Future enhancement:** Add cancellation support to `RemoteSyncAdapter`
or a configurable dispose timeout with documented risk.

### 10. FIFO ordering when timestamps are equal

The contract orders by `createdAt` then `sequenceNumber`. When both are
equal (or both null), ordering is adapter-defined. The engine does not
guarantee deterministic FIFO in this edge case.

**Mitigation:** Adapters should use monotonically increasing
`sequenceNumber` for deterministic ordering. The engine relies on
adapter behavior for this edge case.

---

## 11. Required Core API Changes

**None for Phase 3.0.**

The existing contracts are sufficient:

- `LocalStorageAdapter` provides all needed methods for push-only sync.
- `RemoteSyncAdapter.push` returns `SyncResult` (sealed: Success/Conflict/Failure).
- `ConnectivityProvider` provides `isConnected` and `onConnectivityChanged`.
- `SyncError` has `isRetryable`, `retryAfter`, `code` for retry decisions.
- `SyncConflict` has `remoteSnapshot` for conflict resolution.
- `SyncResult` has `operationId` for result validation.

### Potential future additions (not Phase 3)

| Addition | Purpose | Compatibility |
|----------|---------|---------------|
| `LocalStorageAdapter.markConflict` | Persist conflict state + remote snapshot durably | Breaking — new method on interface |
| `LocalStorageAdapter.markResolved` | Clean conflict resolution without semantic mismatch | Breaking — new method on interface |
| `LocalStorageAdapter.claimOperations` | Atomic claiming for multi-instance | Breaking — new method on interface |
| `RemoteSyncAdapter.pull` | Pull synchronization | Breaking — new method on interface |
| `RemoteSyncAdapter.push` with `CancelToken` | Cancellation support for dispose | Breaking — signature change |
| `SyncEngineConfig.rateLimit` | Global rate limiting | Non-breaking — new config field |

These are documented for future phases but not required now.

---

## 12. Proposed Implementation Breakdown

### Phase 3.1 — Core Engine Skeleton

**Files to create:**
- `lib/src/engine/sync_engine.dart` — main engine class
- `lib/src/engine/sync_engine_config.dart` — configuration with validation
- `lib/src/engine/sync_cycle_report.dart` — report value object
- `lib/src/engine/sync_conflict_event.dart` — conflict event
- `lib/src/engine/retry_policy.dart` — interface
- `lib/src/engine/exponential_backoff_retry_policy.dart` — default impl
- Update `lib/local_remote_synchronization.dart` — export new types

**Tests:**
- Engine lifecycle (start, syncNow, dispose)
- `start()` retry after recovery failure
- Concurrent `start()` calls share one startup
- `syncNow()` before `start()` throws
- Single-flight cycle reservation (concurrent `syncNow` calls)
- Basic sync cycle with mock adapters
- Connectivity-triggered sync
- Result `operationId` validation
- Configuration validation (batchSize, maxConcurrency, retry policy args)

### Phase 3.2 — Retry and Failure Handling

**Files to modify:**
- `lib/src/engine/sync_engine.dart` — add retry logic
- `lib/src/engine/exponential_backoff_retry_policy.dart` — refine algorithm

**Tests:**
- Exponential backoff timing
- Jitter distribution
- `retryAfter` as minimum (not shortened by backoff/jitter)
- Max attempts → dead-letter
- Non-retryable → immediate dead-letter
- Numeric overflow prevention for large attempt counts
- All error handling matrix scenarios

### Phase 3.3 — Conflict Handling

**Files to modify:**
- `lib/src/engine/sync_engine.dart` — add conflict detection, ordered
  persistence, and event emission
- `lib/src/engine/sync_conflict_event.dart` — finalize

**Tests:**
- Conflict event emitted only after `markDeadLetter` succeeds
- No event emitted if `markDeadLetter` throws
- Conflict stream delivery semantics (broadcast, no listener = lost)
- Multiple conflicts in one cycle
- Operation remains `deadLetter` after conflict

### Phase 3.4 — Concurrency and FIFO Verification

**Files to modify:**
- `lib/src/engine/sync_engine.dart` — verify batch/concurrency logic

**Tests:**
- Per-entity FIFO preservation under concurrency
- Cross-entity parallelism
- Batch size limits
- Max concurrency limits
- Failed operation blocks entity but not others
- Dead-letter blocks entity permanently
- Result `operationId` mismatch handling

### Phase 3.5 — Integration Tests

**Files to create:**
- `test/engine/sync_engine_integration_test.dart` — end-to-end with real adapters

**Tests:**
- Full sync cycle with Sqflite adapter + mock remote
- Crash recovery (inFlight → pending → retry on next start)
- Connectivity loss during push
- Disposal during active cycle (no timeout, waits for completion)
- Multiple sequential cycles
- `start()` failure and retry

### Phase 3.6 — Documentation and Examples

**Files to create:**
- `packages/local_remote_synchronization/README.md` — add SyncEngine section
- `docs/architecture.md` — update status from "planned" to "implemented"
- `examples/basic_offline_first_app/` — add SyncEngine usage (if example exists)

---

## Appendix A — Usage Example

```dart
import 'package:local_remote_synchronization/local_remote_synchronization.dart';
import 'package:local_remote_sqflite/local_remote_sqflite.dart';
import 'package:local_remote_supabase/local_remote_supabase.dart'; // future

Future<void> main() async {
  // Initialize adapters
  final db = await openDatabase('app.db');
  final localStorage = SqfliteLocalStorageAdapter(database: db);
  final remoteSync = SupabaseRemoteAdapter(client: supabase);
  final connectivity = ConnectivityPlusProvider(); // implements ConnectivityProvider

  // Create engine with defaults
  final engine = SyncEngine(
    localStorage: localStorage,
    remoteSync: remoteSync,
    connectivity: connectivity,
    config: SyncEngineConfig(
      batchSize: 100,
      maxConcurrency: 10,
    ),
    retryPolicy: ExponentialBackoffRetryPolicy(
      initialDelay: const Duration(seconds: 2),
      maxDelay: const Duration(minutes: 10),
      maxAttempts: 8,
    ),
  );

  // Handle conflicts — attach listener BEFORE start to avoid losing events
  engine.conflicts.listen((event) {
    print('Conflict on ${event.operation.entityType}/${event.operation.entityId}');
    print('Remote snapshot: ${event.conflict.remoteSnapshot}');
    // NOTE: remoteSnapshot is in-memory only. If the app crashes before
    // this handler runs, the snapshot is lost. Handle conflicts promptly
    // or re-fetch from remote.
    //
    // Semantic mismatch warning: markSucceeded means "remote accepted,"
    // not "discard conflict." Use with care.
  });

  // Monitor sync cycles
  engine.reports.listen((report) {
    print('Sync cycle: $report');
    if (report.cycleError != null) {
      print('Cycle error: ${report.cycleError}');
    }
  });

  // Start engine (recovers in-flight once, begins auto-sync)
  await engine.start();

  // Manual sync — requires start() to have completed
  final report = await engine.syncNow();
  print('Manual sync: $report');

  // ... app runs ...

  // Cleanup — waits for active cycle to finish (no timeout)
  await engine.dispose();
  await localStorage.dispose();
}
```

## Appendix B — Decision Records

The following decisions are significant enough to document but do not
warrant separate ADR files:

| Decision | Rationale |
|----------|-----------|
| Engine in Core, not separate package | Zero non-Core dependencies; architecture doc already plans it in Core |
| Chunked concurrency over sliding window | Simplicity and predictability; most workloads don't need optimization |
| Conflict → deadLetter then emit event | Durable persistence before notification; no event if storage fails |
| Conflict recovery not guaranteed | Broadcast stream, no listener = lost; remoteSnapshot not persisted |
| No `markSucceeded` as conflict resolution | Semantic mismatch; documented as known limitation |
| Recovery once on `start()`, not per cycle | Prevents interfering with other engine instances' in-flight ops |
| `syncNow()` requires `start()` | Ensures recovery completes before any sync cycle |
| Single-flight via synchronous `_activeCycle` assignment | Eliminates race condition without locking abstractions |
| Dispose waits without timeout | No cancellation support in contracts; safe first-version policy |
| `retryAfter` as minimum, not override | Server delay is a floor; local backoff/jitter cannot shorten it |
| RNG owned by `ExponentialBackoffRetryPolicy` only | Single ownership; `SyncEngine` does not accept `Random` |
| Result `operationId` validation | Prevents cross-operation contamination from adapter bugs |
| No auto-sync on enqueue (watchPendingCount) | Avoids sync storms; apps can call syncNow() explicitly |
| One engine per outbox recommended | Current contract lacks atomic claiming; idempotency keys mitigate |
