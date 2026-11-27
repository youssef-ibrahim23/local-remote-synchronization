# Phase 3 — Sync Engine Architecture

> **Status:** Design proposal — awaiting architecture review before implementation.
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
                           │ depends on
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
└──────┬──────────────────────────────┬───────────────────────┘
       │ implements                  │ implements
┌──────▼──────────┐         ┌─────────▼──────────────────────┐
│ LocalStorage    │         │ RemoteSyncAdapter              │
│ Adapter         │         │ (push-only)                    │
│                 │         │                                │
│ Sqflite/Drift/  │         │ Supabase/Firestore/REST/       │
│ Hive (existing) │         │ GraphQL (future)               │
└─────────────────┘         └────────────────────────────────┘
```

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

- Recover stale `inFlight` operations on `start()`.
- Trigger sync cycles on connectivity restoration (configurable).
- Execute manual sync cycles via `syncNow()`.
- Prevent overlapping sync cycles within the engine instance.
- Fetch eligible operations, push them through `RemoteSyncAdapter`, and
  persist outcomes through `LocalStorageAdapter`.
- Emit conflict events for application-level resolution.
- Emit sync cycle reports for observability.
- Cancel all subscriptions and release resources on `dispose()`.

### `SyncEngineConfig`

Immutable configuration value object. Controls batch size, concurrency,
connectivity behavior, and offline sync policy. Has sensible defaults so
most applications need no configuration.

### `RetryPolicy`

Abstract interface for computing retry delays and dead-letter decisions.
Keeps the engine decoupled from the specific backoff algorithm. The default
implementation is `ExponentialBackoffRetryPolicy`.

### `ExponentialBackoffRetryPolicy`

Concrete `RetryPolicy` implementation providing:

- Exponential backoff: `initialDelay * 2^(attemptCount - 1)`.
- Configurable `initialDelay`, `maxDelay`, `maxAttempts`, and `jitterFactor`.
- Jitter to prevent thundering-herd retries.
- Honors `SyncError.retryAfter` when the server provides a delay.
- Dead-letters on non-retryable errors or when `maxAttempts` is reached.

### `SyncCycleReport`

Immutable value object summarizing a single sync cycle. Returned by
`syncNow()` and emitted on the `reports` stream. Contains counts of
fetched/succeeded/failed/conflicted/dead-lettered operations and timing
information.

### `SyncConflictEvent`

Immutable value object emitted on the `conflicts` stream when a
`SyncConflict` is encountered. Contains the full `SyncOperation` and the
`SyncConflict` result (including `remoteSnapshot`) so the application can
perform conflict resolution without an extra storage lookup.

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
  /// [config] — engine configuration (defaults provided).
  /// [retryPolicy] — retry strategy (defaults to
  ///   [ExponentialBackoffRetryPolicy]).
  /// [random] — injectable RNG for jitter (defaults to [Random.secure]).
  ///   Injectable for deterministic testing.
  SyncEngine({
    required LocalStorageAdapter localStorage,
    required RemoteSyncAdapter remoteSync,
    required ConnectivityProvider connectivity,
    SyncEngineConfig config = const SyncEngineConfig(),
    RetryPolicy? retryPolicy,
    Random? random,
  });

  /// Whether the engine has been started and not yet disposed.
  bool get isRunning;

  /// Stream of [SyncCycleReport]s emitted after each completed cycle.
  /// Useful for observability, logging, and UI status indicators.
  Stream<SyncCycleReport> get reports;

  /// Stream of [SyncConflictEvent]s emitted when a push returns
  /// [SyncConflict]. The application must resolve conflicts and call the
  /// appropriate [LocalStorageAdapter] method to finalize the operation.
  Stream<SyncConflictEvent> get conflicts;

  /// Starts the engine.
  ///
  /// 1. Recovers stale `inFlight` operations via
  ///    [LocalStorageAdapter.recoverInFlightOperations].
  /// 2. Subscribes to [ConnectivityProvider.onConnectivityChanged].
  /// 3. If [SyncEngineConfig.syncOnStart] is `true` and the device is
  ///    online, triggers an initial sync cycle.
  ///
  /// Calling [start] more than once is a no-op.
  /// Throws [StateError] if called after [dispose].
  Future<void> start();

  /// Manually triggers a sync cycle.
  ///
  /// If a cycle is already in progress, returns the in-flight cycle's
  /// [SyncCycleReport] future (deduplication — no overlapping cycles).
  ///
  /// If [SyncEngineConfig.syncWhenOffline] is `false` (default) and the
  /// device is offline, returns a report with `skippedOffline: true`
  /// without fetching operations.
  ///
  /// Throws [StateError] if called after [dispose].
  Future<SyncCycleReport> syncNow();

  /// Releases all resources.
  ///
  /// 1. Cancels the connectivity subscription.
  /// 2. Waits for any in-flight sync cycle to complete (with a timeout).
  /// 3. Closes internal stream controllers.
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
  const SyncEngineConfig({
    this.batchSize = 50,
    this.maxConcurrency = 5,
    this.syncOnStart = true,
    this.syncOnConnectivityRestore = true,
    this.syncWhenOffline = false,
  });

  /// Maximum number of operations fetched per batch.
  /// Default: 50.
  final int batchSize;

  /// Maximum number of concurrent pushes within a batch.
  /// Default: 5.
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
  /// [SyncError.retryAfter] is non-null, implementations should honor it
  /// as a minimum delay.
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
/// Delay formula: `min(initialDelay * 2^(attemptCount - 1), maxDelay)`
/// then multiplied by a jitter factor in `[1 - jitterFactor, 1 + jitterFactor]`.
///
/// Example with `initialDelay = 1s`, `maxDelay = 5m`, `jitterFactor = 0.2`:
///   attempt 1 → ~1s   (0.8s – 1.2s)
///   attempt 2 → ~2s   (1.6s – 2.4s)
///   attempt 3 → ~4s   (3.2s – 4.8s)
///   attempt 4 → ~8s   (6.4s – 9.6s)
///   ...
///   attempt 10 → ~5m  (4m – 6m, capped)
final class ExponentialBackoffRetryPolicy implements RetryPolicy {
  /// Creates an [ExponentialBackoffRetryPolicy].
  ///
  /// [initialDelay] — delay before the first retry. Default: 1 second.
  /// [maxDelay] — maximum delay between retries. Default: 5 minutes.
  /// [maxAttempts] — maximum push attempts before dead-lettering.
  ///   Default: 10.
  /// [jitterFactor] — fractional jitter applied to each delay.
  ///   `0.0` = no jitter, `0.2` = ±20%. Default: 0.2.
  /// [random] — injectable RNG for jitter. Defaults to [Random.secure].
  ExponentialBackoffRetryPolicy({
    Duration initialDelay = const Duration(seconds: 1),
    Duration maxDelay = const Duration(minutes: 5),
    this.maxAttempts = 10,
    this.jitterFactor = 0.2,
    Random? random,
  });

  /// Maximum number of push attempts before dead-lettering.
  final int maxAttempts;

  /// Fractional jitter applied to computed delays.
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
    required this.skippedOffline,
  });

  /// When the cycle started.
  final DateTime startedAt;

  /// When the cycle completed (null if still running).
  final DateTime? completedAt;

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

  /// Whether the cycle was skipped because the device is offline.
  final bool skippedOffline;

  /// Duration of the sync cycle.
  Duration get duration => completedAt!.difference(startedAt);

  /// Whether the cycle completed without errors.
  bool get isSuccess => failed == 0 && conflicted == 0;

  @override
  String toString() =>
      'SyncCycleReport(fetched: $fetched, succeeded: $succeeded, '
      'failed: $failed, conflicted: $conflicted, '
      'deadLettered: $deadLettered, skippedOffline: $skippedOffline, '
      'duration: $duration)';
}
```

### `SyncConflictEvent`

```dart
/// Emitted when a push returns [SyncConflict].
///
/// The engine parks the operation as `deadLetter` to prevent further
/// automatic pushes for the same entity. The application must resolve the
/// conflict and call the appropriate [LocalStorageAdapter] method:
///
/// - Apply remote snapshot locally → call `markSucceeded` to remove.
/// - Discard local operation → call `markDeadLetter` (already done).
/// - Merge and re-enqueue → call `markSucceeded` then enqueue new mutation.
final class SyncConflictEvent {
  const SyncConflictEvent({
    required this.operation,
    required this.conflict,
  });

  /// The operation that encountered the conflict.
  final SyncOperation operation;

  /// The conflict result from the remote adapter.
  final SyncConflict conflict;
}
```

### Public API Summary

| Type | Purpose | Constructor params |
|------|---------|-------------------|
| `SyncEngine` | Orchestrator | `localStorage`, `remoteSync`, `connectivity`, `config?`, `retryPolicy?`, `random?` |
| `SyncEngineConfig` | Configuration | `batchSize`, `maxConcurrency`, `syncOnStart`, `syncOnConnectivityRestore`, `syncWhenOffline` |
| `RetryPolicy` | Interface | — |
| `ExponentialBackoffRetryPolicy` | Default retry | `initialDelay`, `maxDelay`, `maxAttempts`, `jitterFactor`, `random?` |
| `SyncCycleReport` | Cycle result | internal |
| `SyncConflictEvent` | Conflict notification | internal |

**No Core API changes required.** All types use existing contracts.

---

## 4. Sync Cycle Sequence

```
syncNow() called (manual or connectivity-triggered)
    │
    ▼
[Check disposed?] ──yes──► return skipped report
    │ no
    ▼
[Check _activeCycle != null?] ──yes──► return _activeCycle future
    │ no
    ▼
[Check connectivity?] ──offline & !syncWhenOffline──► return skipped report
    │ online or forced
    ▼
_create SyncCycleReport(startedAt: now)_
    │
    ▼
_activeCycle = _runCycle(report)_
    │
    ▼
┌─────────────────────────────────────────────────────┐
│ _runCycle:                                          │
│                                                     │
│ 1. recoverInFlightOperations()                      │
│    └─ resets stale inFlight → pending               │
│                                                     │
│ 2. loop:                                            │
│    a. fetchEligibleOperations(now, batchSize,        │
│       preservePerEntityOrder: true)                 │
│    b. if empty → break                              │
│    c. for each chunk of maxConcurrency:             │
│       i.   markInFlight(op.operationId)             │
│       ii.  remoteSync.push(op)                      │
│       iii. handle result:                           │
│            SyncSuccess → markSucceeded             │
│            SyncFailure → markFailed/markDeadLetter │
│            SyncConflict → emit + markDeadLetter    │
│            Exception → treat as SyncFailure        │
│       iv.  update report counters                   │
│    d. if fetched < batchSize → break                │
│                                                     │
│ 3. report.completedAt = now                         │
│ 4. reports.add(report)                              │
│ 5. return report                                    │
└─────────────────────────────────────────────────────┘
    │
    ▼
_activeCycle = null (whenComplete)
    │
    ▼
return report
```

### Key properties

- **Single-flight:** `_activeCycle` ensures only one cycle runs at a time.
  Concurrent `syncNow()` calls share the same future.
- **Recovery first:** `recoverInFlightOperations()` runs at the start of
  every cycle, not just on `start()`. This handles crashes that occur
  mid-cycle.
- **Batch loop:** Continues fetching until `fetchEligibleOperations`
  returns fewer than `batchSize` or empty. This drains the outbox in one
  cycle when possible.
- **Chunked concurrency:** Operations within a chunk are pushed
  concurrently. Chunks are processed sequentially.

---

## 5. Concurrency and FIFO Strategy

### How batch fetching and concurrency interact

`fetchEligibleOperations(preservePerEntityOrder: true)` returns at most
**one operation per entity** — the earliest eligible one. This is the key
to FIFO preservation under concurrency:

```
Outbox state:
  Entity A: op-A1 (seq 1, pending), op-A2 (seq 2, pending), op-A3 (seq 3, pending)
  Entity B: op-B1 (seq 4, pending), op-B2 (seq 5, pending)
  Entity C: op-C1 (seq 6, pending)

fetchEligibleOperations(batchSize: 10, preservePerEntityOrder: true)
  → returns [op-A1, op-B1, op-C1]  (earliest per entity)

Process chunk [op-A1, op-B1, op-C1] concurrently:
  op-A1 → inFlight → push → succeeded
  op-B1 → inFlight → push → failed (retry in 30s)
  op-C1 → inFlight → push → succeeded

Next fetchEligibleOperations:
  → returns [op-A2, op-C1's successor... wait, op-C1 succeeded so removed]
  → Actually: [op-A2, op-B2(blocked by op-B1 failed), op-C2 if exists]
```

Wait — `op-B1` failed with `nextRetryAt` in the future. It's still in the
outbox with status `failed`. `fetchEligibleOperations` checks:
- `pending` → always eligible
- `failed` with `nextRetryAt <= now` → eligible
- `failed` with `nextRetryAt > now` → NOT eligible

But `preservePerEntityOrder` checks if ANY earlier operation for the same
entity exists with a non-succeeded status. `op-B1` is `failed` (not
succeeded), so `op-B2` is blocked until `op-B1` either succeeds or is
dead-lettered.

This means:
- **Within an entity:** Strict FIFO. Only the earliest non-succeeded
  operation is eligible.
- **Across entities:** Full parallelism. Different entities don't block
  each other.
- **Failed operations:** Block their entity until retry succeeds or
  dead-letters. This is correct — you don't want to push B2 before B1
  succeeds.

### Is `fetchEligibleOperations` sufficient for FIFO under concurrency?

**Yes, within a single engine instance.** The sequence is:

1. `fetchEligibleOperations` returns earliest-per-entity ops.
2. Engine calls `markInFlight` on each before pushing.
3. `markInFlight` changes status to `inFlight`, which is a blocking
   status for FIFO purposes.
4. Even if the push fails, the operation remains `inFlight` or `failed`,
   still blocking later ops for the same entity.

The only gap is between `fetchEligibleOperations` and `markInFlight` —
another writer could theoretically modify the operation in that window.
Within one engine instance, the sync cycle mutex prevents this. The
adapter-level mutex (in Hive) or database transaction isolation (in
Sqflite/Drift) provides additional safety.

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
| Overlapping cycles | Prevented by `_activeCycle` mutex |
| Cross-instance | Not protected — rely on idempotency keys |

---

## 6. Retry Algorithm and Examples

### `ExponentialBackoffRetryPolicy` algorithm

```
computeDelay(attemptCount, error):
  1. If error.retryAfter != null → return error.retryAfter
     (server-suggested delay takes precedence)
  2. base = initialDelay * 2^(attemptCount - 1)
  3. capped = min(base, maxDelay)
  4. jitter = 1 - jitterFactor + random() * 2 * jitterFactor
  5. return Duration(milliseconds: capped.inMilliseconds * jitter)

shouldDeadLetter(attemptCount, error):
  1. If !error.isRetryable → return true
  2. If attemptCount >= maxAttempts → return true
  3. Return false
```

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
  → computeDelay returns 30s (retryAfter takes precedence)
  → markFailed(attemptCount: 1, nextRetryAt: now + 30s)
```

### Example: conflict

```
Push attempt 1 → SyncConflict(remoteSnapshot: {...})
  → Emit SyncConflictEvent on conflicts stream
  → markDeadLetter(attemptCount: 1, error: SyncError(conflict, ...))
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

### Decision: Park + Notify

The engine:

1. Emits a `SyncConflictEvent` on the `conflicts` stream containing the
   full `SyncOperation` and `SyncConflict` (including `remoteSnapshot`).
2. Calls `markDeadLetter` with a `SyncError(code: SyncErrorCode.conflict)`
   to park the operation.
3. The application subscribes to `conflicts`, resolves the conflict, and
   calls the appropriate `LocalStorageAdapter` method.

### Why `deadLetter` and not `failed`?

- `deadLetter` blocks later operations for the same entity — correct,
  you don't want to push B2 before resolving B1's conflict.
- `failed` with `nextRetryAt` would eventually retry — wrong, conflicts
  need manual resolution.
- `deadLetter` is visible in the outbox for inspection and manual
  recovery.

### Application resolution flow

```dart
engine.conflicts.listen((event) async {
  final op = event.operation;
  final conflict = event.conflict;

  // Option A: Accept remote state — mark succeeded (removes from outbox)
  await localStorage.markSucceeded(op.operationId);

  // Option B: Discard local change — already dead-lettered, optionally
  // clean up local domain data

  // Option C: Merge and re-enqueue — mark succeeded, then enqueue
  // a new LocalMutation with merged data
  await localStorage.markSucceeded(op.operationId);
  await localStorage.runInTransaction((outbox) async {
    await outbox.enqueue(LocalMutation(
      entityType: op.entityType,
      entityId: op.entityId,
      type: op.type,
      payload: mergedPayload,
    ));
  });
});
```

### Core API change needed?

**No.** The existing `markDeadLetter` + `markSucceeded` methods are
sufficient. The `remoteSnapshot` is delivered via the `conflicts` stream,
not persisted in the outbox. This is a deliberate trade-off:

- **Pro:** No Core changes, simpler contract.
- **Con:** `remoteSnapshot` is not persisted — if the app crashes after
  the conflict is emitted but before resolution, the snapshot is lost.
  The app must handle conflicts promptly or re-fetch from remote.

If persistent conflict storage is needed in the future, a
`markConflict(operationId, conflict)` method could be added to
`LocalStorageAdapter`. This is documented as a potential Phase 3.2+
enhancement but is not required for the initial engine.

---

## 8. Error Handling Matrix

| Scenario | Engine Behavior | Operation State After |
|----------|----------------|----------------------|
| `SyncSuccess` | `markSucceeded` | Removed from outbox |
| `SyncFailure` (retryable, attempts left) | `markFailed` with computed `nextRetryAt` | `failed`, eligible at `nextRetryAt` |
| `SyncFailure` (non-retryable or max attempts) | `markDeadLetter` | `deadLetter`, blocks entity |
| `SyncConflict` | Emit `SyncConflictEvent` + `markDeadLetter` | `deadLetter`, blocks entity |
| `RemoteSyncAdapter.push` throws | Treat as `SyncFailure(unknown, isRetryable: true)` | `failed` or `deadLetter` per retry policy |
| `markInFlight` throws | Log warning, skip operation this cycle | Unchanged (still `pending`/`failed`) |
| `markSucceeded` throws | Log warning, continue cycle | `inFlight` — recovered next cycle |
| `markFailed` throws | Log warning, continue cycle | `inFlight` — recovered next cycle |
| `markDeadLetter` throws | Log warning, continue cycle | `inFlight` — recovered next cycle |
| `fetchEligibleOperations` throws | Abort cycle, emit error report | Unchanged |
| `recoverInFlightOperations` throws | Abort cycle, emit error report | `inFlight` ops remain |
| Connectivity lost mid-push | Push fails with network error → retry policy | `failed` with backoff |
| App terminated mid-push | OS kills process | `inFlight` → recovered on next start |
| `dispose()` during cycle | Wait for cycle to complete (timeout), then close | `inFlight` ops recovered on next start |
| `syncNow()` after `dispose()` | Throws `StateError` | — |
| `start()` after `dispose()` | Throws `StateError` | — |

### Key invariants

- **No operation disappears** due to an unhandled exception. Every code
  path either marks the operation or leaves it in a recoverable state.
- **`inFlight` is always recoverable.** `recoverInFlightOperations` runs
  at the start of every cycle, so stale claims are reset.
- **Storage failures are non-fatal.** The engine logs and continues —
  a failed `markSucceeded` means the operation stays `inFlight` and
  will be retried (idempotency key prevents duplicate remote application).
- **Unexpected exceptions are retryable.** Conservative default — better
  to retry than to lose an operation.

---

## 9. Lifecycle and Disposal Behavior

### `start()`

```
start()
  ├─ if _started → return (idempotent)
  ├─ if _disposed → throw StateError
  ├─ _started = true
  ├─ await localStorage.recoverInFlightOperations()
  ├─ _connectivitySub = connectivity.onConnectivityChanged.listen((online) {
  │     if (online && config.syncOnConnectivityRestore) unawaited(syncNow());
  │   })
  ├─ if (config.syncOnStart && await connectivity.isConnected) {
  │     unawaited(syncNow());
  │   }
  └─ return
```

### `syncNow()`

```
syncNow()
  ├─ if _disposed → throw StateError
  ├─ if _activeCycle != null → return _activeCycle
  ├─ if (!config.syncWhenOffline && !await connectivity.isConnected) {
  │     return SyncCycleReport(skippedOffline: true, ...)
  │   }
  ├─ report = SyncCycleReport(startedAt: now, ...)
  ├─ _activeCycle = _runCycle(report)
  ├─ _activeCycle.whenComplete(() => _activeCycle = null)
  └─ return _activeCycle
```

### `dispose()`

```
dispose()
  ├─ if _disposed → return (idempotent)
  ├─ _disposed = true
  ├─ await _connectivitySub?.cancel()
  ├─ if (_activeCycle != null) {
  │     await _activeCycle.timeout(30s, onTimeout: () => currentReport)
  │   }
  ├─ await _reportController.close()
  ├─ await _conflictController.close()
  └─ return
```

### Disposal during an active cycle

- `_disposed` is checked at the start of each batch iteration. If set,
  the cycle stops fetching new operations but lets in-flight pushes
  complete.
- In-flight operations remain `inFlight` and are recovered on next `start()`.
- Stream controllers are closed after the cycle completes (or times out).

### Connectivity behavior

| Event | Action |
|-------|--------|
| `start()` + online + `syncOnStart` | Trigger `syncNow()` |
| `start()` + offline | No sync; wait for connectivity |
| Connectivity → online + `syncOnConnectivityRestore` | Trigger `syncNow()` |
| Connectivity → offline | Let current cycle finish; no new triggers |
| Manual `syncNow()` + offline + `!syncWhenOffline` | Return skipped report |
| Manual `syncNow()` + offline + `syncWhenOffline` | Attempt push anyway |

### Avoiding infinite retry loops

- **Connectivity triggers** are edge-triggered (only on `false → true`
  transition). No repeated triggers while staying online.
- **Retry delays** ensure failed operations aren't retried immediately.
  `nextRetryAt` gates eligibility.
- **Dead-letter** stops automatic retries for non-retryable errors and
  max-attempt exhaustion.
- **Cycle mutex** prevents overlapping cycles from stacking up.

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

The `remoteSnapshot` from `SyncConflict` is delivered via stream but not
stored durably. If the app crashes before resolving, the snapshot is lost.

**Mitigation:** App should handle conflicts promptly or re-fetch from
remote. Documented as acceptable for Phase 3.

**Future enhancement:** Add `markConflict` to `LocalStorageAdapter` to
persist conflict state and remote snapshot.

### 3. No pull synchronization

Engine is push-only. Remote changes are not fetched. This is by design —
pull requires a different contract (`RemoteSyncAdapter.pull` or similar)
and conflict resolution strategy.

### 4. No background scheduling

Engine runs only when the app is in the foreground (or when manually
triggered). No WorkManager/BGTaskScheduler integration. Documented in
architecture.md as a platform-level concern.

### 5. Chunked concurrency, not sliding window

Operations are processed in chunks of `maxConcurrency`. A slow operation
in a chunk delays the next chunk even if other slots are free. A sliding
window (start next op as soon as one completes) would improve throughput
but adds complexity.

**Trade-off:** Simplicity and predictability over maximum throughput.
Most sync workloads don't need sliding-window optimization.

### 6. No per-entity concurrency limit

`maxConcurrency` limits total parallel pushes, not per-entity. Since
`fetchEligibleOperations` returns at most one op per entity, per-entity
concurrency is naturally limited to 1. This is correct for FIFO.

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

---

## 11. Required Core API Changes

**None for Phase 3.0.**

The existing contracts are sufficient:

- `LocalStorageAdapter` provides all needed methods.
- `RemoteSyncAdapter.push` returns `SyncResult` (sealed: Success/Conflict/Failure).
- `ConnectivityProvider` provides `isConnected` and `onConnectivityChanged`.
- `SyncError` has `isRetryable`, `retryAfter`, `code` for retry decisions.
- `SyncConflict` has `remoteSnapshot` for conflict resolution.

### Potential future additions (not Phase 3)

| Addition | Purpose | Compatibility |
|----------|---------|---------------|
| `LocalStorageAdapter.markConflict` | Persist conflict state + remote snapshot | Breaking — new method on interface |
| `LocalStorageAdapter.claimOperations` | Atomic claiming for multi-instance | Breaking — new method on interface |
| `RemoteSyncAdapter.pull` | Pull synchronization | Breaking — new method on interface |
| `SyncEngineConfig.rateLimit` | Global rate limiting | Non-breaking — new config field |

These are documented for future phases but not required now.

---

## 12. Proposed Implementation Breakdown

### Phase 3.1 — Core Engine Skeleton

**Files to create:**
- `lib/src/engine/sync_engine.dart` — main engine class
- `lib/src/engine/sync_engine_config.dart` — configuration
- `lib/src/engine/sync_cycle_report.dart` — report value object
- `lib/src/engine/sync_conflict_event.dart` — conflict event
- `lib/src/engine/retry_policy.dart` — interface
- `lib/src/engine/exponential_backoff_retry_policy.dart` — default impl
- Update `lib/local_remote_synchronization.dart` — export new types

**Tests:**
- Engine lifecycle (start, syncNow, dispose)
- Basic sync cycle with mock adapters
- Connectivity-triggered sync
- Cycle deduplication (concurrent syncNow calls)

### Phase 3.2 — Retry and Failure Handling

**Files to modify:**
- `lib/src/engine/sync_engine.dart` — add retry logic
- `lib/src/engine/exponential_backoff_retry_policy.dart` — refine algorithm

**Tests:**
- Exponential backoff timing
- Jitter distribution
- Max attempts → dead-letter
- Non-retryable → immediate dead-letter
- `retryAfter` override
- All error handling matrix scenarios

### Phase 3.3 — Conflict Handling

**Files to modify:**
- `lib/src/engine/sync_engine.dart` — add conflict detection and emission
- `lib/src/engine/sync_conflict_event.dart` — finalize

**Tests:**
- Conflict emission on `SyncConflict` result
- Operation parked as dead-letter
- Conflict stream delivers full operation + remote snapshot
- Multiple conflicts in one cycle

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

### Phase 3.5 — Integration Tests

**Files to create:**
- `test/engine/sync_engine_integration_test.dart` — end-to-end with real adapters

**Tests:**
- Full sync cycle with Sqflite adapter + mock remote
- Crash recovery (inFlight → pending → retry)
- Connectivity loss during push
- Disposal during active cycle
- Multiple sequential cycles

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
    config: const SyncEngineConfig(
      batchSize: 100,
      maxConcurrency: 10,
    ),
    retryPolicy: ExponentialBackoffRetryPolicy(
      initialDelay: const Duration(seconds: 2),
      maxDelay: const Duration(minutes: 10),
      maxAttempts: 8,
    ),
  );

  // Handle conflicts
  engine.conflicts.listen((event) {
    print('Conflict on ${event.operation.entityType}/${event.operation.entityId}');
    print('Remote snapshot: ${event.conflict.remoteSnapshot}');
    // Resolve and call localStorage.markSucceeded or re-enqueue
  });

  // Monitor sync cycles
  engine.reports.listen((report) {
    print('Sync cycle: $report');
  });

  // Start engine (recovers in-flight, begins auto-sync)
  await engine.start();

  // Manual sync
  final report = await engine.syncNow();
  print('Manual sync: $report');

  // ... app runs ...

  // Cleanup
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
| Conflict → deadLetter + stream notification | No Core changes needed; remoteSnapshot delivered via stream |
| No auto-sync on enqueue (watchPendingCount) | Avoids sync storms; apps can call syncNow() explicitly |
| One engine per outbox recommended | Current contract lacks atomic claiming; idempotency keys mitigate |
| `syncWhenOffline` config flag | Allows manual override for false-negative connectivity checks |
