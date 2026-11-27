# Phase 3 — Sync Engine Architecture

> **Status:** Design proposal — revised after second architecture review.
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
│                 │ │ (future)           │ │                    │
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
- Apply an engine-level per-entity concurrency guard so that two
  operations for the same `(entityType, entityId)` are never pushed
  concurrently within one cycle, even if an adapter returns multiple
  operations for that entity.
- Use a fixed eligibility cutoff per cycle so that an operation is never
  attempted more than once within the same cycle.
- Emit conflict events **after** the durable `deadLetter` transition
  succeeds.
- Emit sync cycle reports for observability, including cycle-level errors.
- Stop accepting new sync requests, cancel connectivity subscriptions,
  and wait for startup and the active cycle to settle on `dispose()`.

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
- Jitter to prevent thundering-herd retries, with a strictly positive
  minimum delay to avoid tight retry loops.
- Honors `SyncError.retryAfter` as a **minimum** delay that local backoff
  and jitter must not shorten.
- Dead-letters on non-retryable errors or when `maxAttempts` is reached.
- Owns its random source for jitter; `SyncEngine` does not accept a
  separate `Random` parameter.
- Validates constructor arguments: `initialDelay > 0`,
  `maxDelay >= initialDelay`, `maxAttempts > 0`,
  `jitterFactor` in `[0.0, 1.0]`.
- Validates `attemptCount >= 1` in both `computeDelay` and
  `shouldDeadLetter`.

### `SyncCycleReport`

Immutable value object summarizing a single sync cycle. Returned by
`syncNow()` and emitted on the `reports` stream. Contains counts of
fetched/succeeded/failed/conflicted/dead-lettered operations, cycle-level
error state, and timing information. `completedAt` is non-null for all
emitted reports.

Counter relationships:
- `conflicted` counts operations whose remote result was `SyncConflict`.
- `deadLettered` counts **every** operation transitioned to `deadLetter`,
  including conflicts, non-retryable failures, and max-attempt exhaustion.
- Therefore `conflicted` is a **subset** of `deadLettered`.

### `SyncConflictEvent`

Immutable value object emitted on the `conflicts` stream **after** the
operation has been durably marked `deadLetter`. Contains the full
`SyncOperation` and the `SyncConflict` result (including `remoteSnapshot`)
as an ephemeral broadcast notification. The event is not durable storage.
The Core contract does not provide a conflict-resolution or discard
workflow.

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
  /// durably persisted as `deadLetter`.
  ///
  /// This is an **ephemeral broadcast notification**, not durable storage.
  /// If no listener is attached when the event is emitted, the event is
  /// lost. The `remoteSnapshot` is not persisted separately and may be
  /// lost if the process crashes before the application handles the event.
  ///
  /// The operation remains `deadLetter` after notification. Later
  /// operations for the same entity remain blocked. The Core contract
  /// does **not** provide a supported conflict-resolution or discard
  /// workflow. The application may inspect the dead-lettered operation
  /// and remote state manually, but no automated resolution is guaranteed.
  Stream<SyncConflictEvent> get conflicts;

  /// Starts the engine.
  ///
  /// 1. Recovers stale `inFlight` operations via
  ///    [LocalStorageAdapter.recoverInFlightOperations] **once** per
  ///    successful engine startup lifecycle.
  /// 2. Subscribes to [ConnectivityProvider.onConnectivityChanged].
  /// 3. Samples the initial connectivity state, reconciling any events
  ///    received during sampling.
  /// 4. Sets `_started = true`.
  /// 5. If [SyncEngineConfig.syncOnStart] is `true` and the device is
  ///    online, schedules an initial sync cycle (does not await it).
  ///
  /// Calling [start] when `_started` is already `true` returns
  /// immediately (idempotent).
  ///
  /// Concurrent calls to [start] share one startup operation — the second
  /// caller awaits the same [Future].
  ///
  /// If recovery, connectivity subscription, or connectivity sampling
  /// fails, [start] throws, partially initialized resources are cleaned
  /// up, `_started` remains `false`, and a subsequent [start] call will
  /// retry from the beginning. No sync cycle can begin before startup
  /// completes successfully.
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
  /// 1. If startup is in progress, waits for it to settle.
  /// 2. Sets `_disposed = true` so no new sync cycles can start.
  /// 3. Cancels the connectivity subscription.
  /// 4. Waits for the active sync cycle to complete (if any).
  ///    Does **not** impose a timeout — remote calls are not cancellable,
  ///    so shutdown time is bounded only by the remote adapter's own
  ///    timeouts.
  /// 5. Closes internal stream controllers **after** startup, the active
  ///    cycle, and any pending auto-triggers have completed and no
  ///    further events can be emitted.
  ///
  /// Concurrent calls to [dispose] share one disposal Future so every
  /// caller awaits completed cleanup.
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
///
/// Both methods validate that [attemptCount] is >= 1. The engine
/// guarantees this by passing `op.attemptCount + 1` (where
/// `op.attemptCount` starts at 0).
abstract interface class RetryPolicy {
  /// Computes the delay before the next retry attempt.
  ///
  /// [attemptCount] is the number of completed push attempts (1 after the
  /// first failure, 2 after the second, etc.). Must be >= 1.
  ///
  /// [error] is the [SyncError] that caused the failure. If
  /// [SyncError.retryAfter] is non-null, implementations **must** honor it
  /// as a minimum delay — local backoff and jitter must not produce a
  /// delay shorter than `retryAfter`.
  Duration computeDelay(int attemptCount, SyncError error);

  /// Returns `true` if the operation should be moved to `deadLetter`
  /// instead of being scheduled for another retry.
  ///
  /// [attemptCount] is the number of completed push attempts. Must be >= 1.
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
///      └─ uses saturating exponentiation: the exponent is bounded by
///         log2(maxDelay / initialDelay), so overflow cannot occur for
///         practical configurations. For very large attemptCount, base
///         is capped at maxDelay before any multiplication.
///   2. jittered = base * (1 - jitterFactor + random() * 2 * jitterFactor)
///      └─ with jitterFactor = 1.0, the lower bound is 0, which could
///         produce a zero delay. To prevent tight retry loops, the
///         result is clamped to a strictly positive minimum:
///         max(jittered, Duration(milliseconds: 1))
///   3. if error.retryAfter != null:
///        return max(jittered, error.retryAfter)  ← retryAfter is minimum
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

  /// Number of operations whose remote result was [SyncConflict].
  /// This is a subset of [deadLettered].
  final int conflicted;

  /// Number of operations transitioned to `deadLetter`, including:
  /// - Conflicts ([conflicted])
  /// - Non-retryable failures
  /// - Max-attempt exhaustion
  /// - Protocol violations (operationId mismatch)
  final int deadLettered;

  /// Cycle-level error that prevented the cycle from completing normally
  /// (e.g., `fetchEligibleOperations` threw, `markInFlight` failed,
  /// or a storage transition failed at the cycle level). Null if the
  /// cycle completed without cycle-level errors. Individual operation
  /// failures are counted in [failed] and [deadLettered], not here.
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
  /// - [deadLettered] > 0 (operations exhausted retries, non-retryable,
  ///   or protocol violations)
  /// - [cycleError] != null (cycle-level failure)
  ///
  /// Returns `true` if:
  /// - [skippedOffline] is `true` — this means "no failure occurred,"
  ///   but does **not** mean any operation was synchronized. A skipped
  ///   cycle is a no-op, not a successful sync.
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
/// The operation remains `deadLetter` after notification. Later
/// operations for the same entity remain blocked. The Core contract
/// does **not** provide a supported conflict-resolution or discard
/// workflow. [LocalStorageAdapter.markSucceeded] is semantically
/// incorrect as a conflict-resolution API (it means "remote accepted,
/// not "discard conflict") and must not be used as one.
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
  ├─ if _started → return  (idempotent — already running)
  ├─ if _startupFuture != null → return _startupFuture  (share one startup)
  ├─ _startupFuture = _performStartup()
  └─ return _startupFuture

_performStartup():
  ├─ try {
  │     ├─ await localStorage.recoverInFlightOperations()  ← once
  │     │   └─ if throws → cleanup, rethrow
  │     │
  │     ├─ _pendingConnectivityEvents = <bool>[]  ← buffer
  │     ├─ _connectivitySub = connectivity.onConnectivityChanged.listen(
  │     │     (online) {
  │     │       if (_initializing) {
  │     │         _pendingConnectivityEvents.add(online)  ← buffer
  │     │       } else {
  │     │         _handleConnectivityEvent(online)
  │     │       }
  │     │     },
  │     │     onError: (e) { log; }
  │     │   )
  │     │   └─ if throws → cleanup, rethrow
  │     │
  │     ├─ _initializing = true
  │     ├─ sampledConnected = await connectivity.isConnected
  │     │   └─ if throws → cleanup, rethrow
  │     │
  │     ├─ _initializing = false
  │     ├─ _isConnected = sampledConnected
  │     ├─ _wasConnected = sampledConnected
  │     │
  │     ├─ // Reconcile buffered events: apply only the LAST event,
  │     │   // which represents the most recent connectivity state.
  │     │   // Events received during sampling may be newer than the
  │     │   // sample, so they take precedence.
  │     ├─ if (_pendingConnectivityEvents.isNotEmpty) {
  │     │     final latest = _pendingConnectivityEvents.last
  │     │     _pendingConnectivityEvents.clear()
  │     │     _isConnected = latest
  │     │     _wasConnected = latest
  │     │   }
  │     │
  │     ├─ if (_disposed) → cleanup, return  ← disposal during startup
  │     │
  │     ├─ _started = true  ← set BEFORE scheduling auto-sync
  │     │
  │     ├─ if (config.syncOnStart && _isConnected) {
  │     │     unawaited(_triggerAutoSync())  ← scheduled, not awaited
  │     │   }
  │     │
  │     └─ _startupFuture = null
  │
  │ } catch (e) {
  │     _startupFuture = null
  │     _started = false
  │     cleanup partial resources (cancel sub, clear buffers)
  │     rethrow
  │ }
```

**Failed startup:** If any step throws, `_startupFuture` is cleared,
`_started` remains `false`, partial resources (connectivity subscription,
buffers) are cleaned up, and the exception propagates to the caller. A
subsequent `start()` call will create a new `_startupFuture` and retry
from the beginning. No sync cycle can begin before startup completes
because `_started` is only set to `true` after all startup steps succeed.

**`syncOnStart` ordering:** `_started = true` is set **before**
`_triggerAutoSync()` is scheduled. This ensures the auto-trigger's call
to `syncNow()` passes the `_started` check. The auto-trigger cannot run
while startup is incomplete because it is scheduled after `_started` is
set.

**Initial connectivity sampling:** The subscription is established
**before** sampling `isConnected`. Events received during sampling are
buffered in `_pendingConnectivityEvents`. After sampling completes, the
last buffered event (if any) overrides the sample, because it represents
the most recent connectivity state. This prevents an older sample from
overwriting a newer event.

### Sync cycle (`syncNow()` or auto-trigger)

```
syncNow()
  ├─ if _disposed → throw StateError
  ├─ if !_started → throw StateError("SyncEngine.start() must complete first")
  ├─ if _activeCycle != null → return _activeCycle  (share active cycle)
  ├─ _activeCycle = _runCycle()  ← synchronous assignment, returns Future
  └─ return _activeCycle

_runCycle():
  ├─ report = SyncCycleReport(startedAt: now, ...)
  ├─ eligibilityCutoff = now  ← fixed cutoff for this cycle
  ├─ attemptedOperationIds = <String>{}  ← track attempted ops
  ├─ entityInFlight = <String>{}  ← per-entity concurrency guard
  │
  ├─ if (!config.syncWhenOffline && !_isConnected) {
  │     report.skippedOffline = true
  │     report.completedAt = now
  │     reports.add(report)
  │     return report
  │   }
  │
  ├─ try {
  │     loop:
  │       a. operations = await localStorage.fetchEligibleOperations(
  │            now: eligibilityCutoff,  ← FIXED cutoff
  │            limit: config.batchSize,
  │            preservePerEntityOrder: true,
  │          )
  │          └─ if throws → report.cycleError = SyncError(storage, ...), break
  │
  │       b. if operations.isEmpty → break
  │
  │       c. report.fetched += operations.length
  │
  │       d. // Filter out operations already attempted this cycle
  │          // (shouldn't happen with fixed cutoff, but defensive)
  │          operations = operations.where(
  │            (op) => !attemptedOperationIds.contains(op.operationId)
  │          ).toList()
  │          if (operations.isEmpty) → break
  │
  │       e. // Apply per-entity concurrency guard: group by entity,
  │          // select only the first operation per entity that isn't
  │          // already in entityInFlight
  │          final entityFirstOp = <String, SyncOperation>{}
  │          for (final op in operations) {
  │            final key = '${op.entityType}:${op.entityId}'
  │            if (!entityInFlight.contains(key) &&
  │                !entityFirstOp.containsKey(key)) {
  │              // Select earliest by sequenceNumber (null = 0),
  │              // then createdAt, then operationId (deterministic tiebreak)
  │              final existing = entityFirstOp[key]
  │              if (existing == null || _isEarlier(op, existing)) {
  │                entityFirstOp[key] = op
  │              }
  │            }
  │          }
  │          final selectedOps = entityFirstOp.values.toList()
  │
  │       f. for each chunk of maxConcurrency:
  │          i.   // Mark all ops in chunk as inFlight and add to
  │               // entityInFlight BEFORE pushing
  │               for (final op in chunk) {
  │                 try {
  │                   await localStorage.markInFlight(op.operationId)
  │                   attemptedOperationIds.add(op.operationId)
  │                   entityInFlight.add('${op.entityType}:${op.entityId}')
  │                 } catch (e) {
  │                   // markInFlight failure = cannot safely determine
  │                   // if operation was claimed. ABORT CYCLE.
  │                   report.cycleError = SyncError(
  │                     code: SyncErrorCode.storage,
  │                     message: 'markInFlight failed: $e',
  │                     isRetryable: true,
  │                   )
  │                   break loop  ← abort entire cycle
  │                 }
  │               }
  │
  │          ii.  // Push all ops in chunk concurrently
  │               final results = await Future.wait(
  │                 chunk.map((op) => _pushAndHandle(op, report)),
  │               )
  │
  │          iii. // Remove entity keys from entityInFlight after
  │               // chunk completes (success or failure)
  │               for (final op in chunk) {
  │                 entityInFlight.remove('${op.entityType}:${op.entityId}')
  │               }
  │
  │       g. if operations.length < config.batchSize → break
  │          // (fewer than batchSize means no more eligible ops)
  │
  │     } catch (e) {
  │       report.cycleError = SyncError(unknown, ...)
  │     } finally {
  │       report.completedAt = now
  │       reports.add(report)
  │       _activeCycle = null
  │     }
  │     return report
```

**Single-flight reservation:** `_activeCycle` is assigned synchronously
in `syncNow()` (immediately after the null check, before any `await`).
`_runCycle()` is called synchronously and returns a `Future` that is
immediately assigned to `_activeCycle`. Two concurrent callers cannot
both pass the check — the first caller assigns `_activeCycle` before
yielding to the event loop, so the second caller sees the non-null value
and returns the same future. No async gap exists between the check and
the assignment. No `Completer` is needed.

**Manual and connectivity-triggered sharing:** Both `syncNow()` and
`_triggerAutoSync()` call the same `syncNow()` method which performs the
single-flight check and reservation. They share the same `_activeCycle`
future.

### `_pushAndHandle(operation, report)`

```
_pushAndHandle(op, report):
  ├─ try {
  │     result = await remoteSync.push(op)
  │   } catch (e, st) {
  │     result = SyncFailure(
  │       operationId: op.operationId,
  │       error: SyncError(
  │         code: SyncErrorCode.unknown,
  │         message: 'Unexpected push error: $e',
  │         isRetryable: true,
  │         cause: e,
  │         stackTrace: st,
  │       ),
  │     )
  │   }
  │
  ├─ // Validate result.operationId matches
  │   if (result.operationId != op.operationId) {
  │     // Protocol violation — immediate dead-letter, NOT markFailed
  │     final error = SyncError(
  │       code: SyncErrorCode.unknown,
  │       message: 'Protocol violation: result.operationId '
  │         '(${result.operationId}) != pushed operationId '
  │         '(${op.operationId})',
  │       isRetryable: false,
  │     )
  │     try {
  │       await localStorage.markDeadLetter(
  │         op.operationId,
  │         attemptCount: op.attemptCount + 1,
  │         error: error,
  │       )
  │       report.deadLettered++
  │     } catch (e) {
  │       // Storage failure — op stays inFlight, recovered on next start
  │       log warning
  │     }
  │     return
  │   }
  │
  ├─ switch (result) {
  │     case SyncSuccess:
  │       try {
  │         await localStorage.markSucceeded(op.operationId)
  │         report.succeeded++
  │       } catch (e) {
  │         log warning  // op stays inFlight
  │       }
  │
  │     case SyncFailure:
  │       final error = result.error
  │       final newAttemptCount = op.attemptCount + 1
  │       if (retryPolicy.shouldDeadLetter(newAttemptCount, error)) {
  │         try {
  │           await localStorage.markDeadLetter(
  │             op.operationId,
  │             attemptCount: newAttemptCount,
  │             error: error,
  │           )
  │           report.deadLettered++
  │         } catch (e) {
  │           log warning
  │         }
  │       } else {
  │         final delay = retryPolicy.computeDelay(newAttemptCount, error)
  │         try {
  │           await localStorage.markFailed(
  │             op.operationId,
  │             attemptCount: newAttemptCount,
  │             nextRetryAt: now.add(delay),
  │             error: error,
  │           )
  │           report.failed++
  │         } catch (e) {
  │           log warning
  │         }
  │       }
  │
  │     case SyncConflict:
  │       final error = SyncError(
  │         code: SyncErrorCode.conflict,
  │         message: 'Remote conflict: ${result.remoteVersion ?? "unknown"}',
  │         isRetryable: false,
  │       )
  │       try {
  │         await localStorage.markDeadLetter(
  │           op.operationId,
  │           attemptCount: op.attemptCount + 1,
  │           error: error,
  │         )
  │         report.deadLettered++
  │         report.conflicted++
  │         // Emit event ONLY after markDeadLetter succeeds
  │         conflictController.add(SyncConflictEvent(
  │           operation: op,
  │           conflict: result,
  │         ))
  │       } catch (e) {
  │         // markDeadLetter failed — no event emitted
  │         log warning  // op stays inFlight
  │       }
  │   }
```

### Key properties

- **Recovery once:** `recoverInFlightOperations()` runs only during
  `start()`, not during sync cycles. This prevents resetting `inFlight`
  operations belonging to another active engine instance during manual
  syncs.
- **Idempotent start:** If `_started` is already `true`, `start()`
  returns immediately. Recovery executes only once per successful
  startup lifecycle.
- **Single-flight:** `_activeCycle` is assigned synchronously before the
  first `await`, eliminating the race condition. No `Completer` needed.
- **Fixed eligibility cutoff:** `eligibilityCutoff = now` is captured at
  cycle start and used for all `fetchEligibleOperations` calls. This
  ensures an operation whose `nextRetryAt` becomes eligible during the
  cycle is NOT fetched again in the same cycle — preventing same-cycle
  retries.
- **Per-entity concurrency guard:** `entityInFlight` tracks which
  entities have operations currently being pushed. Even if an adapter
  returns multiple operations for the same entity, only the earliest is
  selected per chunk. This is a defensive engine-level rule.
- **`markInFlight` failure aborts cycle:** If `markInFlight` throws, the
  engine cannot safely determine whether the operation was claimed. The
  cycle aborts with `cycleError` set. No further batches are fetched.
  This prevents infinite loops where the same unclaimed operation is
  fetched repeatedly.
- **All chunk operations awaited:** `Future.wait` ensures every
  concurrent push in a chunk completes before the next chunk starts.
- **Result validation:** Every `SyncResult` is checked against the pushed
  operation's `operationId` before any state transition. Mismatched
  results cause immediate `markDeadLetter` (non-retryable protocol
  violation), never `markFailed`.
- **Conflict ordering:** `markDeadLetter` succeeds **before** the conflict
  event is emitted. If storage fails, no event is emitted.

---

## 5. Concurrency and FIFO Strategy

### How batch fetching and concurrency interact

`fetchEligibleOperations(preservePerEntityOrder: true)` is documented in
the Core contract as returning operations where "an operation is not
eligible if any earlier operation for the same `(entityType, entityId)`
has a status other than `succeeded`." However, the contract does **not**
guarantee that at most one operation per entity is returned, and the
current adapter implementations have known limitations:

**Sqflite and Drift:** The `NOT EXISTS` clause uses
`prev.sequence_number < o.sequence_number`. In SQLite:
- If both sequence numbers are `NULL`, the comparison is `NULL` (falsy),
  so the filter does not block — multiple operations with null sequence
  numbers for the same entity could all be eligible.
- SQLite `ASC` ordering places `NULL` values **first**, not last.
- The `sequence_number` column is `INTEGER PRIMARY KEY AUTOINCREMENT`,
  which always generates non-null values, but the `SyncOperation`
  model's `sequenceNumber` field is `int?` (nullable). Operations
  constructed outside the adapter's enqueue path (e.g., imported or
  manually inserted) could have null sequence numbers.

**Hive:** Uses `op.sequenceNumber ?? 0` as a fallback. Null sequence
numbers are treated as 0, which could cause ordering ambiguity if a real
sequence number is also 0 (unlikely with autoincrement, but not
contractually guaranteed).

**Conclusion:** The current adapters do **not** establish a universal
guarantee that at most one eligible operation per entity is returned for
every possible persisted record. The engine must apply a defensive
per-entity concurrency guard.

### Engine-level per-entity guard

Regardless of what the adapter returns, the engine ensures that two
operations for the same `(entityType, entityId)` are never pushed
concurrently within one cycle:

1. After fetching a batch, the engine groups operations by
   `(entityType, entityId)`.
2. For each entity, the engine selects the **earliest** operation using
   this deterministic tiebreak order:
   - `sequenceNumber` ascending (null treated as 0)
   - `createdAt` ascending
   - `operationId` ascending (final deterministic tiebreak)
3. Only the selected operation per entity is pushed in the current chunk.
4. `entityInFlight` tracks entities with operations currently being
   pushed. Operations for entities already in `entityInFlight` are
   skipped in subsequent chunks within the same cycle.
5. After a chunk completes, entity keys are removed from
   `entityInFlight`, allowing the next operation for that entity to be
   fetched in the next batch iteration.

This guard does **not** claim deterministic FIFO ordering — it only
prevents concurrent pushes for the same entity. The actual ordering
depends on adapter behavior and the tiebreak rules above.

### FIFO guarantees and limitations

| Aspect | Behavior | Source |
|--------|----------|--------|
| At most one eligible op per entity | **Not guaranteed** by contract or all adapters. Engine applies defensive guard. | Engine-level guard |
| Ordering within batch | `createdAt` ascending, then `sequenceNumber` ascending. Null sequenceNumbers: SQLite places first, Hive treats as 0. | Adapter-dependent |
| Ordering when timestamps equal | `sequenceNumber` ascending as tiebreaker; if both null, order is adapter-defined | Contract does not specify; adapter-dependent |
| `sequenceNumber` null | SQLite: sorts first, doesn't block via NOT EXISTS. Hive: treated as 0. | Adapter-dependent; no universal guarantee |
| `failed` blocks entity | Yes — `preservePerEntityOrder` checks for any non-`succeeded` earlier operation. But "earlier" is determined by `sequence_number <`, which fails for null/equal values. | Contract documents rule; adapter implementation may not enforce for null seq |
| `deadLetter` blocks entity | Same as `failed` — depends on sequence_number comparison. | Same limitation |
| `inFlight` blocks entity | Same as `failed` — depends on sequence_number comparison. | Same limitation |
| Unrelated entities proceed | Yes — blocking is per `(entityType, entityId)`, not global | Contract explicitly documents |

### Is `fetchEligibleOperations` sufficient for FIFO under concurrency?

**No, not universally.** The contract documents the blocking rule but
does not guarantee:
- At most one operation per entity per fetch.
- Deterministic ordering when sequence numbers are null or equal.
- Correct blocking behavior when sequence number comparisons involve
  null values.

**The engine compensates** with:
- Per-entity concurrency guard (prevents concurrent pushes for same entity).
- Deterministic tiebreak for selecting earliest operation per entity.
- Fixed eligibility cutoff (prevents same-cycle retries).

**However, the engine cannot compensate for:**
- Incorrect blocking decisions by the adapter (e.g., an adapter failing
  to block a later operation because of null sequence number comparison).
- Non-deterministic ordering across batches when timestamps and sequence
  numbers are equal.

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

### Adapter/contract corrections needed (future task)

The following corrections may be required in a later task to establish
universal FIFO guarantees:

1. **Sqflite/Drift:** The `NOT EXISTS` clause should handle null
   sequence numbers explicitly, e.g., using
   `COALESCE(prev.sequence_number, 0) < COALESCE(o.sequence_number, 0)`
   or requiring non-null sequence numbers.
2. **Sqflite/Drift:** The `ORDER BY` clause should explicitly place null
   sequence numbers last, e.g.,
   `ORDER BY o.created_at_ms ASC, o.sequence_number IS NULL, o.sequence_number ASC`.
3. **Hive:** The `?? 0` fallback should be documented or replaced with
   explicit null handling.
4. **Core contract:** `fetchEligibleOperations` should explicitly
   document whether at most one operation per entity is guaranteed, and
   define ordering semantics for null/equal sequence numbers.

These are **not** Phase 3 scope. The engine's defensive guard provides
safety within a single engine instance regardless.

### Concurrency model summary

| Aspect | Behavior |
|--------|----------|
| Batch size | `config.batchSize` ops per fetch |
| Concurrency | `config.maxConcurrency` parallel pushes per chunk |
| FIFO | Per-entity guard prevents concurrent pushes; ordering depends on adapter |
| Cross-entity | Full parallelism |
| Overlapping cycles | Prevented by synchronous `_activeCycle` assignment |
| Same-cycle retries | Prevented by fixed eligibility cutoff |
| Cross-instance | Not protected — rely on idempotency keys |

---

## 6. Retry Algorithm and Examples

### `ExponentialBackoffRetryPolicy` algorithm

```
computeDelay(attemptCount, error):
  0. Validate attemptCount >= 1, else throw ArgumentError
  1. base = min(initialDelay * 2^(attemptCount - 1), maxDelay)
     └─ saturating exponentiation: exponent bounded by
        log2(maxDelay / initialDelay). For large attemptCount,
        base is capped at maxDelay before multiplication.
  2. jittered = base * (1 - jitterFactor + random() * 2 * jitterFactor)
     └─ with jitterFactor = 1.0, lower bound is 0.
        Clamp to strictly positive: max(jittered, 1ms)
  3. if error.retryAfter != null:
        return max(jittered, error.retryAfter)  ← retryAfter is minimum
     else:
        return jittered

shouldDeadLetter(attemptCount, error):
  0. Validate attemptCount >= 1, else throw ArgumentError
  1. If !error.isRetryable → return true
  2. If attemptCount >= maxAttempts → return true
  3. Return false
```

### Same-cycle retry prevention

The engine uses a **fixed eligibility cutoff** (`eligibilityCutoff = now`
captured at cycle start) for all `fetchEligibleOperations` calls within
a cycle. This means:

- An operation whose `nextRetryAt` is set to a time **after** the cutoff
  will not be eligible in subsequent batch iterations of the same cycle.
- An operation whose `nextRetryAt` is set to a time **before or equal to**
  the cutoff was already eligible at cycle start and would have been
  fetched in the first batch (if within `batchSize` limit).
- Therefore, no operation is attempted more than once within a single
  cycle, even if its retry delay is very short.

Additionally, `attemptedOperationIds` tracks all operations that have
been pushed in the current cycle. Any operation in this set is filtered
out of subsequent fetches as a defensive measure.

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

### `jitterFactor = 1.0` edge case

With `jitterFactor = 1.0`, the jitter multiplier ranges from
`1 - 1.0 = 0.0` to `1 + 1.0 = 2.0`. A multiplier of 0.0 would produce a
zero delay, creating a tight retry loop. To prevent this, the result is
clamped to a strictly positive minimum:

```
jittered = max(jittered, Duration(milliseconds: 1))
```

This ensures the delay is always > 0, even at the extreme of
`jitterFactor = 1.0`.

### Numeric overflow prevention

`2^(attemptCount - 1)` can overflow for large `attemptCount`. The
implementation uses saturating exponentiation without relying on a
hard-coded bit-width threshold:

```
// Compute base without overflow:
// The exponent is bounded by log2(maxDelay / initialDelay).
// Once base would exceed maxDelay, cap it immediately.

if (attemptCount == 1) → base = initialDelay
else {
  // Iteratively double, checking for overflow at each step
  base = initialDelay
  for (i = 1; i < attemptCount; i++) {
    if (base >= maxDelay) { base = maxDelay; break; }
    // Check if doubling would overflow
    if (base > maxDelay ~/ 2) { base = maxDelay; break; }
    base *= 2
  }
  base = min(base, maxDelay)
}
```

This approach:
- Does not rely on a hard-coded 63-bit or 32-bit threshold.
- Works correctly on any platform (Dart `int` is 64-bit on native,
  but the algorithm is platform-agnostic).
- Caps at `maxDelay` well before any overflow can occur.
- Is O(attemptCount) but attemptCount is bounded by `maxAttempts` (default 10).

### Example: network failure with default config

```
Config: initialDelay=1s, maxDelay=5m, maxAttempts=10, jitterFactor=0.2
Cycle eligibilityCutoff = T0

Push attempt 1 → SyncFailure(network, isRetryable=true)
  → markFailed(attemptCount: 1, nextRetryAt: T0 + ~1s)
  → nextRetryAt > T0, so NOT eligible in this cycle

[Next cycle at T1 > T0 + 1s] Push attempt 2 → SyncFailure(network)
  → markFailed(attemptCount: 2, nextRetryAt: T1 + ~2s)
  → nextRetryAt > T1, so NOT eligible in this cycle

... continues with exponential backoff across cycles ...

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
  → report.deadLettered++ AND report.conflicted++
  → Operation parked — blocks later ops for same entity
  → Application receives ephemeral notification; no guaranteed resolution
```

### `attemptCount` semantics

`SyncOperation.attemptCount` starts at `0`. The engine increments it on
each failure:

- `markFailed(attemptCount: op.attemptCount + 1, ...)`
- `markDeadLetter(attemptCount: op.attemptCount + 1, ...)`

This means `attemptCount` represents the number of **completed** push
attempts, not the current attempt number. After the first failure,
`attemptCount = 1`.

The engine passes `op.attemptCount + 1` to retry policy methods, which
guarantees `attemptCount >= 1` in all calls.

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
2. **After** `markDeadLetter` succeeds, increments `report.deadLettered`
   and `report.conflicted`, then emits a `SyncConflictEvent` on the
   `conflicts` stream containing the full `SyncOperation` and
   `SyncConflict` (including `remoteSnapshot`).
3. If `markDeadLetter` throws, the operation remains in its previous
   state (`inFlight`), no event is emitted, `report.deadLettered` and
   `report.conflicted` are not incremented, and the error is logged. The
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
  are gone.
- **Conflict recovery is NOT guaranteed.** The engine provides best-effort
  notification only.

### Conflict resolution — no supported workflow

The Core contract has **no dedicated conflict-resolution or discard
operation**. The available methods are:

- `markSucceeded(operationId)` — removes the operation from the outbox.
  Semantically means "the remote accepted this operation." Using it to
  discard a conflicted operation is a **semantic mismatch** — the remote
  did not accept the local change. **This must not be advertised as a
  supported conflict-resolution workflow.**
- `markDeadLetter(operationId, ...)` — already called by the engine.
  There is no way to "un-dead-letter" or transition to a resolved state.
- `runInTransaction` + `enqueue` — can enqueue a new mutation, but there
  is no atomic way to remove the dead-lettered operation and enqueue the
  replacement in one transaction.

**Known limitation:** The application cannot cleanly resolve a conflict
using existing Core methods. The dead-lettered operation remains in the
outbox indefinitely, blocking later operations for the same entity. The
application may:

1. Inspect the dead-lettered operation and remote state manually.
2. Apply `remoteSnapshot` to local domain state directly (bypassing the
   outbox).
3. Accept that the entity's sync is blocked until a future Core API
   addition provides a resolution mechanism.

Any merge-and-re-enqueue workflow that calls `markSucceeded` followed by
`enqueue` is **NOT atomic** — if the app crashes between the two calls,
the original operation is lost and the merged mutation is never created.
This must not be advertised as a guaranteed workflow.

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
updates. They are documented as future enhancements, **not Phase 3 scope**.

---

## 8. Error Handling Matrix

| Scenario | Engine Behavior | Operation State After | Report Counter |
|----------|----------------|----------------------|----------------|
| `SyncSuccess` | Validate `operationId`, then `markSucceeded` | Removed from outbox | `succeeded++` |
| `SyncFailure` (retryable, attempts left) | Validate `operationId`, then `markFailed` with computed `nextRetryAt` | `failed`, eligible at `nextRetryAt` (next cycle) | `failed++` |
| `SyncFailure` (non-retryable or max attempts) | Validate `operationId`, then `markDeadLetter` | `deadLetter`, blocks entity | `deadLettered++` |
| `SyncConflict` | Validate `operationId`, `markDeadLetter`, then emit event | `deadLetter`, blocks entity | `deadLettered++`, `conflicted++` |
| `SyncResult.operationId` mismatch | **Immediate** `markDeadLetter` with `SyncErrorCode.unknown`, `isRetryable: false` | `deadLetter`, blocks entity | `deadLettered++` |
| `RemoteSyncAdapter.push` throws | Treat as `SyncFailure(unknown, isRetryable: true)` | `failed` or `deadLetter` per retry policy | `failed++` or `deadLettered++` |
| `markInFlight` throws | **Abort cycle** with `cycleError` | Unchanged (still `pending`/`failed`) | `cycleError` set |
| `markSucceeded` throws | Log warning, continue with other ops in chunk | `inFlight` — recovered on next `start()` | No counter increment |
| `markFailed` throws | Log warning, continue with other ops in chunk | `inFlight` — recovered on next `start()` | No counter increment |
| `markDeadLetter` throws | Log warning, continue with other ops in chunk, no conflict event emitted | `inFlight` — recovered on next `start()` | No counter increment |
| `fetchEligibleOperations` throws | Set `report.cycleError`, abort cycle | Unchanged | `cycleError` set |
| `recoverInFlightOperations` throws (in `start`) | `start` throws, cleanup, `_started` remains `false` | `inFlight` ops remain; retry on next `start` | — |
| Connectivity subscription fails (in `start`) | `start` throws, cleanup, `_started` remains `false` | — | — |
| Connectivity sampling fails (in `start`) | `start` throws, cleanup, `_started` remains `false` | — | — |
| Connectivity lost mid-push | Push fails with network error → retry policy | `failed` with backoff | `failed++` or `deadLettered++` |
| App terminated mid-push | OS kills process | `inFlight` → recovered on next `start()` | — |
| `dispose()` during startup | Wait for startup to settle, then proceed with disposal | Depends on startup outcome | — |
| `dispose()` during cycle | Wait for cycle to complete (no timeout), then close streams | `inFlight` ops recovered on next `start()` | — |
| `syncNow()` before `start()` | Throws `StateError` | — | — |
| `syncNow()` after `dispose()` | Throws `StateError` | — | — |
| `start()` after `dispose()` | Throws `StateError` | — | — |
| `start()` when already started | Returns immediately (idempotent) | — | — |
| `start()` concurrent calls | Share one `_startupFuture` | — | — |
| `syncNow()` concurrent calls | Share one `_activeCycle` future | — | — |
| `dispose()` concurrent calls | Share one disposal Future | — | — |

### Storage failure classification

**Cycle-aborting storage failures:**
- `fetchEligibleOperations` throws → cannot determine what to push.
- `markInFlight` throws → cannot safely determine whether the operation
  was claimed. Continuing could cause the same operation to be fetched
  and pushed repeatedly in an infinite loop.

**Operation-level storage failures (non-aborting):**
- `markSucceeded` throws → operation stays `inFlight`, recovered on next
  `start()`. Other operations in the chunk can continue.
- `markFailed` throws → same as above.
- `markDeadLetter` throws → same as above. For conflicts, no event is
  emitted.

**Rationale:** `markInFlight` is the claiming mechanism. If it fails,
the engine cannot know whether the operation was claimed by this engine,
another engine, or no one. Aborting is the only safe choice. Post-push
transitions (`markSucceeded`, `markFailed`, `markDeadLetter`) are safe
to skip because the operation remains `inFlight` and will be recovered
on next `start()` — the remote adapter's idempotency key prevents
duplicate application.

### Key invariants

- **No operation disappears** due to an unhandled exception. Every code
  path either marks the operation or leaves it in a recoverable state.
- **`inFlight` is recoverable on `start()` only.** Recovery does not run
  during sync cycles, preventing interference with other engine instances.
- **Result validation prevents cross-operation contamination.** A
  mismatched `SyncResult` never causes the wrong operation to be marked
  succeeded. It causes immediate `markDeadLetter` (non-retryable).
- **Conflict events are emitted only after durable persistence.** If
  `markDeadLetter` fails, no event is emitted.
- **`markInFlight` failure aborts the cycle.** The engine does not
  continue fetching when it cannot safely claim operations.
- **Same-cycle retries are impossible.** The fixed eligibility cutoff
  and `attemptedOperationIds` set prevent an operation from being
  attempted more than once per cycle.
- **Unexpected exceptions are retryable.** Conservative default — better
  to retry than to lose an operation.

---

## 9. Lifecycle and Disposal Behavior

### `start()`

```
start()
  ├─ if _disposed → throw StateError
  ├─ if _started → return  (idempotent)
  ├─ if _startupFuture != null → return _startupFuture
  ├─ _startupFuture = _performStartup()
  └─ return _startupFuture

_performStartup():
  ├─ try {
  │     ├─ await localStorage.recoverInFlightOperations()  ← once
  │     │
  │     ├─ _pendingConnectivityEvents = <bool>[]
  │     ├─ _initializing = true
  │     ├─ _connectivitySub = connectivity.onConnectivityChanged.listen(
  │     │     (online) {
  │     │       if (_initializing) {
  │     │         _pendingConnectivityEvents.add(online)
  │     │       } else {
  │     │         _handleConnectivityEvent(online)
  │     │       }
  │     │     },
  │     │     onError: (e) { log; }
  │     │   )
  │     │
  │     ├─ sampledConnected = await connectivity.isConnected
  │     │
  │     ├─ _initializing = false
  │     ├─ _isConnected = sampledConnected
  │     ├─ _wasConnected = sampledConnected
  │     │
  │     ├─ // Reconcile buffered events
  │     ├─ if (_pendingConnectivityEvents.isNotEmpty) {
  │     │     final latest = _pendingConnectivityEvents.last
  │     │     _pendingConnectivityEvents.clear()
  │     │     _isConnected = latest
  │     │     _wasConnected = latest
  │     │   }
  │     │
  │     ├─ if (_disposed) → cleanup, return
  │     │
  │     ├─ _started = true  ← BEFORE scheduling auto-sync
  │     │
  │     ├─ if (config.syncOnStart && _isConnected) {
  │     │     unawaited(_triggerAutoSync())
  │     │   }
  │     │
  │     └─ _startupFuture = null
  │
  │ } catch (e) {
  │     _startupFuture = null
  │     _started = false
  │     _initializing = false
  │     await _connectivitySub?.cancel()
  │     _connectivitySub = null
  │     _pendingConnectivityEvents.clear()
  │     rethrow
  │ }
```

**`start()` does not await the initial auto-sync.** It sets `_started =
true` first, then schedules `_triggerAutoSync()` via `unawaited`. The
auto-trigger's call to `syncNow()` passes the `_started` check because
`_started` is already `true`. The caller can `await engine.start()` and
then `await engine.syncNow()` if they need to wait for the first cycle.

### `syncNow()`

```
syncNow()
  ├─ if _disposed → throw StateError
  ├─ if !_started → throw StateError("SyncEngine.start() must complete first")
  ├─ if _activeCycle != null → return _activeCycle
  ├─ _activeCycle = _runCycle()  ← synchronous assignment
  └─ return _activeCycle
```

**Single-flight:** `_activeCycle` is assigned synchronously in
`syncNow()` before any `await`. `_runCycle()` is called synchronously
and returns a `Future` that is immediately assigned to `_activeCycle`.
Two concurrent callers cannot both pass the null check because Dart's
single-threaded event loop ensures the assignment happens before the
second caller's continuation runs. No `Completer` is needed.

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
  ├─ if _disposeFuture != null → return _disposeFuture  (share disposal)
  ├─ _disposeFuture = _performDispose()
  └─ return _disposeFuture

_performDispose():
  ├─ // Wait for startup to settle if in progress
  ├─ if (_startupFuture != null) {
  │     try { await _startupFuture; } catch (e) { /* startup failed */ }
  │   }
  │
  ├─ _disposed = true
  │
  ├─ await _connectivitySub?.cancel()
  ├─ _connectivitySub = null
  │
  ├─ // Wait for active cycle to complete (no timeout)
  ├─ if (_activeCycle != null) {
  │     await _activeCycle
  │   }
  │
  ├─ // Wait for any pending auto-triggers to settle
  ├─ // (auto-triggers call syncNow() which checks _disposed,
  │   //  so they will return immediately or complete quickly)
  │
  ├─ await _reportController.close()
  ├─ await _conflictController.close()
  │
  └─ _disposeFuture = null
```

**No timeout on dispose:** The `RemoteSyncAdapter` contract has no
cancellation support. Imposing a timeout would risk closing stream
controllers while the active cycle can still emit events (reports,
conflicts). Instead, `dispose()` waits indefinitely for the active cycle
to complete. Shutdown time is bounded only by the remote adapter's own
timeouts and the storage adapter's responsiveness.

**Disposal during startup:** If `dispose()` is called while `start()` is
in progress, `_performDispose` first awaits `_startupFuture`. The startup
code checks `_disposed` after asynchronous gaps and will not create
subscriptions or schedule sync after disposal. If startup already
completed before `_disposed` was set, the disposal proceeds normally.

**Concurrent dispose:** The `_disposeFuture` ensures all concurrent
`dispose()` calls share one disposal operation. Every caller awaits the
same completed cleanup.

### Disposal during an active cycle

- `_disposed` is set to `true` immediately, preventing new `syncNow()`
  calls from starting new cycles.
- The active cycle continues to completion — in-flight pushes finish,
  storage transitions complete, and the report is emitted.
- After the cycle completes, `_activeCycle` is set to `null` (in the
  `finally` block of `_runCycle`).
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

### Initial connectivity sampling and reconciliation

1. `start()` subscribes to `onConnectivityChanged` **before** sampling
   `isConnected`. This ensures no transition is missed between sampling
   and subscription.
2. `_initializing = true` is set so events received during sampling are
   buffered in `_pendingConnectivityEvents` rather than processed
   immediately.
3. `isConnected` is awaited to get the current state.
4. `_initializing = false` is set.
5. `_isConnected` and `_wasConnected` are initialized to the sampled
   value.
6. If `_pendingConnectivityEvents` is non-empty, the **last** buffered
   event overrides the sample. This is because the last event represents
   the most recent connectivity state, which may be newer than the
   sample (the sample was taken at a point in time before the event
   was received).
7. Buffered events are cleared after reconciliation.

This prevents an older initial sample from overwriting a newer
connectivity event received while sampling.

### Avoiding infinite retry loops

- **Connectivity triggers** are edge-triggered (only on `false → true`
  transition). The `_wasConnected` flag deduplicates repeated `true`
  events.
- **Retry delays** ensure failed operations aren't retried immediately.
  `nextRetryAt` gates eligibility.
- **Fixed eligibility cutoff** ensures an operation is not retried
  within the same cycle, even if its retry delay is very short.
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

### 2. FIFO guarantees depend on adapter implementation

The Core contract documents the blocking rule but does not guarantee:
- At most one operation per entity per fetch.
- Deterministic ordering when sequence numbers are null or equal.
- Correct blocking behavior when sequence number comparisons involve
  null values.

The engine applies a defensive per-entity guard, but cannot compensate
for incorrect adapter-level blocking decisions.

**Adapter/contract corrections needed (future task):**
- Sqflite/Drift: handle null sequence numbers in `NOT EXISTS` and
  `ORDER BY` clauses.
- Hive: document or replace `?? 0` fallback.
- Core contract: explicitly document ordering and blocking semantics
  for null/equal sequence numbers.

### 3. Conflict `remoteSnapshot` not persisted

The `remoteSnapshot` from `SyncConflict` is delivered via broadcast
stream but not stored durably. If the app crashes before resolving, the
snapshot is lost. Conflict recovery is **not guaranteed**.

**Mitigation:** App should handle conflicts promptly or re-fetch from
remote. Documented as acceptable for Phase 3.

**Future enhancement:** Add `markConflict` to `LocalStorageAdapter` to
persist conflict state and remote snapshot.

### 4. No dedicated conflict-resolution or discard API

The Core contract has no `markResolved` or `markConflict` method.
`markSucceeded` is semantically incorrect for conflict resolution.
Merge-and-re-enqueue is not atomic. The dead-lettered operation blocks
the entity indefinitely.

**Future enhancement:** Add `markResolved` or `markConflict` to
`LocalStorageAdapter`.

### 5. No pull synchronization

Engine is push-only. Remote changes are not fetched. This is by design —
pull requires a different contract (`RemoteSyncAdapter.pull` or similar)
and conflict resolution strategy.

### 6. No background scheduling

Engine runs only when the app is in the foreground (or when manually
triggered). No WorkManager/BGTaskScheduler integration. Documented in
architecture.md as a platform-level concern.

### 7. Chunked concurrency, not sliding window

Operations are processed in chunks of `maxConcurrency`. A slow operation
in a chunk delays the next chunk even if other slots are free. A sliding
window (start next op as soon as one completes) would improve throughput
but adds complexity.

**Trade-off:** Simplicity and predictability over maximum throughput.
Most sync workloads don't need sliding-window optimization.

### 8. `watchPendingCount` not used for auto-sync

Engine does not listen to `watchPendingCount` to trigger syncs on new
mutations. Syncs are triggered by connectivity changes, manual calls, or
`syncOnStart`. Auto-sync on enqueue could be added in Phase 3.2+.

**Trade-off:** Avoids complexity and potential for sync storms. Apps
that need immediate sync can call `syncNow()` after enqueueing.

### 9. Retry policy is per-operation, not global

Each operation's retry delay is computed independently. There's no
global rate limiting or circuit breaker. If many operations fail
simultaneously, they all retry around the same time (mitigated by
jitter).

**Future enhancement:** Add a global rate limiter or token bucket.

### 10. Dispose has no timeout

`dispose()` waits indefinitely for the active cycle to complete. If the
remote adapter hangs, `dispose()` hangs. This is a safe first-version
policy — no cancellation support exists in the contracts.

**Future enhancement:** Add cancellation support to `RemoteSyncAdapter`
or a configurable dispose timeout with documented risk.

### 11. `markInFlight` failure aborts entire cycle

If `markInFlight` fails for any operation, the entire cycle aborts with
`cycleError`. This is conservative but may reduce throughput if storage
is intermittently unavailable. Other operations in the chunk that were
already pushed will complete, but no new batches will be fetched.

**Trade-off:** Safety over throughput. The alternative (continuing after
`markInFlight` failure) risks infinite loops and duplicate pushes.

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
| `LocalStorageAdapter.fetchEligibleOperations` contract clarification | Document null/equal sequence number semantics | Non-breaking — documentation only |
| Sqflite/Drift null sequence number handling | Fix NOT EXISTS and ORDER BY for null values | Non-breaking — adapter fix |
| Hive null sequence number handling | Document or replace `?? 0` fallback | Non-breaking — adapter fix |
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
- `start()` idempotency (already started returns immediately)
- `start()` retry after recovery failure
- `start()` retry after connectivity subscription failure
- `start()` retry after connectivity sampling failure
- Concurrent `start()` calls share one startup
- `syncNow()` before `start()` throws
- `dispose()` during startup
- `dispose()` concurrent calls share one disposal
- Single-flight cycle reservation (concurrent `syncNow` calls)
- Basic sync cycle with mock adapters
- Connectivity-triggered sync
- Connectivity event buffering during startup
- Result `operationId` validation (mismatch → immediate dead-letter)
- Configuration validation (batchSize, maxConcurrency, retry policy args)
- `markInFlight` failure aborts cycle
- Same-cycle retry prevention (fixed eligibility cutoff)
- Per-entity concurrency guard

### Phase 3.2 — Retry and Failure Handling

**Files to modify:**
- `lib/src/engine/sync_engine.dart` — add retry logic
- `lib/src/engine/exponential_backoff_retry_policy.dart` — refine algorithm

**Tests:**
- Exponential backoff timing
- Jitter distribution
- `jitterFactor = 1.0` produces strictly positive delay
- `retryAfter` as minimum (not shortened by backoff/jitter)
- Max attempts → dead-letter
- Non-retryable → immediate dead-letter
- Numeric overflow prevention for large attempt counts
- `attemptCount >= 1` validation
- Same-cycle retry prevention
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
- `report.conflicted` is subset of `report.deadLettered`
- No conflict-resolution workflow advertised

### Phase 3.4 — Concurrency and FIFO Verification

**Files to modify:**
- `lib/src/engine/sync_engine.dart` — verify batch/concurrency logic

**Tests:**
- Per-entity concurrency guard (no concurrent pushes for same entity)
- Cross-entity parallelism
- Batch size limits
- Max concurrency limits
- Failed operation blocks entity but not others
- Dead-letter blocks entity permanently
- Result `operationId` mismatch handling
- Fixed eligibility cutoff prevents same-cycle retries
- Adapter returning multiple ops per entity → guard selects earliest

### Phase 3.5 — Integration Tests

**Files to create:**
- `test/engine/sync_engine_integration_test.dart` — end-to-end with real adapters

**Tests:**
- Full sync cycle with Sqflite adapter + mock remote
- Crash recovery (inFlight → pending → retry on next start)
- Connectivity loss during push
- Disposal during active cycle (no timeout, waits for completion)
- Disposal during startup
- Multiple sequential cycles
- `start()` failure and retry
- Connectivity event buffering during startup

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

  // Handle conflicts — attach listener BEFORE start to avoid losing events.
  // This is an ephemeral notification, not durable storage.
  // The remoteSnapshot is lost if the app crashes before this handler runs.
  // The operation remains deadLetter; later ops for the same entity are blocked.
  // Core does NOT provide a conflict-resolution workflow.
  engine.conflicts.listen((event) {
    print('Conflict on ${event.operation.entityType}/${event.operation.entityId}');
    print('Remote snapshot: ${event.conflict.remoteSnapshot}');
    // The application may inspect and manually resolve, but no automated
    // resolution is guaranteed. markSucceeded is semantically incorrect
    // as a conflict-resolution API.
  });

  // Monitor sync cycles
  engine.reports.listen((report) {
    print('Sync cycle: $report');
    if (report.cycleError != null) {
      print('Cycle error: ${report.cycleError}');
    }
    if (report.skippedOffline) {
      print('Cycle skipped: device offline (no operations synchronized)');
    }
  });

  // Start engine (recovers in-flight once, begins auto-sync)
  await engine.start();

  // Manual sync — requires start() to have completed
  final report = await engine.syncNow();
  print('Manual sync: $report');

  // ... app runs ...

  // Cleanup — waits for startup and active cycle to finish (no timeout)
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
| No `markSucceeded` as conflict resolution | Semantic mismatch; documented as known limitation; not a supported workflow |
| Recovery once on `start()`, not per cycle | Prevents interfering with other engine instances' in-flight ops |
| `start()` idempotent when already started | Recovery executes only once per successful startup lifecycle |
| `syncNow()` requires `start()` | Ensures recovery completes before any sync cycle |
| `_started = true` before scheduling auto-sync | Auto-trigger cannot run while startup is incomplete |
| Single-flight via synchronous `_activeCycle` assignment | Eliminates race condition without locking abstractions |
| No `Completer` for cycle reservation | Synchronous assignment is sufficient in Dart's event loop |
| Fixed eligibility cutoff per cycle | Prevents same-cycle retries even with short retry delays |
| `attemptedOperationIds` defensive set | Additional guard against same-cycle duplicate attempts |
| Per-entity concurrency guard | Defensive rule; adapters don't guarantee one-op-per-entity |
| `markInFlight` failure aborts cycle | Cannot safely determine if operation was claimed; prevents infinite loops |
| Post-push storage failures don't abort cycle | Operation stays `inFlight`, recovered on next `start()` |
| Dispose waits without timeout | No cancellation support in contracts; safe first-version policy |
| Dispose shares one Future for concurrent calls | Every caller awaits completed cleanup |
| Startup checks `_disposed` after async gaps | Prevents creating subscriptions after disposal |
| Connectivity events buffered during startup | Prevents older sample from overwriting newer event |
| `retryAfter` as minimum, not override | Server delay is a floor; local backoff/jitter cannot shorten it |
| RNG owned by `ExponentialBackoffRetryPolicy` only | Single ownership; `SyncEngine` does not accept `Random` |
| `jitterFactor = 1.0` clamped to positive | Prevents zero-delay tight retry loops |
| Overflow-safe saturating backoff | No hard-coded bit-width threshold; platform-agnostic |
| Result `operationId` validation → immediate dead-letter | Protocol violation is non-retryable; never apply wrong result |
| `conflicted` is subset of `deadLettered` | Every conflict is dead-lettered; dead-lettered includes other causes |
| No auto-sync on enqueue (watchPendingCount) | Avoids sync storms; apps can call syncNow() explicitly |
| One engine per outbox recommended | Current contract lacks atomic claiming; idempotency keys mitigate |
