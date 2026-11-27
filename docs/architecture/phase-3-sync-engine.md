# Phase 3 — Sync Engine Architecture

> **Status:** Design proposal — revised after final architecture review.
>
> **Scope:** Push-only synchronization orchestration. No pull, no conflict
> resolution strategies, no background scheduling, no Flutter lifecycle.

> **Documentation levels:** Sections 1–11 describe the **target
> architecture** — the complete intended `SyncEngine` after Phase 3.6.
> Section 12 defines the **incremental implementation scope** — exactly
> which parts are built in each phase. A component described in the
> target architecture does **not** imply it is implemented in Phase 3.1;
> later phases extend the existing implementation without requiring
> premature implementation of their functionality. Phase tags
> (`[3.1]`–`[3.4]`) appear where target-state descriptions map to a
> specific implementation phase.

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

> **Target state.** The responsibilities below describe the complete
> `SyncEngine` after Phase 3.6 — not the Phase 3.1 subset. See Section 12
> for which responsibilities land in each phase.

### `SyncEngine`

The central orchestrator. Responsibilities:

- Recover stale `inFlight` operations **once** during successful `start()`.
- Trigger sync cycles on connectivity restoration (configurable).
- Execute manual sync cycles via `syncNow()`.
- Prevent overlapping sync cycles within the engine instance via a
  synchronous reservation token (`_CycleSlot`) assigned before the first
  asynchronous gap, with guaranteed cleanup of only its own reservation
  on every exit path.
- Fetch eligible operations, push them through `RemoteSyncAdapter`, and
  persist outcomes through `LocalStorageAdapter`.
- Validate `SyncResult.operationId` against the pushed operation before
  applying any state transition.
- Apply an engine-level per-entity ordering guard using collision-safe
  entity keys, ensuring two operations for the same `(entityType, entityId)`
  are never pushed concurrently within one cycle, and tracking entity
  outcomes across the whole cycle.
- Use a fixed eligibility cutoff per cycle so that an operation is never
  attempted more than once within the same cycle.
- Accumulate cycle statistics in an internal mutable accumulator and
  construct exactly one immutable `SyncCycleReport` per cycle.
- Emit conflict events **after** the durable `deadLetter` transition
  succeeds, using a pre-transition snapshot of the operation.
- Emit sync cycle reports for observability, including cycle-level errors
  and operation-level storage transition failures.
- On `dispose()`, set a disposal-requested state synchronously before any
  `await`, then await startup cleanup, cancel subscriptions, await the
  active cycle reservation, and close streams.

### `SyncEngineConfig`

Immutable configuration value object. Controls batch size, concurrency,
connectivity behavior, and offline sync policy. Has sensible defaults so
most applications need no configuration. Constructor validates that
`batchSize > 0` and `maxConcurrency > 0`.

### `RetryPolicy`

Abstract interface for computing retry delays and dead-letter decisions.
Keeps the engine decoupled from the specific backoff algorithm. The default
implementation is `ExponentialBackoffRetryPolicy`.

Implementations must safely support concurrent calls within a single Dart
isolate. They may be stateful (e.g., for jitter RNG) but must not corrupt
shared state when called concurrently from multiple `_pushAndHandle`
futures within one cycle.

### `ExponentialBackoffRetryPolicy`

Concrete `RetryPolicy` implementation providing:

- Exponential backoff: `initialDelay * 2^(attemptCount - 1)`.
- Configurable `initialDelay`, `maxDelay`, `maxAttempts`, and `jitterFactor`.
- Jitter to prevent thundering-herd retries, with a strictly positive
  minimum delay that never exceeds `maxDelay`.
- Honors `SyncError.retryAfter` as a **minimum** delay that local backoff
  and jitter must not shorten. A server-provided `retryAfter` larger than
  `maxDelay` is preserved — `maxDelay` caps local backoff only, not
  server-suggested delays.
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
`syncNow()` and emitted on the `reports` stream. Exactly one instance is
constructed per cycle, at cycle completion, from an internal mutable
`_CycleAccumulator`. `completedAt` is always populated. Contains counts of
fetched/succeeded/failed/conflicted/dead-lettered/storage-error
operations, cycle-level error state, and timing information.

Counter relationships:
- `fetched` counts the total number of operations returned by
  `fetchEligibleOperations` across all batch iterations in the cycle,
  **before** any filtering (attempted-operation dedup, per-entity
  selection). It represents raw adapter results, not unique operations
  selected for pushing.
- `succeeded`, `failed`, `conflicted`, and `deadLettered` are incremented
  **only after** the corresponding storage transition
  (`markSucceeded`/`markFailed`/`markDeadLetter`) completes without
  throwing.
- `conflicted` counts operations whose remote result was `SyncConflict`.
- `deadLettered` counts **every** operation transitioned to `deadLetter`,
  including conflicts, non-retryable failures, max-attempt exhaustion,
  and protocol violations. It is incremented only when `markDeadLetter`
  completes without throwing.
- Therefore `conflicted` is a **subset** of `deadLettered`.
- `storageErrors` counts operation-level storage transition failures
  (`markSucceeded`, `markFailed`, or `markDeadLetter` throwing **after**
  a push result was received). Each represents a persisted state that is
  **unknown** — the operation may or may not have been transitioned.
- Cycle-level storage failures (`fetchEligibleOperations` or
  `markInFlight` throwing) are represented by `cycleError`, not
  `storageErrors`, because they abort the cycle rather than affecting a
  single operation's post-push transition.

### `SyncConflictEvent`

Immutable value object emitted on the `conflicts` stream **after** the
operation has been durably marked `deadLetter`. Contains:
- `operation`: a **pre-transition snapshot** of the `SyncOperation` as it
  was fetched from the outbox (status was `pending` or `failed` at fetch
  time). The adapter does **not** mutate the in-memory `SyncOperation`
  instance. The event's `operation.status` reflects the pre-transition
  state, not `deadLetter`.
- `conflict`: the `SyncConflict` result (including `remoteSnapshot`).

The event is an ephemeral broadcast notification, not durable storage.
The Core contract does not provide a conflict-resolution or discard
workflow.

---

## 3. Proposed Public API

> **API staging.** The declarations below are the **final public API**
> after Phase 3.3 — they are not all present after Phase 3.1:
>
> - **Phase 3.1** ships `SyncEngine` (constructor **without**
>   `retryPolicy`, **no** `conflicts` stream), `SyncEngineConfig`, and
>   `SyncCycleReport`.
> - **Phase 3.2** adds `RetryPolicy`, `ExponentialBackoffRetryPolicy`,
>   and the `retryPolicy` constructor parameter.
> - **Phase 3.3** adds `SyncConflictEvent` and the `conflicts` stream.

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
  /// [config] — engine configuration. When `null`, a default
  ///   [SyncEngineConfig] is created internally. [SyncEngineConfig]'s
  ///   constructor performs runtime validation (`batchSize > 0`,
  ///   `maxConcurrency > 0`), so it is **not** a const constructor and
  ///   cannot be used as a default parameter value.
  /// [retryPolicy] — retry strategy (defaults to
  ///   [ExponentialBackoffRetryPolicy]). **Parameter added in
  ///   Phase 3.2** — the Phase 3.1 constructor does not include it.
  SyncEngine({
    required LocalStorageAdapter localStorage,
    required RemoteSyncAdapter remoteSync,
    required ConnectivityProvider connectivity,
    SyncEngineConfig? config,
    RetryPolicy? retryPolicy,
  });

  /// Whether [start] has completed successfully and [dispose] has not
  /// been requested.
  bool get isRunning;

  /// Stream of [SyncCycleReport]s emitted after each completed cycle.
  /// Exactly one immutable report is constructed per cycle and emitted
  /// after all storage transitions have settled. Useful for observability,
  /// logging, and UI status indicators.
  Stream<SyncCycleReport> get reports;

  /// Stream of [SyncConflictEvent]s emitted after a conflict has been
  /// durably persisted as `deadLetter`.
  ///
  /// **Added in Phase 3.3** — not present on the Phase 3.1/3.2 engine.
  ///
  /// This is an **ephemeral broadcast notification**, not durable storage.
  /// If no listener is attached when the event is emitted, the event is
  /// lost. The `remoteSnapshot` is not persisted separately and may be
  /// lost if the process crashes before the application handles the event.
  ///
  /// The [SyncConflictEvent.operation] is a **pre-transition snapshot** —
  /// its `status` field reflects the state at fetch time (e.g., `pending`
  /// or `failed`), not `deadLetter`. The adapter does not mutate the
  /// in-memory `SyncOperation` instance.
  ///
  /// The operation remains `deadLetter` in the outbox after notification
  /// (assuming `markDeadLetter` succeeded). Later operations for the same
  /// entity remain blocked. The Core contract does **not** provide a
  /// supported conflict-resolution or discard workflow. The application
  /// may inspect the dead-lettered operation and remote state manually,
  /// but no automated resolution is guaranteed.
  Stream<SyncConflictEvent> get conflicts;

  /// Starts the engine.
  ///
  /// 1. Recovers stale `inFlight` operations via
  ///    [LocalStorageAdapter.recoverInFlightOperations] **once** per
  ///    successful engine startup lifecycle.
  /// 2. Initializes connectivity monitoring: creates the event buffer,
  ///    sets `_initializing = true`, subscribes to
  ///    [ConnectivityProvider.onConnectivityChanged], then samples
  ///    [ConnectivityProvider.isConnected]. Events received during
  ///    sampling are buffered and reconciled with the sample.
  /// 3. Sets `_started = true`.
  /// 4. If [SyncEngineConfig.syncOnStart] is `true` and the device is
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
  /// If [recoverInFlightOperations] throws, the persisted state may be
  /// **partially changed** — the Core contract does not guarantee atomic
  /// recovery. A subsequent [start] retries recovery; adapters are
  /// expected to make recovery idempotent, but this is not contractually
  /// guaranteed (see Section 10).
  ///
  /// If [dispose] is called while [start] is in progress, a
  /// disposal-requested state is set synchronously before any `await`.
  /// Startup checks this state after asynchronous gaps and will not
  /// schedule new work or create subscriptions after disposal is
  /// requested.
  ///
  /// Throws [StateError] if called after [dispose] has been requested.
  Future<void> start();

  /// Manually triggers a sync cycle.
  ///
  /// Requires [start] to have completed successfully. Throws [StateError]
  /// if called before [start] or after [dispose] has been requested.
  ///
  /// If a cycle is already in progress, returns the in-flight cycle's
  /// [SyncCycleReport] future (deduplication — no overlapping cycles).
  ///
  /// **Reservation mechanism:** a private `_CycleSlot` object is created
  /// and assigned to `_activeCycleSlot` synchronously, before any
  /// `await`. The slot holds a `Completer<SyncCycleReport>`. The cycle
  /// executor completes the completer with the immutable report when
  /// finished, and clears `_activeCycleSlot` in a `finally` block only if
  /// `identical(_activeCycleSlot, slot)` — so a cycle always cleans up
  /// only its own reservation, including skipped-offline early returns.
  /// Concurrent callers share `slot.completer.future`; sequential callers
  /// (after the previous slot was cleared) each get their own cycle.
  ///
  /// If [SyncEngineConfig.syncWhenOffline] is `false` (default) and the
  /// device is offline, returns a report with `skippedOffline: true`
  /// without fetching operations.
  Future<SyncCycleReport> syncNow();

  /// Releases all resources.
  ///
  /// 1. Sets `_disposeRequested = true` **synchronously, before any
  ///    `await`**. This immediately prevents `start()` and `syncNow()`
  ///    from proceeding (they throw `StateError`), prevents pending
  ///    connectivity events and auto-triggers from starting new work,
  ///    and causes an in-progress startup to abort after its current
  ///    asynchronous gap.
  /// 2. If startup is in progress, awaits its settlement.
  /// 3. Cancels the connectivity subscription.
  /// 4. If a cycle reservation (`_activeCycleSlot`) exists, awaits its
  ///    completer — the active cycle runs to completion (in-flight pushes
  ///    finish, storage transitions settle, the report is emitted).
  ///    Does **not** impose a timeout — remote calls are not cancellable.
  /// 5. Closes internal stream controllers. Because `_disposeRequested`
  ///    was set before any `await`, no new cycle can be reserved after
  ///    step 1, and the only possible emitter (the awaited active cycle)
  ///    has completed — so no event can be emitted after close.
  ///
  /// Concurrent calls to [dispose] share one disposal Future so every
  /// caller awaits completed cleanup.
  ///
  /// Does NOT close the [LocalStorageAdapter], [RemoteSyncAdapter], or
  /// [ConnectivityProvider] — the application owns their lifecycles.
  ///
  /// Operations currently `inFlight` remain `inFlight` (or in an unknown
  /// state if a storage transition failed during the cycle) and will be
  /// recovered to `pending` on the next [start] call — subject to the
  /// atomicity caveat described for [start].
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

### `RetryPolicy` — introduced in Phase 3.2

```dart
/// Strategy for computing retry delays and dead-letter decisions.
///
/// Implementations must safely support concurrent calls within a single
/// Dart isolate. They may be stateful (e.g., for jitter RNG), but must
/// not corrupt shared state when called concurrently from multiple
/// `_pushAndHandle` futures within one sync cycle.
///
/// The engine calls [computeDelay] and [shouldDeadLetter] for each failed
/// operation.
///
/// Both methods validate that [attemptCount] is >= 1. The engine
/// guarantees this by passing `op.attemptCount + 1` (where
/// `op.attemptCount` starts at 0).
///
/// **Exception semantics:** Implementations should treat a throw from
/// either method as a programmer error, not a transient condition. The
/// engine does **not** catch policy exceptions per-operation — they
/// propagate to the cycle error boundary, which aborts the cycle with
/// `cycleError` set (see Section 8). Rationale: a broken policy affects
/// every failed operation identically, and the engine cannot determine
/// the correct storage transition without a delay/dead-letter decision.
/// Aborting is consistent with the `markInFlight` failure policy.
abstract interface class RetryPolicy {
  /// Computes the delay before the next retry attempt.
  ///
  /// [attemptCount] is the number of completed push attempts (1 after the
  /// first failure, 2 after the second, etc.). Must be >= 1.
  ///
  /// [error] is the [SyncError] that caused the failure. If
  /// [SyncError.retryAfter] is non-null, implementations **must** honor it
  /// as a minimum delay — local backoff and jitter must not produce a
  /// delay shorter than `retryAfter`. A server-provided `retryAfter`
  /// larger than any local `maxDelay` is preserved.
  ///
  /// If this method throws, the exception propagates to the cycle error
  /// boundary: the cycle aborts with `cycleError`, the operation remains
  /// `inFlight` (claimed, result received, no transition applied), and it
  /// is recovered to `pending` on the next [SyncEngine.start]. No outcome
  /// counter is incremented for it.
  Duration computeDelay(int attemptCount, SyncError error);

  /// Returns `true` if the operation should be moved to `deadLetter`
  /// instead of being scheduled for another retry.
  ///
  /// [attemptCount] is the number of completed push attempts. Must be >= 1.
  /// [error] is the [SyncError] that caused the failure.
  ///
  /// If this method throws, the exception propagates to the cycle error
  /// boundary with the same semantics as [computeDelay].
  bool shouldDeadLetter(int attemptCount, SyncError error);
}
```

### `ExponentialBackoffRetryPolicy` — introduced in Phase 3.2

```dart
/// Default [RetryPolicy] using exponential backoff with jitter.
///
/// Delay formula:
///   1. base = min(initialDelay * 2^(attemptCount - 1), maxDelay)
///      └─ local backoff cap is [maxDelay]. This cap applies to local
///         backoff only, NOT to server-provided [SyncError.retryAfter].
///   2. jittered = base * (1 - jitterFactor + random() * 2 * jitterFactor)
///      └─ with jitterFactor = 1.0, the lower bound is 0, which could
///         produce a zero delay. To prevent tight retry loops, the
///         result is clamped to a strictly positive minimum that never
///         exceeds [maxDelay]:
///         min(max(jittered, Duration(milliseconds: 1)), maxDelay)
///   3. if error.retryAfter != null:
///        return max(jittered, error.retryAfter)  ← retryAfter is minimum
///      else:
///        return jittered
///
/// `retryAfter` is a **minimum** — it is never shortened by local backoff
/// or jitter. A `retryAfter` larger than [maxDelay] is preserved because
/// [maxDelay] caps local backoff only, not server-suggested delays.
///
/// Example with `initialDelay = 1s`, `maxDelay = 5m`, `jitterFactor = 0.2`:
///   attempt 1 → ~1s   (0.8s – 1.2s)
///   attempt 2 → ~2s   (1.6s – 2.4s)
///   attempt 3 → ~4s   (3.2s – 4.8s)
///   attempt 4 → ~8s   (6.4s – 9.6s)
///   ...
///   attempt 10 → ~5m  (4m – 5m, jittered then capped at maxDelay=5m)
///
/// If error.retryAfter = 10m (> maxDelay=5m):
///   return 10m  (server suggestion preserved, not capped by maxDelay)
final class ExponentialBackoffRetryPolicy implements RetryPolicy {
  /// Creates an [ExponentialBackoffRetryPolicy].
  ///
  /// [initialDelay] — delay before the first retry. Default: 1 second.
  ///   Must be > 0.
  /// [maxDelay] — maximum delay for **local backoff** between retries.
  ///   Default: 5 minutes. Must be >= [initialDelay].
  ///   This does NOT cap server-provided [SyncError.retryAfter].
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
///
/// Exactly one instance is constructed per cycle, at cycle completion,
/// from an internal mutable accumulator. It is never mutated after
/// construction and is the same instance returned by [SyncEngine.syncNow]
/// and emitted on [SyncEngine.reports].
///
/// **Field availability:** the type ships in Phase 3.1. Counters tied to
/// later phases stay at `0` until their phase lands — `failed`,
/// `deadLettered`, and `storageErrors` from Phase 3.2, `conflicted` from
/// Phase 3.3.
final class SyncCycleReport {
  const SyncCycleReport._({
    required this.startedAt,
    required this.completedAt,
    required this.fetched,
    required this.succeeded,
    required this.failed,
    required this.conflicted,
    required this.deadLettered,
    required this.storageErrors,
    required this.cycleError,
    required this.skippedOffline,
  });

  /// When the cycle started.
  final DateTime startedAt;

  /// When the cycle completed. Always populated — the report is only
  /// constructed after the cycle has fully finished.
  final DateTime completedAt;

  /// Number of operations returned by `fetchEligibleOperations` across
  /// all batch iterations in the cycle, **before** any filtering
  /// (attempted-operation dedup or per-entity selection). This counts
  /// raw adapter results, not unique operations selected for pushing.
  final int fetched;

  /// Number of operations successfully pushed and confirmed removed
  /// from the outbox. Incremented **only** when `markSucceeded` completes
  /// without throwing. If `markSucceeded` throws, the persisted state is
  /// unknown, [storageErrors] is incremented, and this counter is not.
  final int succeeded;

  /// Number of operations that failed and were scheduled for retry.
  /// Incremented **only** when `markFailed` completes without throwing.
  /// If `markFailed` throws, [storageErrors] is incremented instead.
  final int failed;

  /// Number of operations whose remote result was [SyncConflict].
  /// This is a subset of [deadLettered]. Incremented **only** when
  /// `markDeadLetter` completes without throwing.
  final int conflicted;

  /// Number of operations transitioned to `deadLetter`, including:
  /// - Conflicts ([conflicted])
  /// - Non-retryable failures
  /// - Max-attempt exhaustion
  /// - Protocol violations (operationId mismatch)
  /// Incremented **only** when `markDeadLetter` completes without
  /// throwing. If `markDeadLetter` throws, [storageErrors] is
  /// incremented instead.
  final int deadLettered;

  /// Number of operation-level storage transition failures: a push
  /// result was received (`SyncSuccess`, `SyncFailure`, or
  /// `SyncConflict`) but the corresponding `markSucceeded`, `markFailed`,
  /// or `markDeadLetter` call threw. Each failure means the persisted
  /// state of that operation is **unknown** — it may or may not have
  /// been transitioned.
  ///
  /// Cycle-level storage failures (`fetchEligibleOperations` or
  /// `markInFlight` throwing) are NOT counted here — they set
  /// [cycleError] and abort the cycle instead.
  final int storageErrors;

  /// Cycle-level error that prevented the cycle from completing normally
  /// (e.g., `fetchEligibleOperations` threw, `markInFlight` failed).
  /// Null if the cycle completed without cycle-level errors. Individual
  /// operation failures are counted in [failed], [deadLettered], and
  /// [storageErrors], not here.
  final SyncError? cycleError;

  /// Whether the cycle was skipped because the device is offline.
  final bool skippedOffline;

  /// Duration of the sync cycle. Safe to call on any emitted report —
  /// [completedAt] is always non-null.
  Duration get duration => completedAt.difference(startedAt);

  /// Whether the cycle completed **without any failure**.
  ///
  /// Returns `false` if any of the following occurred:
  /// - [failed] > 0 (operations scheduled for retry)
  /// - [conflicted] > 0 (operations parked as deadLetter due to conflict)
  /// - [deadLettered] > 0 (operations exhausted retries, non-retryable,
  ///   or protocol violations)
  /// - [storageErrors] > 0 (operation-level storage transition failed;
  ///   persisted state unknown)
  /// - [cycleError] != null (cycle-level failure)
  ///
  /// Returns `true` only when none of the above occurred. Note:
  /// - A `skippedOffline` report returns `true` because **no failure
  ///   occurred** — but this does **NOT** mean any operation was
  ///   synchronized. A skipped cycle is a no-op.
  /// - `isSuccess == true` is **not proof that synchronization
  ///   occurred**. To determine whether operations were actually
  ///   synchronized, inspect [succeeded], [fetched], and
  ///   [skippedOffline]. A cycle that fetched zero eligible operations
  ///   also returns `isSuccess == true`.
  bool get isSuccess =>
      cycleError == null &&
      failed == 0 &&
      conflicted == 0 &&
      deadLettered == 0 &&
      storageErrors == 0;

  @override
  String toString() =>
      'SyncCycleReport(fetched: $fetched, succeeded: $succeeded, '
      'failed: $failed, conflicted: $conflicted, '
      'deadLettered: $deadLettered, storageErrors: $storageErrors, '
      'skippedOffline: $skippedOffline, cycleError: $cycleError, '
      'duration: $duration)';
}
```

`isSuccess` semantics — three distinct concepts that must not be
conflated:

| Concept | How to determine |
|---------|------------------|
| No errors occurred | `report.isSuccess` |
| Operations were actually synchronized | `report.succeeded > 0` (and `report.skippedOffline == false`) |
| Cycle was skipped (offline) | `report.skippedOffline == true` — `isSuccess` may still be `true`, meaning only "no failure occurred" |

### `SyncConflictEvent` — introduced in Phase 3.3

```dart
/// Emitted on [SyncEngine.conflicts] after a conflict has been durably
/// persisted as `deadLetter`.
///
/// The event is emitted only after [LocalStorageAdapter.markDeadLetter]
/// has completed without throwing. If the storage transition throws, the
/// persisted state is **unknown** (the operation may or may not have been
/// dead-lettered), [SyncCycleReport.storageErrors] is incremented, no
/// event is emitted, and the error is logged.
///
/// [SyncConflictEvent.operation] is a **pre-transition snapshot** of the
/// `SyncOperation` as it was fetched from the outbox. Its `status` field
/// reflects the state at fetch time (e.g., `pending` or `failed`), not
/// `deadLetter`. The adapter does **not** mutate the in-memory
/// `SyncOperation` instance — the engine holds a reference to the same
/// immutable object that was fetched.
///
/// Delivery semantics: the event is added to a broadcast stream. If no
/// listener is attached at the time of emission, the event is lost. The
/// `remoteSnapshot` is not persisted separately — if the process crashes
/// before the application handles the event, the snapshot may be lost.
///
/// The operation remains `deadLetter` in the outbox after notification
/// (assuming `markDeadLetter` succeeded). Later operations for the same
/// entity remain blocked. The Core contract does **not** provide a
/// supported conflict-resolution or discard workflow.
/// [LocalStorageAdapter.markSucceeded] is semantically incorrect as a
/// conflict-resolution API (it means "remote accepted", not "discard
/// conflict") and must not be used as one.
final class SyncConflictEvent {
  const SyncConflictEvent({
    required this.operation,
    required this.conflict,
  });

  /// Pre-transition snapshot of the operation that encountered the
  /// conflict. Its `status` reflects the fetch-time state, not
  /// `deadLetter`.
  final SyncOperation operation;

  /// The conflict result from the remote adapter, including
  /// [SyncConflict.remoteSnapshot].
  final SyncConflict conflict;
}
```

### Internal `_CycleAccumulator` (not public API)

The engine uses a private mutable accumulator during cycle execution so
that `SyncCycleReport` can remain immutable:

```dart
/// Private mutable accumulator — never exposed publicly.
final class _CycleAccumulator {
  _CycleAccumulator({required this.startedAt, required this.eligibilityCutoff});

  final DateTime startedAt;

  /// Fixed cycle-wide eligibility cutoff — **Phase 3.2**. In Phase 3.1,
  /// each `fetchEligibleOperations` call simply passes the current time;
  /// the captured cutoff and its reuse across fetches arrive with retry
  /// scheduling in Phase 3.2.
  final DateTime eligibilityCutoff;

  int fetched = 0;
  int succeeded = 0;

  /// Populated from Phase 3.2 (failure handling).
  int failed = 0;

  /// Populated from Phase 3.3 (conflict handling).
  int conflicted = 0;

  /// Populated from Phase 3.2 (dead-letter decisions).
  int deadLettered = 0;

  /// Populated from Phase 3.2 (operation-level storage failure reporting).
  int storageErrors = 0;
  SyncError? cycleError;
  bool skippedOffline = false;

  /// Constructs the single immutable [SyncCycleReport] for this cycle.
  /// Called exactly once, at cycle completion, on every exit path
  /// (normal, skipped-offline, cycle error, unexpected exception).
  SyncCycleReport build(DateTime completedAt) => SyncCycleReport._(
        startedAt: startedAt,
        completedAt: completedAt,
        fetched: fetched,
        succeeded: succeeded,
        failed: failed,
        conflicted: conflicted,
        deadLettered: deadLettered,
        storageErrors: storageErrors,
        cycleError: cycleError,
        skippedOffline: skippedOffline,
      );
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
| `SyncConflictEvent` | Conflict notification — **public type introduced in Phase 3.3**, exported from the package barrel; constructor is public | `operation`, `conflict` |

**No Core API changes required.** All types use existing contracts.

---

## 4. Sync Cycle Sequence

This section is the **single authoritative description** of the
**target-state** engine startup, cycle reservation, cycle execution, and
result handling — the complete algorithm after Phase 3.4. Section 9
references this section and covers only disposal, lifecycle edge cases,
and connectivity behavior.

**Phase legend:** `[3.1]` engine foundation · `[3.2]` retry and failure
handling · `[3.3]` conflict handling · `[3.4]` per-entity ordering guard
and entity blocking. Unmarked steps are Phase 3.1 foundation. Steps
marked with a later phase are **not implemented until that phase** —
the algorithm shown is the final target, not the Phase 3.1 subset.

### Startup (`start()`) — Phase 3.1

```
start()
  ├─ if _disposeRequested → throw StateError("SyncEngine is disposed")
  ├─ if _started → return  (idempotent — already running)
  ├─ if _startupFuture != null → return _startupFuture  (share one startup)
  ├─ _startupFuture = _performStartup()
  └─ return _startupFuture

_performStartup():
  ├─ try {
  │     ├─ // Step 1: Recover in-flight operations (once)
  │     ├─ await localStorage.recoverInFlightOperations()
  │     │   └─ if _disposeRequested → cleanup, return
  │     │   └─ if throws → cleanup, rethrow
  │     │      NOTE: persisted state may be PARTIALLY changed — the
  │     │      Core contract does not guarantee atomic recovery.
  │     │
  │     ├─ // Step 2: Initialize connectivity monitoring
  │     ├─ _pendingConnectivityEvents = <bool>[]  ← buffer FIRST
  │     ├─ _initializing = true                   ← set BEFORE subscribing
  │     ├─ _connectivitySub = connectivity.onConnectivityChanged.listen(
  │     │     (online) {
  │     │       if (_disposeRequested) return
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
  │     ├─ if _disposeRequested → cleanup, return
  │     │
  │     ├─ // Step 3: Sample initial connectivity
  │     ├─ sampledConnected = await connectivity.isConnected
  │     │   └─ if _disposeRequested → cleanup, return
  │     │   └─ if throws → cleanup, rethrow
  │     │
  │     ├─ _initializing = false
  │     ├─ _isConnected = sampledConnected
  │     ├─ _wasConnected = sampledConnected
  │     │
  │     ├─ // Step 4: Reconcile buffered events
  │     ├─ if (_pendingConnectivityEvents.isNotEmpty) {
  │     │     final latest = _pendingConnectivityEvents.last
  │     │     _pendingConnectivityEvents.clear()
  │     │     _isConnected = latest
  │     │     _wasConnected = latest
  │     │   }
  │     │
  │     ├─ if _disposeRequested → cleanup, return
  │     │
  │     ├─ // Step 5: Mark started and schedule auto-sync
  │     ├─ _started = true  ← BEFORE scheduling auto-sync
  │     │
  │     ├─ if (config.syncOnStart && _isConnected && !_disposeRequested) {
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

**Failed startup:** If any step throws, `_startupFuture` is cleared,
`_started` remains `false`, partial resources (connectivity subscription,
buffers) are cleaned up, and the exception propagates to the caller. A
subsequent `start()` call will create a new `_startupFuture` and retry
from the beginning — including re-running `recoverInFlightOperations()`.
Adapters are expected to make recovery idempotent, but the Core contract
does not guarantee atomic recovery: if recovery partially succeeded
before throwing, some operations may already be `pending` while others
remain `inFlight`. The next recovery attempt will reset the remaining
`inFlight` operations. No sync cycle can begin before startup completes
because `_started` is only set to `true` after all startup steps succeed.

**`syncOnStart` ordering:** `_started = true` is set **before**
`_triggerAutoSync()` is scheduled. This ensures the auto-trigger's call
to `syncNow()` passes the `_started` check. The auto-trigger cannot run
while startup is incomplete because it is scheduled after `_started` is
set.

**Initial connectivity sampling:** `_initializing = true` and the event
buffer are initialized **before** subscribing. The subscription is
established **before** sampling `isConnected`. Both synchronous and
asynchronous events received during sampling are buffered (the listener
checks `_initializing` which is already `true`). After sampling
completes, `_initializing` is set to `false`, and the last buffered event
(if any) overrides the sample. This prevents an older sample from
overwriting a newer event.

**Disposal during startup:** `_disposeRequested` is checked after every
asynchronous gap. If disposal is requested, startup cleans up and
returns without setting `_started` or scheduling auto-sync.

### Cycle reservation (`syncNow()`) — Phase 3.1

```
syncNow()
  ├─ if _disposeRequested → throw StateError("SyncEngine is disposed")
  ├─ if !_started → throw StateError("SyncEngine.start() must complete first")
  ├─ existing = _activeCycleSlot
  ├─ if existing != null → return existing.completer.future  (share cycle)
  │
  ├─ // Synchronous reservation — no async gap before assignment
  ├─ slot = _CycleSlot()               // holds Completer<SyncCycleReport>
  ├─ _activeCycleSlot = slot           // reservation token
  ├─ unawaited(_executeCycle(slot))    // never throws; completes completer
  └─ return slot.completer.future
```

`_CycleSlot` is a private final class:

```dart
final class _CycleSlot {
  final Completer<SyncCycleReport> completer = Completer<SyncCycleReport>();
}
```

**Why a reservation token instead of comparing Futures:** The previous
design compared `_activeCycle` against the Future returned by `_runCycle()`
inside its own `finally` block, which relied on undefined timing (whether
the Future's `finally` runs before or after the assignment). The slot
object is created and assigned **synchronously** in `syncNow()` before any
`await`, so its identity is known to `_executeCycle` from the start. Every
exit path — normal completion, skipped-offline, cycle error, unexpected
exception — funnels through a single `finally` that clears
`_activeCycleSlot` only when `identical(_activeCycleSlot, slot)`.

**Guarantees:**
- **Reservation before async gap:** `_activeCycleSlot = slot` executes
  synchronously; Dart's single-threaded event loop ensures no other
  caller can interleave between the null check and the assignment.
- **Every exit path cleans up:** the `finally` in `_executeCycle` runs on
  all paths, including skipped-offline early returns.
- **Only own reservation cleared:** `identical(_activeCycleSlot, slot)`
  prevents a stale cycle from clearing a newer reservation.
- **Offline early returns create separate cycles:** a skipped-offline
  cycle completes its completer and clears its slot in `finally` before
  the next `syncNow()` call's synchronous body runs, so two sequential
  offline `syncNow()` calls each get their own cycle and report.
- **Concurrent calls share one cycle:** the second caller sees
  `_activeCycleSlot != null` and returns `slot.completer.future`.
- **No reliance on Future timing:** the completer exists before the cycle
  starts; there is no window where the cycle could complete before its
  "Future" is assigned.

### Cycle execution (`_executeCycle(slot)`) — mixed; steps annotated inline

```
_executeCycle(slot):
  ├─ accumulator = _CycleAccumulator(
  │     startedAt: clock.now(),
  │     // [3.2] captured once; reused for every fetch this cycle.
  │     // [3.1] fetches pass clock.now() per call instead.
  │     eligibilityCutoff: clock.now(),
  │   )
  ├─ attemptedOperationIds = <String>{}   ← same-cycle retry guard: [3.2]
  ├─ entityBlocked = <String>{}           ← entity outcomes: [3.4]
  │
  ├─ try {                              ← OUTER try: guarantees cleanup
  │     try {                          ← INNER try: captures cycle errors
  │       if (!config.syncWhenOffline && !_isConnected) {
  │         accumulator.skippedOffline = true
  │       } else {
  │       loop:
  │         a. operations = await localStorage.fetchEligibleOperations(
  │              // [3.2] now: accumulator.eligibilityCutoff (fixed
  │              //      cycle-wide cutoff). [3.1] now: clock.now()
  │              //      (current time per fetch; no fixed cutoff).
  │              now: accumulator.eligibilityCutoff,
  │              limit: config.batchSize,
  │              preservePerEntityOrder: true,
  │            )
  │            └─ if throws → accumulator.cycleError = SyncError(storage,...), break loop
  │
  │         b. if operations.isEmpty → break loop
  │
  │         c. accumulator.fetched += operations.length  ← raw results
  │
  │         d. // [3.2] Filter ops already attempted this cycle
  │            //     (same-cycle retry prevention)
  │            operations = operations.where(
  │              (op) => !attemptedOperationIds.contains(op.operationId)
  │            ).toList()
  │            if (operations.isEmpty) → break loop
  │
  │         e. // [3.4] Per-entity ordering guard:
  │            // 1. Group by entity using collision-safe key
  │            // 2. Skip entities already blocked this cycle
  │            // 3. Sort each group by (sequenceNumber, createdAt, operationId)
  │            // 4. Select only the earliest per entity
  │            final byEntity = <String, List<SyncOperation>>{}
  │            for (final op in operations) {
  │              final key = jsonEncode([op.entityType, op.entityId])
  │              if (entityBlocked.contains(key)) continue
  │              byEntity.putIfAbsent(key, () => []).add(op)
  │            }
  │
  │            final selectedOps = <SyncOperation>[]
  │            for (final entry in byEntity.entries) {
  │              final group = entry.value
  │              group.sort((a, b) {
  │                final seqA = a.sequenceNumber ?? 0
  │                final seqB = b.sequenceNumber ?? 0
  │                if (seqA != seqB) return seqA.compareTo(seqB)
  │                final createdCompare = a.createdAt.compareTo(b.createdAt)
  │                if (createdCompare != 0) return createdCompare
  │                return a.operationId.compareTo(b.operationId)
  │              })
  │              selectedOps.add(group.first)  ← earliest per entity
  │            }
  │
  │         f. for each chunk of maxConcurrency from selectedOps:
  │            i.   // Claim ALL ops in chunk BEFORE any push
  │                 // If ANY markInFlight fails, abort cycle — do NOT
  │                 // push any op in this chunk
  │                 final chunkClaims = <SyncOperation>[]
  │                 var claimFailed = false
  │                 for (final op in chunk) {
  │                   try {
  │                     await localStorage.markInFlight(op.operationId)
  │                     chunkClaims.add(op)
  │                     attemptedOperationIds.add(op.operationId)  ← [3.2]
  │                   } catch (e) {
  │                     // Storage transition failed — persisted state UNKNOWN.
  │                     // The operation may or may not have been claimed.
  │                     // Abort cycle; previously claimed ops in this chunk
  │                     // remain inFlight (or unknown) and will be
  │                     // recovered on next start.
  │                     accumulator.cycleError = SyncError(
  │                       code: SyncErrorCode.storage,
  │                       message: 'markInFlight failed (state unknown): $e',
  │                       isRetryable: true,
  │                     )
  │                     claimFailed = true
  │                     break
  │                   }
  │                 }
  │                 if (claimFailed) break loop  ← abort entire cycle
  │
  │            ii.  // All claims succeeded — now push concurrently
  │                 await Future.wait(
  │                   chunkClaims.map((op) => _pushAndHandle(
  │                     op, accumulator, entityBlocked,
  │                   )),
  │                 )
  │
  │         g. if operations.length < config.batchSize → break loop
  │       }                                   ← end else / loop
  │     } catch (e, st) {
  │       // Unexpected cycle-level failure
  │       accumulator.cycleError ??= SyncError(
  │         code: SyncErrorCode.unknown,
  │         message: 'Unexpected cycle error: $e',
  │         isRetryable: true,
  │         cause: e,
  │         stackTrace: st,
  │       )
  │     }
  │
  │     // Build the single immutable report — every exit path reaches here
  │     final report = accumulator.build(clock.now())
  │
  │     // Emit report BEFORE completing the completer. Guarded: the
  │     // dispose-ordering invariant makes a closed controller impossible
  │     // here, but a failure must not prevent completer completion.
  │     try {
  │       _reportController.add(report)
  │     } catch (_) {
  │       // Unreachable per dispose ordering; swallow so the completer
  │       // below is always completed exactly once.
  │     }
  │
  │     slot.completer.complete(report)   ← exactly once, always reached
  │
  │   } finally {
  │     // SINGLE cleanup path — clears only THIS cycle's reservation
  │     if (identical(_activeCycleSlot, slot)) {
  │       _activeCycleSlot = null
  │     }
  │   }
```

**Report construction:** `_CycleAccumulator` is mutable during execution.
Exactly one immutable `SyncCycleReport` is built at the end via
`accumulator.build(clock.now())`, which always populates `completedAt`.
The same instance is emitted on `reports` and completes the slot's
completer, so `syncNow()` awaiters and stream listeners see the identical
object. No mutable report is ever exposed.

**Ordering of emit vs complete:** The report is added to
`_reportController` before `slot.completer.complete(report)`. Stream
`add` is synchronous into the controller's buffer; completer listeners
run as microtasks after `complete()`. The `finally` block clears the slot
synchronously after `complete()` returns, before any listener resumes —
so a caller that does `await engine.syncNow()` followed immediately by
another `syncNow()` deterministically gets a new cycle.

**`_reportController.add` safety:** `dispose()` awaits the active slot's
completer before closing controllers, and `_disposeRequested` prevents
any new reservation after disposal begins. A cycle can therefore never
emit after its controller is closed.

**`_executeCycle` never throws:** Every fallible operation — all storage
and remote calls (inner `try`/`catch`), the stream emit (guarded
`try`/`catch`), and `Completer.complete` (cannot throw on first
completion; `build` is a pure constructor call) — is either captured or
non-throwing. The outer `finally` always runs. Therefore the
`unawaited(_executeCycle(slot))` invocation in `syncNow()` can never
produce an unhandled asynchronous error, and `slot.completer` is
completed exactly once on every path — `syncNow()` awaiters can never
hang.

### `_triggerAutoSync()` — Phase 3.1

```
_triggerAutoSync()
  ├─ if _disposeRequested || !_started → return
  ├─ try {
  │     await syncNow()
  │   } catch (e) {
  │     log warning  ← prevent unhandled async error
  │     // Includes StateError from syncNow() if disposal was requested
  │     // between this guard check and the syncNow() call.
  │   }
```

**Auto-trigger/dispose race — exact guard strategy:**

1. `dispose()` sets `_disposeRequested = true` **synchronously** before
   any `await`. From that instant:
   - `syncNow()` throws `StateError` at its first check — no new slot can
     be reserved.
   - `_triggerAutoSync()` returns immediately at its guard check.
   - The connectivity listener ignores events (`if (_disposeRequested)
     return`).
   - An in-progress startup aborts after its current `await`.
2. A pending auto-trigger falls into exactly one of three cases:
   - **Has not yet called `syncNow()`:** hits the `_disposeRequested`
     guard in `_triggerAutoSync` or in `syncNow` → returns/throws →
     caught and logged. No work starts.
   - **Already inside `syncNow()` awaiting an existing slot:** that slot
     is the active cycle; `dispose()` awaits its completer before closing
     streams. The trigger's own continuation does nothing after the
     await.
   - **Already reserved a new slot:** impossible — reservation requires
     passing the `_disposeRequested` check, which now fails.
3. `dispose()` awaits `_activeCycleSlot?.completer.future` (if a slot
   exists) **before** closing `_reportController` and
   `_conflictController`. Since no new reservation can be created after
   `_disposeRequested` is set, and the only report/conflict emitter is
   the awaited cycle, no event can be added to a closed controller.

Auto-triggers call `syncNow()` which handles deduplication. Errors
(including `StateError` from disposal races) are caught and logged to
prevent unhandled asynchronous errors.

### `_pushAndHandle(operation, accumulator, entityBlocked)` — mixed; branches annotated inline (`entityBlocked` parameter is used from Phase 3.4)

```
_pushAndHandle(op, acc, entityBlocked):
  ├─ entityKey = jsonEncode([op.entityType, op.entityId])   ← [3.4]
  │
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
  ├─ // operationId validation — staged behavior:
  │   //
  │   // [3.1 INTERIM] On mismatch, NEVER call markSucceeded (or any
  │   //   outcome transition). Treat as a cycle-level protocol error:
  │   //   throw to the cycle error boundary → cycleError set → cycle
  │   //   aborts after the current chunk settles. The claimed op stays
  │   //   inFlight → recovered to pending on next start. No outcome
  │   //   counter is incremented for it.
  │   //
  │   // [3.2 FINAL] On mismatch, treat as a non-retryable protocol
  │   //   violation → markDeadLetter; deadLettered++ only after the
  │   //   transition succeeds, storageErrors++ if it throws. Never
  │   //   markSucceeded/markFailed. (Pseudocode below shows FINAL.)
  │   if (result.operationId != op.operationId) {
  │     // Protocol violation — immediate dead-letter, NOT markFailed
  │     entityBlocked.add(entityKey)  ← block entity for rest of cycle [3.4]
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
  │       acc.deadLettered++        ← only after successful transition
  │     } catch (e) {
  │       // Storage transition failed — persisted state UNKNOWN
  │       acc.storageErrors++
  │       log warning
  │     }
  │     return
  │   }
  │
  ├─ switch (result) {
  │     // [3.1] success path; [3.2] storageErrors reporting on
  │     // transition failure; [3.4] entity blocking
  │     case SyncSuccess:
  │       try {
  │         await localStorage.markSucceeded(op.operationId)
  │         acc.succeeded++          ← only after successful transition
  │         // Entity NOT blocked — next op for this entity may be
  │         // considered in a later fetch within this cycle
  │       } catch (e) {
  │         // Storage transition failed — persisted state UNKNOWN
  │         acc.storageErrors++
  │         entityBlocked.add(entityKey)
  │         log warning
  │       }
  │
  │     // [3.2] retry scheduling / dead-letter decisions / policy
  │     // exception propagation; [3.4] entity blocking
  │     case SyncFailure:
  │       final error = result.error
  │       final newAttemptCount = op.attemptCount + 1
  │       entityBlocked.add(entityKey)  ← block entity for rest of cycle
  │       // NOTE: retryPolicy calls are NOT wrapped in try/catch.
  │       // A throw propagates through Future.wait to the cycle error
  │       // boundary → cycleError set, cycle aborts after the current
  │       // chunk's pushes settle. This op remains inFlight (claimed,
  │       // result received, no transition applied) → recovered to
  │       // pending on next start. No outcome counter is incremented.
  │       if (retryPolicy.shouldDeadLetter(newAttemptCount, error)) {
  │         try {
  │           await localStorage.markDeadLetter(
  │             op.operationId,
  │             attemptCount: newAttemptCount,
  │             error: error,
  │           )
  │           acc.deadLettered++      ← only after successful transition
  │         } catch (e) {
  │           // Storage transition failed — persisted state UNKNOWN
  │           acc.storageErrors++
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
  │           acc.failed++            ← only after successful transition
  │         } catch (e) {
  │           // Storage transition failed — persisted state UNKNOWN
  │           acc.storageErrors++
  │           log warning
  │         }
  │       }
  │
  │     // [3.3] conflict → dead-letter + event emission;
  │     // [3.4] entity blocking
  │     case SyncConflict:
  │       final error = SyncError(
  │         code: SyncErrorCode.conflict,
  │         message: 'Remote conflict: ${result.remoteVersion ?? "unknown"}',
  │         isRetryable: false,
  │       )
  │       entityBlocked.add(entityKey)  ← block entity for rest of cycle
  │       try {
  │         await localStorage.markDeadLetter(
  │           op.operationId,
  │           attemptCount: op.attemptCount + 1,
  │           error: error,
  │         )
  │         acc.deadLettered++        ← only after successful transition
  │         acc.conflicted++           ← subset of deadLettered
  │         // Emit event ONLY after markDeadLetter completes without throwing
  │         // operation is a PRE-TRANSITION snapshot (status at fetch time)
  │         _conflictController.add(SyncConflictEvent(
  │           operation: op,
  │           conflict: result,
  │         ))
  │       } catch (e) {
  │         // markDeadLetter threw — persisted state UNKNOWN.
  │         // No event emitted. Entity remains blocked for this cycle.
  │         acc.storageErrors++
  │         log warning
  │       }
  │   }
```

### Key properties

- **Recovery once:** `recoverInFlightOperations()` runs only during
  `start()`, not during sync cycles. This prevents resetting `inFlight`
  operations belonging to another active engine instance during manual
  syncs. If recovery throws, persisted state may be partially changed —
  the contract does not guarantee atomic recovery (Section 10).
- **Idempotent start:** If `_started` is already `true`, `start()`
  returns immediately. Recovery executes only once per successful
  startup lifecycle.
- **Single-flight via reservation token:** `_CycleSlot` is created and
  assigned synchronously before any `await`. No race window, no reliance
  on Future identity or async timing.
- **Guaranteed cleanup:** A single `finally` block clears
  `_activeCycleSlot` on every exit path — including skipped-offline —
  and only when `identical(_activeCycleSlot, slot)`.
- **Immutable reports:** One `_CycleAccumulator` per cycle; exactly one
  `SyncCycleReport` built at completion with `completedAt` populated.
- **Storage failures are reported** `[3.2]`: Every post-push storage
  transition failure increments `storageErrors`, making `isSuccess`
  false. The persisted state is documented as unknown.
- **Fixed eligibility cutoff** `[3.2]`: `eligibilityCutoff` is captured
  at cycle start and used for all `fetchEligibleOperations` calls. This
  ensures an operation whose `nextRetryAt` becomes eligible during the
  cycle is NOT fetched again in the same cycle — preventing same-cycle
  retries.
- **Per-entity ordering guard** `[3.4]`: Uses `jsonEncode([entityType,
  entityId])` for collision-safe keys. Groups operations by entity,
  sorts each group by `(sequenceNumber, createdAt, operationId)`, and
  selects only the earliest per entity. Entities blocked by a previous
  failure/conflict/storage-error in the same cycle are skipped.
- **`markInFlight` failure aborts cycle** `[3.1]`: If any `markInFlight`
  in a chunk fails, the cycle aborts with `cycleError` set. No remote
  pushes are started for that chunk. Previously claimed operations in
  the chunk remain `inFlight` or in an unknown state and will be
  recovered on next `start()`.
- **All chunk claims before pushes** `[3.1]`: All `markInFlight` calls
  for a chunk must succeed before any `remoteSync.push` is called. This
  prevents pushing operations that were not successfully claimed.
- **All chunk operations awaited** `[3.1]`: `Future.wait` ensures every
  concurrent push in a chunk completes before the next chunk starts.
- **Entity blocking across cycle** `[3.4]`: After a failure, conflict,
  dead-lettering, or failed storage transition, the entity is added to
  `entityBlocked` and no later operations for that entity are considered
  for the remainder of the cycle. After `markSucceeded` succeeds, the
  entity is NOT blocked — the next operation for that entity may be
  fetched in a later batch iteration.
- **Result validation** `[3.1]` / dead-letter transition `[3.2]`: Every
  `SyncResult` is checked against the pushed operation's `operationId`
  before any state transition — `markSucceeded` is never called for a
  mismatched result. **Phase 3.1 interim:** a mismatch is a cycle-level
  protocol error that aborts the cycle; the claimed op stays `inFlight`
  for recovery and no outcome counter is incremented. **Phase 3.2
  final:** a mismatch causes immediate `markDeadLetter` (non-retryable
  protocol violation), never `markFailed`; `deadLettered` increments
  only if the transition succeeds, otherwise `storageErrors` increments.
- **Conflict ordering** `[3.3]`: `markDeadLetter` completes without
  throwing **before** the conflict event is emitted. The event's
  `operation` is a pre-transition snapshot. A `markDeadLetter` failure
  increments `storageErrors`, emits no event, and leaves persisted state
  unknown.

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
per-entity ordering guard.

### Engine-level per-entity ordering guard — target behavior, Phase 3.4

Regardless of what the adapter returns, the engine ensures that two
operations for the same `(entityType, entityId)` are never pushed
concurrently within one cycle, and that entity outcomes are tracked
across the whole cycle:

1. **Collision-safe entity keys:** The engine uses
   `jsonEncode([entityType, entityId])` to construct entity keys. This
   prevents collisions when `entityType` or `entityId` contain delimiter
   characters (e.g., `:`).

2. **Group and sort:** After fetching a batch, the engine:
   - Filters out operations whose `operationId` is in
     `attemptedOperationIds`.
   - Groups remaining operations by entity key.
   - Skips entities already in `entityBlocked` (blocked by a previous
     failure, conflict, dead-lettering, or failed storage transition in
     this cycle).
   - Sorts each group by `(sequenceNumber ?? 0, createdAt,
     operationId)` ascending — a deterministic tiebreak.
   - Selects only the **first** (earliest) operation per entity.

3. **Chunk claiming:** All selected operations are chunked by
   `maxConcurrency`. For each chunk, all `markInFlight` calls must
   succeed before any push begins. If any claim fails, the cycle aborts.

4. **Entity outcome tracking:** After each push completes:
   - `SyncSuccess` + `markSucceeded` succeeds → entity is **not** blocked.
     The next operation for this entity may be eligible in a later
     batch fetch within the same cycle.
   - `SyncFailure` (retryable or dead-letter) → entity is **blocked** for
     the rest of the cycle.
   - `SyncConflict` + `markDeadLetter` succeeds → entity is **blocked**.
   - `SyncResult.operationId` mismatch → entity is **blocked**.
   - Any post-push storage transition throws → entity is **blocked**
     (persisted state is unknown; be conservative).

5. **No global FIFO claim:** Sorting a fetched batch and selecting one
   operation per entity does **not** establish global FIFO ordering
   across the entire outbox. It only prevents concurrent pushes for the
   same entity within one cycle. Global FIFO depends on adapter
   eligibility decisions, which the engine cannot repair.

### FIFO guarantees and limitations

| Aspect | Behavior | Source |
|--------|----------|--------|
| At most one eligible op per entity | **Not guaranteed** by contract or all adapters. Engine applies defensive guard. | Engine-level guard |
| No concurrent pushes for same entity | **Guaranteed** by engine-level guard within one cycle | Engine |
| Ordering within batch | `sequenceNumber` ascending, then `createdAt`, then `operationId` | Engine tiebreak |
| Ordering when timestamps equal | `operationId` ascending as final tiebreak (deterministic) | Engine tiebreak |
| `sequenceNumber` null | Treated as 0 by engine tiebreak. Adapter behavior varies (SQLite: first, Hive: 0). | Adapter-dependent + engine tiebreak |
| `failed` blocks entity | Yes — `preservePerEntityOrder` checks for any non-`succeeded` earlier operation. But "earlier" is determined by `sequence_number <`, which fails for null/equal values. | Contract documents rule; adapter may not enforce for null seq |
| `deadLetter` blocks entity | Same as `failed` — depends on sequence_number comparison. | Same limitation |
| `inFlight` blocks entity | Same as `failed` — depends on sequence_number comparison. | Same limitation |
| Entity blocked within cycle | Engine tracks `entityBlocked` set; blocks after failure/conflict/storage-error | Engine |
| Entity unblocked after success | Engine does not block entity after successful `markSucceeded` | Engine |
| Unrelated entities proceed | Yes — blocking is per `(entityType, entityId)`, not global | Contract + engine |
| Global FIFO | **Not guaranteed** — depends on adapter eligibility decisions | Limitation |

### Is `fetchEligibleOperations` sufficient for FIFO under concurrency?

**No, not universally.** The contract documents the blocking rule but
does not guarantee:
- At most one operation per entity per fetch.
- Deterministic ordering when sequence numbers are null or equal.
- Correct blocking behavior when sequence number comparisons involve
  null values.

**The engine compensates** with:
- Per-entity ordering guard (prevents concurrent pushes for same entity).
- Deterministic tiebreak for selecting earliest operation per entity.
- Fixed eligibility cutoff (prevents same-cycle retries).
- Entity blocking across the cycle (prevents later ops after failure).

**However, the engine cannot compensate for:**
- Incorrect blocking decisions by the adapter (e.g., an adapter failing
  to block a later operation because of null sequence number comparison).
- Non-deterministic ordering across batches when timestamps and sequence
  numbers are equal.
- Establishing universal FIFO without adapter/contract guarantees.

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

The remote adapter's `idempotencyKey` **may** prevent duplicate
application on the backend, but this is **not guaranteed** — it depends
on the remote adapter and backend explicitly enforcing the key. Having
an `idempotencyKey` field in the Core `SyncOperation` model does not
automatically prevent duplicate remote effects.

**Crash scenario:** If a remote push succeeds but the local
`markSucceeded` transition fails (or the process crashes before it
completes), the operation remains in an unknown state. On the next
`start()`, recovery resets it to `pending`, and it will be pushed again.
Whether this causes a duplicate remote effect depends entirely on the
remote adapter and backend's idempotency enforcement.

**Recommendation:** Run one `SyncEngine` per outbox. If multiple engines
are needed (e.g., different remote targets), use separate outboxes.

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
| Per-entity key | `jsonEncode([entityType, entityId])` — collision-safe |
| No concurrent same-entity pushes | Guaranteed by engine guard |
| Entity blocked after failure | Tracked in `entityBlocked` for rest of cycle |
| Entity unblocked after success | Not blocked; next op may be fetched |
| FIFO | Per-entity guard prevents concurrency; global FIFO not guaranteed |
| Cross-entity | Full parallelism |
| Overlapping cycles | Prevented by synchronous `_CycleSlot` reservation |
| Same-cycle retries | Prevented by fixed eligibility cutoff |
| Cross-instance | Not protected — idempotency enforcement is adapter/backend-dependent |

---

## 6. Retry Algorithm and Examples — target behavior, Phase 3.2

### `ExponentialBackoffRetryPolicy` algorithm

```
computeDelay(attemptCount, error):
  0. Validate attemptCount >= 1, else throw ArgumentError
  1. base = min(initialDelay * 2^(attemptCount - 1), maxDelay)
     └─ local backoff cap is maxDelay. This caps LOCAL backoff only.
  2. jittered = base * (1 - jitterFactor + random() * 2 * jitterFactor)
     └─ with jitterFactor = 1.0, lower bound is 0.
        Clamp to strictly positive, never exceeding maxDelay:
        min(max(jittered, 1ms), maxDelay)
  3. if error.retryAfter != null:
        return max(jittered, error.retryAfter)
        └─ retryAfter is a MINIMUM. If retryAfter > maxDelay,
           retryAfter is preserved (maxDelay does not cap server delays).
     else:
        return jittered

shouldDeadLetter(attemptCount, error):
  0. Validate attemptCount >= 1, else throw ArgumentError
  1. If !error.isRetryable → return true
  2. If attemptCount >= maxAttempts → return true
  3. Return false
```

### Local backoff cap vs server retryAfter

`maxDelay` caps **local backoff** only. It does **not** cap
server-provided [SyncError.retryAfter]. This distinction is critical:

- Local backoff: `min(initialDelay * 2^(n-1), maxDelay)` — capped.
- Jittered local backoff: `min(max(jittered, 1ms), maxDelay)` — capped.
- Final delay: `max(jittered, retryAfter)` — `retryAfter` can exceed
  `maxDelay` because it comes from the server, not local computation.

Example: `maxDelay = 5m`, `retryAfter = 10m`:
- Local backoff would be capped at 5m.
- `max(5m_jittered, 10m) = 10m` — server suggestion preserved.

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
been claimed in the current cycle. Any operation in this set is filtered
out of subsequent fetches as a defensive measure.

### `retryAfter` semantics

`SyncError.retryAfter` is treated as a **minimum delay**:

- If `retryAfter` is **longer** than the computed local backoff → use
  `retryAfter`.
- If `retryAfter` is **shorter** than the computed local backoff → use
  the local backoff (do not shorten).
- Jitter is applied to the local backoff **before** comparing with
  `retryAfter`, so jitter cannot reduce the delay below `retryAfter`.
- `retryAfter` larger than `maxDelay` is preserved — `maxDelay` caps
  local backoff only.

Edge cases:
- `retryAfter = Duration.zero` → no effect; local backoff applies.
- `retryAfter` very large (e.g., 1 hour) → delay is 1 hour regardless of
  local backoff or `maxDelay`.
- `retryAfter` combined with `maxDelay` → `retryAfter` takes precedence
  over `maxDelay` (server knows best).

### `jitterFactor = 1.0` edge case

With `jitterFactor = 1.0`, the jitter multiplier ranges from
`1 - 1.0 = 0.0` to `1 + 1.0 = 2.0`. A multiplier of 0.0 would produce a
zero delay, creating a tight retry loop. To prevent this, the result is
clamped to a strictly positive minimum that never exceeds `maxDelay`:

```
jittered = min(max(jittered, Duration(milliseconds: 1)), maxDelay)
```

This ensures the delay is always > 0 and <= maxDelay, even at the
extreme of `jitterFactor = 1.0`.

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
  → markFailed(attemptCount: 1, nextRetryAt: T0 + ~1s) succeeds
  → acc.failed++
  → nextRetryAt > T0, so NOT eligible in this cycle

[Next cycle at T1 > T0 + 1s] Push attempt 2 → SyncFailure(network)
  → markFailed(attemptCount: 2, nextRetryAt: T1 + ~2s) succeeds
  → acc.failed++

... continues with exponential backoff across cycles ...

Push attempt 10 → SyncFailure(network, isRetryable=true)
  → shouldDeadLetter(10, error) → true (attemptCount >= maxAttempts)
  → markDeadLetter(attemptCount: 10, error: error) succeeds
  → acc.deadLettered++
```

### Example: non-retryable validation error

```
Push attempt 1 → SyncFailure(validation, isRetryable=false)
  → shouldDeadLetter(1, error) → true (!isRetryable)
  → markDeadLetter(attemptCount: 1, error: error) succeeds
  → acc.deadLettered++
```

Immediately dead-letters — no retry scheduled.

### Example: server-suggested retry delay

```
Push attempt 1 → SyncFailure(rateLimited, isRetryable=true, retryAfter=30s)
  → computeDelay: local backoff = ~1s, jittered = ~1s
                    max(~1s, 30s) = 30s
  → markFailed(attemptCount: 1, nextRetryAt: now + 30s) succeeds
  → acc.failed++

Push attempt 5 → SyncFailure(rateLimited, isRetryable=true, retryAfter=5s)
  → computeDelay: local backoff = ~16s, jittered = ~16s
                    max(~16s, 5s) = ~16s  (local backoff wins)
  → markFailed(attemptCount: 5, nextRetryAt: now + ~16s) succeeds
  → acc.failed++

Push attempt 8 → SyncFailure(rateLimited, isRetryable=true, retryAfter=10m)
  → computeDelay: local backoff capped at maxDelay=5m, jittered = ~5m
                    max(~5m, 10m) = 10m  (retryAfter > maxDelay, preserved)
  → markFailed(attemptCount: 8, nextRetryAt: now + 10m) succeeds
  → acc.failed++
```

### Example: conflict

```
Push attempt 1 → SyncConflict(remoteSnapshot: {...})
  → entityBlocked.add(entityKey)
  → markDeadLetter(attemptCount: 1, error: SyncError(conflict, ...))
     └─ MUST complete without throwing before event is emitted
  → On success: acc.deadLettered++, acc.conflicted++
     → Emit SyncConflictEvent on conflicts stream
       (operation is PRE-TRANSITION snapshot)
  → On markDeadLetter throw: acc.storageErrors++ (no event, state unknown)
  → Operation parked — blocks later ops for same entity this cycle
  → Application receives ephemeral notification; no guaranteed resolution
```

### Example: storage transition failure

```
Push attempt 1 → SyncSuccess
  → markSucceeded(op.operationId) THROWS
  → acc.storageErrors++  (succeeded NOT incremented)
  → entityBlocked.add(entityKey)
  → Persisted state UNKNOWN — op may or may not be removed
  → Report: isSuccess == false (storageErrors > 0)
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

## 7. Conflict Handling Decision — target behavior, Phase 3.3

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

1. Adds the entity key to `entityBlocked` (blocks later operations for
   this entity for the rest of the cycle).
2. Calls `markDeadLetter` with a `SyncError(code: SyncErrorCode.conflict)`.
   **This storage transition must complete without throwing before
   proceeding.**
3. **After** `markDeadLetter` completes without throwing, increments
   `acc.deadLettered` and `acc.conflicted`, then emits a
   `SyncConflictEvent` on the `conflicts` stream containing the
   pre-transition `SyncOperation` snapshot and the `SyncConflict`
   (including `remoteSnapshot`).
4. If `markDeadLetter` throws, the persisted state is **unknown** (the
   operation may or may not have been dead-lettered).
   `acc.storageErrors` is incremented, **no event is emitted**, and the
   error is logged. The entity remains blocked for the rest of the cycle.
   The operation may be recovered to `pending` on the next `start()` if
   it was not actually dead-lettered, and retried — this is safe **only
   if** the remote adapter and backend enforce idempotency.

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

### `SyncConflictEvent.operation` semantics

The `operation` field in `SyncConflictEvent` is a **pre-transition
snapshot** — it is the same immutable `SyncOperation` instance that was
fetched from the outbox and passed to `remoteSync.push`. Its `status`
field reflects the state at fetch time (e.g., `pending` or `failed`),
**not** `deadLetter`. The adapter does **not** mutate the in-memory
`SyncOperation` instance. The engine holds a reference to the same
object throughout the push and conflict handling.

To inspect the current persisted state, the application must call
`fetchEligibleOperations` or otherwise query the adapter — the event's
`operation` does not reflect post-transition state.

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

Counter legend: `succeeded`/`failed`/`deadLettered`/`conflicted` increment
**only after** the named storage transition completes without throwing.
`storageErrors` increments when a post-push storage transition throws
(persisted state unknown). `cycleError` is set for cycle-level failures
that abort the cycle.

> Rows describe **target-state** behavior (post-Phase 3.4). Retry-related
> rows apply from Phase 3.2, conflict rows from Phase 3.3, and
> per-entity/entity-blocking effects from Phase 3.4 — see Section 12.
>
> **Phase 3.1 interim exception:** the two `operationId` mismatch rows
> below describe the Phase 3.2+ final behavior. In Phase 3.1, a mismatch
> instead aborts the cycle with `cycleError` (cycle-level protocol
> error); the claimed op remains `inFlight`, no outcome counter is
> incremented, and `markDeadLetter` is not called.

| Scenario | Engine Behavior | Persisted State After | Report Counter |
|----------|----------------|----------------------|----------------|
| `SyncSuccess` + `markSucceeded` succeeds | Operation removed from outbox | Removed | `succeeded++` |
| `SyncSuccess` + `markSucceeded` throws | Log warning, block entity for cycle | **Unknown** (may or may not be removed) | `storageErrors++` |
| `SyncFailure` (retryable) + `markFailed` succeeds | Operation scheduled for retry | `failed` with `nextRetryAt` | `failed++` |
| `SyncFailure` (retryable) + `markFailed` throws | Log warning, block entity for cycle | **Unknown** | `storageErrors++` |
| `SyncFailure` (non-retryable/max) + `markDeadLetter` succeeds | Operation dead-lettered | `deadLetter` | `deadLettered++` |
| `SyncFailure` (non-retryable/max) + `markDeadLetter` throws | Log warning, block entity for cycle | **Unknown** | `storageErrors++` |
| `SyncConflict` + `markDeadLetter` succeeds | Operation dead-lettered, event emitted | `deadLetter` | `deadLettered++`, `conflicted++` |
| `SyncConflict` + `markDeadLetter` throws | Log warning, block entity, no event | **Unknown** | `storageErrors++` |
| `SyncResult.operationId` mismatch + `markDeadLetter` succeeds | Immediate dead-letter (non-retryable), block entity | `deadLetter` | `deadLettered++` |
| `SyncResult.operationId` mismatch + `markDeadLetter` throws | Block entity, log warning | **Unknown** | `storageErrors++` |
| `RemoteSyncAdapter.push` throws | Treat as `SyncFailure(unknown, isRetryable: true)` | Per retry policy | `failed++` or `deadLettered++` or `storageErrors++` |
| `retryPolicy.shouldDeadLetter` throws | Exception propagates → **cycle aborts** with `cycleError` after current chunk settles; no transition applied to this op | Op remains `inFlight` → recovered on next `start()`; other chunk ops complete normally | `cycleError` set; no outcome counter for this op |
| `retryPolicy.computeDelay` throws | Same as `shouldDeadLetter` throw | Same | `cycleError` set; no outcome counter for this op |
| `markInFlight` throws (first in chunk) | **Abort cycle** with `cycleError`, no pushes | **Unknown** for failed op; previously claimed ops remain `inFlight` or unknown | `cycleError` set |
| `markInFlight` throws (middle in chunk) | **Abort cycle**, no pushes for this chunk | **Unknown** for failed op; previously claimed ops in chunk remain `inFlight` or unknown | `cycleError` set |
| `markInFlight` throws (last in chunk) | **Abort cycle**, no pushes for this chunk | **Unknown** for failed op; all other ops in chunk remain `inFlight` or unknown | `cycleError` set |
| `fetchEligibleOperations` throws | Set `report.cycleError`, abort cycle | Unchanged | `cycleError` set |
| `recoverInFlightOperations` throws (in `start`) | `start` throws, cleanup, `_started` remains `false`; persisted state may be **partially changed** (non-atomic) | Some ops may be `pending`, others `inFlight`; retry on next `start` | — |
| Connectivity subscription fails (in `start`) | `start` throws, cleanup, `_started` remains `false` | — | — |
| Connectivity sampling fails (in `start`) | `start` throws, cleanup, `_started` remains `false` | — | — |
| Connectivity lost mid-push | Push fails with network error → retry policy | Per retry policy | `failed++` or `deadLettered++` or `storageErrors++` |
| Remote push succeeds but `markSucceeded` fails | Operation may be re-pushed on next cycle (recovery) | **Unknown** — may be `inFlight`, `pending`, or removed | `storageErrors++` |
| App terminated mid-push | OS kills process | `inFlight` → recovered on next `start()` | — |
| `dispose()` during startup | Set `_disposeRequested`, await startup cleanup, proceed with disposal | Depends on startup outcome | — |
| `dispose()` during cycle | Set `_disposeRequested`, wait for cycle to complete (no timeout), close streams | `inFlight` ops recovered on next `start()` | — |
| `dispose()` with pending auto-trigger | `_disposeRequested` blocks new reservation; pending trigger gets `StateError` (caught/logged) or awaits existing slot | — | — |
| `syncNow()` before `start()` | Throws `StateError` | — | — |
| `syncNow()` after `dispose()` requested | Throws `StateError` | — | — |
| `start()` after `dispose()` requested | Throws `StateError` | — | — |
| `start()` when already started | Returns immediately (idempotent) | — | — |
| `start()` concurrent calls | Share one `_startupFuture` | — | — |
| `syncNow()` concurrent calls | Share one `_CycleSlot.completer.future` | — | — |
| `dispose()` concurrent calls | Share one disposal Future | — | — |
| Two sequential offline `syncNow()` calls | Each creates separate `_CycleSlot`; each returns own skipped report | — | — |

### Storage failure classification

**Cycle-aborting storage failures (set `cycleError`):**
- `fetchEligibleOperations` throws → cannot determine what to push.
- `markInFlight` throws → cannot safely determine whether the operation
  was claimed. Continuing could cause the same operation to be fetched
  and pushed repeatedly in an infinite loop. No remote pushes are
  started for the chunk. These are **cycle-level** failures — they are
  represented by `cycleError`, not `storageErrors`, because they abort
  the entire cycle rather than affecting one operation's post-push
  transition.

**Operation-level storage failures (increment `storageErrors`, continue
cycle):**
- `markSucceeded` throws → persisted state **unknown**. The operation
  may or may not have been removed. Entity is blocked for rest of cycle.
  Other operations in the chunk can continue.
- `markFailed` throws → persisted state **unknown**. Entity is blocked.
- `markDeadLetter` throws → persisted state **unknown**. Entity is
  blocked. For conflicts, no event is emitted.

**Reporting rule:** `succeeded`, `failed`, `deadLettered`, and
`conflicted` increment **only** when their storage transition completes
without throwing. When a transition throws, `storageErrors` increments
instead — the counters are mutually exclusive per operation outcome.
`isSuccess` is `false` whenever `storageErrors > 0`, so a report can
never indicate success when an operation-level storage failure occurred.

**Rationale:** `markInFlight` is the claiming mechanism. If it fails,
the engine cannot know whether the operation was claimed by this engine,
another engine, or no one. Aborting is the only safe choice. Post-push
transitions (`markSucceeded`, `markFailed`, `markDeadLetter`) are safe
to continue past because the operation's persisted state is unknown and
will be handled by recovery on next `start()` if still `inFlight` — but
this does **not** guarantee the operation stays `inFlight`. The engine
reports these as `storageErrors` rather than silently continuing.

**Idempotency caveat:** Recovery and re-push are safe **only if** the
remote adapter and backend enforce idempotency keys. Having an
`idempotencyKey` field in Core does not automatically prevent duplicate
remote effects.

**Non-storage cycle-aborting faults:**
- `RetryPolicy.shouldDeadLetter` or `RetryPolicy.computeDelay` throws →
  the exception propagates through `Future.wait` (which waits for all
  chunk pushes to settle before propagating the first error) to the
  cycle error boundary → `cycleError` set, cycle aborts. The affected
  operation remains `inFlight` — it was claimed and its push result was
  received, but no storage transition was applied because the engine
  could not determine the correct one. It is recovered to `pending` on
  the next `start()` and retried. **No outcome counter is incremented**
  for it (`storageErrors` is reserved for storage transition failures,
  not policy faults). Other operations in the chunk that already
  received results complete their transitions normally. Rationale: a
  throwing policy is a programmer/configuration fault that would affect
  every failed operation identically; per-operation handling cannot
  determine the correct transition, so aborting is the only safe choice
  — consistent with the `markInFlight` failure policy.

### Key invariants

- **No operation disappears** due to an unhandled exception. Every code
  path either marks the operation or leaves it in a recoverable or
  unknown state. A failed storage transition means the persisted state
  is **unknown** — the operation may or may not have been transitioned.
- **Reports cannot claim success after storage failure.** Any
  operation-level storage transition failure increments `storageErrors`,
  which forces `isSuccess == false`.
- **`inFlight` is recoverable on `start()` only.** Recovery does not run
  during sync cycles, preventing interference with other engine instances.
  Recovery itself is not guaranteed atomic — a failed recovery may leave
  persisted state partially changed.
- **Result validation prevents cross-operation contamination.** A
  mismatched `SyncResult` never causes the wrong operation to be marked
  succeeded — `markSucceeded` is never called for it. From Phase 3.2 a
  mismatch causes immediate `markDeadLetter` (non-retryable); in the
  Phase 3.1 interim it aborts the cycle with `cycleError` instead.
- **Conflict events are emitted only after durable persistence.** If
  `markDeadLetter` throws, no event is emitted, `storageErrors` is
  incremented, and persisted state is unknown.
- **`markInFlight` failure aborts the cycle.** The engine does not
  continue fetching when it cannot safely claim operations. No remote
  pushes are started for the affected chunk. Represented by `cycleError`.
- **All chunk claims before pushes.** All `markInFlight` calls for a
  chunk must succeed before any `remoteSync.push` is called.
- **Same-cycle retries are impossible.** The fixed eligibility cutoff
  and `attemptedOperationIds` set prevent an operation from being
  attempted more than once per cycle.
- **Entity blocking is cycle-scoped.** After failure/conflict/storage-
  error, the entity is blocked for the rest of the cycle. After success,
  the entity is unblocked.
- **RetryPolicy exceptions abort the cycle, not the operation.** A throw
  from `shouldDeadLetter`/`computeDelay` propagates to the cycle error
  boundary → `cycleError`, cycle aborts after the current chunk settles.
  The affected operation remains `inFlight` and is recovered on next
  `start()` — it is retried, never silently dropped or dead-lettered by
  a policy fault.
- **Unexpected exceptions are retryable.** Conservative default — better
  to retry than to lose an operation.

---

## 9. Lifecycle and Disposal Behavior

> **Authoritative pseudocode for `start()`, `syncNow()`, cycle
> reservation, cycle execution, and result handling lives in Section 4.**
> This section covers disposal, lifecycle edge cases, and connectivity
> behavior only.

### `dispose()`

```
dispose()
  ├─ if _disposeRequested → return _disposeFuture  (idempotent + shared)
  ├─ _disposeRequested = true  ← SET SYNCHRONOUSLY, before any await
  ├─ _disposeFuture = _performDispose()
  └─ return _disposeFuture

_performDispose():
  ├─ // Step 1: Await startup settlement if in progress
  ├─ if (_startupFuture != null) {
  │     try { await _startupFuture; } catch (e) { /* startup failed */ }
  │   }
  │   // Startup checks _disposeRequested after async gaps and will
  │   // not schedule new work or create subscriptions after disposal.
  │
  ├─ // Step 2: Cancel connectivity subscription
  ├─ await _connectivitySub?.cancel()
  ├─ _connectivitySub = null
  │
  ├─ // Step 3: Wait for active cycle reservation to complete (no timeout)
  ├─ final slot = _activeCycleSlot
  ├─ if (slot != null) {
  │     await slot.completer.future
  │   }
  │   // The cycle's finally clears _activeCycleSlot; completer.future
  │   // completes with the report before that. No new slot can be
  │   // reserved after _disposeRequested was set.
  │
  ├─ // Step 4: Close stream controllers
  ├─ // Safe: no new cycle can start (syncNow throws on _disposeRequested),
  │   // the only emitter (active cycle) has completed, and pending
  │   // auto-triggers can only get StateError from syncNow (caught/logged).
  ├─ await _reportController.close()
  ├─ await _conflictController.close()
  │
  └─ // _disposeFuture is NOT cleared — engine is permanently disposed
```

**Disposal-requested state:** `_disposeRequested` is set to `true`
**synchronously** in `dispose()`, before any `await`. This is distinct
from disposal completion. The `_disposeRequested` state:
- Prevents `start()` from proceeding (throws `StateError`).
- Prevents `syncNow()` from reserving new cycles (throws `StateError`).
- Causes `_triggerAutoSync()` to return immediately at its guard check.
- Causes an in-progress `syncNow()` call that has not yet reserved to
  throw `StateError` (caught and logged by the trigger).
- Causes the connectivity listener to ignore events.
- Causes an in-progress startup to abort after its current `await`.

**No timeout on dispose:** The `RemoteSyncAdapter` contract has no
cancellation support. Imposing a timeout would risk closing stream
controllers while the active cycle can still emit events (reports,
conflicts). Instead, `dispose()` waits indefinitely for the active cycle
reservation to complete. Shutdown time is bounded only by the remote
adapter's own timeouts and the storage adapter's responsiveness.

**Disposal during startup:** If `dispose()` is called while `start()` is
in progress (e.g., `recoverInFlightOperations()` is pending),
`_disposeRequested` is set synchronously. The startup code checks
`_disposeRequested` after the `recoverInFlightOperations()` await and
will not proceed to subscribe to connectivity or schedule auto-sync.
`_performDispose` then awaits `_startupFuture` (which completes quickly
because startup aborted), cancels any partial subscription, and closes
streams.

**Deterministic test — dispose during `recoverInFlightOperations()`:**
```
1. Create engine with a mock LocalStorageAdapter whose
   recoverInFlightOperations() returns a Completer-controlled Future.
2. Call engine.start() (do not await).
3. Call engine.dispose() (do not await).
4. Verify _disposeRequested is true (synchronously after dispose() call).
5. Complete the recoverInFlightOperations() Completer.
6. Await both start() and dispose().
7. Verify start() completed without setting _started (startup aborted).
8. Verify dispose() completed: subscription cancelled, streams closed.
9. Verify no connectivity subscription was created.
10. Verify no sync cycle was started (no _CycleSlot reserved).
```

**Deterministic test — two sequential offline `syncNow()` calls:**
```
1. Start engine with syncWhenOffline=false and isConnected=false.
2. report1 = await engine.syncNow()  → skippedOffline report, slot1 cleared
3. report2 = await engine.syncNow()  → separate skippedOffline report
4. Verify report1 and report2 are distinct instances.
5. Verify each has skippedOffline=true, isSuccess=true, fetched=0.
6. Verify no overlap: the second call did not share the first's future
   (proven by distinct report instances and separate cycle execution).
```

**Concurrent dispose:** The `_disposeFuture` ensures all concurrent
`dispose()` calls share one disposal operation. Every caller awaits the
same completed cleanup. `_disposeRequested` is checked first for
idempotency — if already true, return the existing `_disposeFuture`.

### Disposal during an active cycle

- `_disposeRequested` is set to `true` synchronously, preventing new
  `syncNow()` calls from reserving new cycles.
- The active cycle continues to completion — in-flight pushes finish,
  storage transitions settle (incrementing counters or `storageErrors`),
  the immutable report is built, emitted, and used to complete the slot's
  completer.
- The cycle's `finally` block clears `_activeCycleSlot` (only if
  `identical`).
- `dispose()` awaits `slot.completer.future`, then closes stream
  controllers — ensuring no events are emitted after close.
- Operations currently `inFlight` remain `inFlight` (or in an unknown
  state if a storage transition failed) and are recovered on the next
  `start()` call — subject to the atomicity caveat for recovery.

### Auto-trigger pending at dispose time

Covered in Section 4 (`_triggerAutoSync` guard strategy). Summary:
`_disposeRequested` is set before any `await`, so any trigger that has
not yet reserved a cycle will fail the `syncNow()` guard and be caught;
any trigger sharing an existing cycle is covered by `dispose()` awaiting
that slot's completer before closing streams.

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
| Connectivity event during `_disposeRequested` | Ignored |
| Auto-trigger during `_disposeRequested` | Returns immediately or gets `StateError` from `syncNow` (caught/logged) |

### Initial connectivity sampling and reconciliation

1. `_pendingConnectivityEvents = <bool>[]` is initialized and
   `_initializing = true` is set **before** subscribing.
2. `start()` subscribes to `onConnectivityChanged`. The listener checks
   `_initializing` (already `true`) and buffers all events — both
   synchronous and asynchronous.
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
- **Cycle mutex** (`_CycleSlot`) prevents overlapping cycles from
  stacking up.
- **Auto-trigger errors** are caught and logged, preventing unhandled
  async errors from crashing the app.

---

## 10. Identified Limitations and Trade-offs

### 1. No atomic claiming across engine instances

`fetchEligibleOperations` + `markInFlight` is not atomic. Two engine
instances can fetch and push the same operations. Mitigated by:
- Remote adapter idempotency keys **may** prevent duplicate application,
  but this depends on the remote adapter and backend explicitly
  enforcing the key. Having an `idempotencyKey` field in Core does not
  automatically prevent duplicate remote effects.
- Recommendation: one engine per outbox.

**Crash scenario:** If a remote push succeeds but the local
`markSucceeded` transition fails (or the process crashes before it
completes), the operation remains in an unknown state. On the next
`start()`, recovery resets it to `pending`, and it will be pushed again.
Whether this causes a duplicate remote effect depends entirely on the
remote adapter and backend's idempotency enforcement.

**Future enhancement:** Add `claimOperations` or atomic status transition
to `LocalStorageAdapter` contract.

### 2. FIFO guarantees depend on adapter implementation

The Core contract documents the blocking rule but does not guarantee:
- At most one operation per entity per fetch.
- Deterministic ordering when sequence numbers are null or equal.
- Correct blocking behavior when sequence number comparisons involve
  null values.

The engine applies a defensive per-entity guard, but cannot compensate
for incorrect adapter-level blocking decisions or establish universal
FIFO without adapter/contract guarantees.

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

If `markInFlight` fails for any operation in a chunk, the entire cycle
aborts with `cycleError`. No remote pushes are started for that chunk.
Previously claimed operations in the chunk remain `inFlight` or in an
unknown state and will be recovered on next `start()`. The persisted
state of the failed claim is **unknown**.

**Trade-off:** Safety over throughput. The alternative (continuing after
`markInFlight` failure) risks infinite loops and duplicate pushes.

### 12. Storage transition failures leave persisted state unknown

When a storage method (e.g., `markSucceeded`, `markFailed`,
`markDeadLetter`) throws, the engine cannot determine whether the
transition was persisted. The persisted state is **unknown** — the
operation may or may not have been transitioned. The engine blocks the
entity for the rest of the cycle, increments `storageErrors` (forcing
`isSuccess == false`), and relies on recovery on the next `start()` if
the operation remains `inFlight`.

**Mitigation:** Recovery on `start()` resets `inFlight` operations to
`pending`. Operations in other states (e.g., `failed`, `deadLetter`)
are not recovered. The engine does not claim that post-push storage
failures always leave an operation `inFlight`.

### 13. `recoverInFlightOperations` is not guaranteed atomic

If `recoverInFlightOperations()` throws partway through, some operations
may already have been transitioned from `inFlight` to `pending` while
others remain `inFlight`. The Core contract does not specify whether
recovery is atomic. The engine treats the persisted state as potentially
partially changed and retries the full startup (including recovery) on
the next `start()` call. Adapters are expected to make recovery
idempotent, but this is not contractually guaranteed.

**Future enhancement:** Document or guarantee atomic recovery in the
`LocalStorageAdapter` contract.

### 14. `isSuccess` does not prove synchronization occurred

`SyncCycleReport.isSuccess` means "no failure occurred during the cycle."
It returns `true` for:
- A `skippedOffline` cycle (no operations were fetched or pushed).
- A cycle that fetched zero eligible operations.
- A cycle where all pushed operations succeeded.

Consumers must not interpret `isSuccess == true` as proof that
synchronization occurred. Check `succeeded > 0` and
`skippedOffline == false` for actual sync evidence.

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
| `LocalStorageAdapter.recoverInFlightOperations` atomicity guarantee | Document or guarantee atomic recovery | Non-breaking — documentation or contract clarification |
| Sqflite/Drift null sequence number handling | Fix NOT EXISTS and ORDER BY for null values | Non-breaking — adapter fix |
| Hive null sequence number handling | Document or replace `?? 0` fallback | Non-breaking — adapter fix |
| `RemoteSyncAdapter.pull` | Pull synchronization | Breaking — new method on interface |
| `RemoteSyncAdapter.push` with `CancelToken` | Cancellation support for dispose | Breaking — signature change |
| `SyncEngineConfig.rateLimit` | Global rate limiting | Non-breaking — new config field |

These are documented for future phases but not required now.

---

## 12. Proposed Implementation Breakdown

> **This section is the authoritative mapping** of target-architecture
> components (Sections 1–11) to implementation phases. Sections 2–4
> describe the complete engine; this section defines exactly which parts
> exist after each phase. Phase 3.1 implements only the foundation —
> retry handling, conflict handling, and the per-entity ordering guard
> are explicitly deferred to 3.2–3.4.

Phases are sequential and non-overlapping. Each phase builds on the
previous one's working engine; later phases add result-handling branches
and verification, not rework.

### Phase 3.1 — Engine Foundation

Lifecycle, startup/disposal, connectivity monitoring, single-flight
cycle reservation, basic fetching and successful push, immutable cycle
reports, and basic concurrency with storage claiming. **No retry
algorithm, no conflict workflow, no per-entity ordering guard** — those
land in 3.2–3.4.

**Files to create:**
- `lib/src/engine/sync_engine.dart` — main engine class
- `lib/src/engine/sync_engine_config.dart` — configuration with validation
- `lib/src/engine/sync_cycle_report.dart` — report value object
- Update `lib/local_remote_synchronization.dart` — export new types

**Internal (not exported):**
- `_CycleSlot` — synchronous cycle reservation token
- `_CycleAccumulator` — mutable per-cycle statistics accumulator

**Constructor note:** `SyncEngine` in this phase accepts
`localStorage`, `remoteSync`, `connectivity`, and `config`. The
`retryPolicy` parameter and `RetryPolicy` type are introduced in
Phase 3.2 together with failure handling.

**Tests:**
- Engine lifecycle (start, syncNow, dispose)
- `start()` idempotency (already started returns immediately)
- `start()` retry after recovery failure — including partial recovery state
- `start()` retry after connectivity subscription failure
- `start()` retry after connectivity sampling failure
- Concurrent `start()` calls share one startup
- `syncNow()` before `start()` throws
- `dispose()` during startup — **deterministic test with
  `recoverInFlightOperations()` pending via Completer**
- `dispose()` concurrent calls share one disposal
- `dispose()` during active cycle (no timeout, waits for completion)
- **Dispose with pending auto-trigger** — no cycle starts, no emit after close
- Single-flight cycle reservation (concurrent `syncNow` calls share slot)
- **Two sequential offline `syncNow()` calls** — distinct report instances,
  separate cycles, slot cleared between calls
- Cycle completing before completer awaited — report still delivered
- Basic sync cycle with mock adapters (fetch → claim → push →
  `markSucceeded`)
- `SyncResult.operationId` validated before `markSucceeded`
- **Phase 3.1 interim mismatch behavior:** mismatched
  `result.operationId` → `markSucceeded` never called; cycle aborts
  with `cycleError`; claimed op remains `inFlight` (recovered next
  `start()`); no outcome counter incremented; no `markDeadLetter` call
- Connectivity-triggered sync
- Connectivity event buffering during startup (synchronous + async)
- Configuration validation (batchSize, maxConcurrency)
- `markInFlight` failure aborts cycle (first, middle, last in chunk)
- `markInFlight` failure: no pushes started for chunk
- Basic chunked concurrency (`maxConcurrency` parallel pushes)
- **Report immutability:** exactly one `SyncCycleReport` instance per
  cycle; same instance returned by `syncNow()` and emitted on `reports`;
  `completedAt` always populated
- **`isSuccess` semantics:** `skippedOffline` → `isSuccess == true` but
  `succeeded == 0`

### Phase 3.2 — Retry and Failure Handling

`RetryPolicy` contract, exponential backoff with jitter, retry
scheduling, non-retryable failures, max-attempt and dead-letter
decisions, result-mismatch dead-lettering, operation-level storage
failure reporting, and retry-policy exception semantics.

**Files to create:**
- `lib/src/engine/retry_policy.dart` — interface
- `lib/src/engine/exponential_backoff_retry_policy.dart` — default impl

**Files to modify:**
- `lib/src/engine/sync_engine.dart` — add `retryPolicy` constructor
  parameter and `SyncFailure` handling branch

**Tests:**
- Exponential backoff timing
- Jitter distribution
- `jitterFactor = 1.0` produces strictly positive delay <= maxDelay
- `retryAfter` as minimum (not shortened by backoff/jitter)
- `retryAfter > maxDelay` is preserved (not capped)
- Retry scheduling via `markFailed` + `nextRetryAt`
- Max attempts → dead-letter
- Non-retryable → immediate dead-letter
- Result `operationId` mismatch → immediate dead-letter — **replaces the
  Phase 3.1 interim cycle-abort**: non-retryable protocol violation,
  `markDeadLetter` called, `deadLettered++` only after successful
  persistence, `storageErrors++` if the transition throws, never
  `succeeded`/`failed`
- **RetryPolicy exceptions:** `shouldDeadLetter`/`computeDelay` throwing
  → cycle aborts with `cycleError` after chunk settles; op remains
  `inFlight`; recovered next `start()`; no outcome counter
- Numeric overflow prevention for large attempt counts
- `attemptCount >= 1` validation
- Same-cycle retry prevention (fixed eligibility cutoff)
- **Operation-level storage failure reporting:** `markSucceeded`/
  `markFailed`/`markDeadLetter` throwing → `storageErrors++`,
  `isSuccess == false`, persisted state unknown
- Remote push succeeds but `markSucceeded` fails → re-push on recovery,
  `storageErrors++` in originating cycle's report
- All error handling matrix scenarios

### Phase 3.3 — Conflict Handling

`SyncConflict` detection, durable dead-letter transition, conflict event
emission, and delivery limitations. **No conflict-resolution workflow.**

**Files to create:**
- `lib/src/engine/sync_conflict_event.dart` — conflict event

**Files to modify:**
- `lib/src/engine/sync_engine.dart` — add `conflicts` stream and
  `SyncConflict` handling branch
- `lib/local_remote_synchronization.dart` — export `SyncConflictEvent`

**Tests:**
- Conflict event emitted only after `markDeadLetter` completes without throwing
- No event emitted if `markDeadLetter` throws → `storageErrors++`
- Conflict stream delivery semantics (broadcast, no listener = lost)
- Multiple conflicts in one cycle
- Operation remains `deadLetter` after conflict
- `report.conflicted` is subset of `report.deadLettered`
- `SyncConflictEvent.operation` is pre-transition snapshot (status != deadLetter)
- No conflict-resolution workflow advertised

### Phase 3.4 — Concurrency and FIFO Verification

Per-entity ordering guard, entity outcome blocking, batch/concurrency
limit verification, adapter edge cases, and cross-instance limitations.

**Files to modify:**
- `lib/src/engine/sync_engine.dart` — add/verify per-entity ordering
  guard (`jsonEncode` keys, group-sort-select) and `entityBlocked`
  tracking

**Tests:**
- Per-entity ordering guard (no concurrent pushes for same entity)
- Cross-entity parallelism
- Batch size limits
- Max concurrency limits
- Entity blocking across cycle: failure → blocked; dead-letter →
  blocked permanently; success → unblocked for later fetch
- Storage-transition failure → entity blocked for rest of cycle
- Adapter returning multiple ops per entity → guard selects earliest
  by `(sequenceNumber, createdAt, operationId)`
- Collision-safe entity keys with special characters (`:` in
  entityType/entityId)
- FIFO limitation documentation verified against Sqflite/Drift/Hive
  null-sequence-number behavior
- Cross-instance race documented (no atomic claiming)

### Phase 3.5 — Integration Tests

Runs against the **completed** implementation (post-Phase 3.4), so all
result-handling paths — success, retry, conflict, dead-letter, and
per-entity ordering — are available under test.

**Files to create:**
- `test/engine/sync_engine_integration_test.dart` — end-to-end with real adapters

**Tests:**
- Full sync cycle with Sqflite adapter + mock remote (success, failure
  with retry across cycles, conflict, dead-letter)
- Crash recovery (inFlight → pending → retry on next start)
- Remote push succeeds but `markSucceeded` fails → re-push on recovery
- Connectivity loss during push
- Disposal during active cycle (no timeout, waits for completion)
- **Disposal during `recoverInFlightOperations()` pending**
- Multiple sequential cycles
- `start()` failure and retry (including partial recovery)
- Connectivity event buffering during startup (sync + async events)
- Two sequential offline `syncNow()` calls

### Phase 3.6 — Documentation and Examples

**Files to create:**
- `packages/local_remote_synchronization/README.md` — add SyncEngine section
- `docs/architecture.md` — update status from "planned" to "implemented"
- `examples/basic_offline_first_app/` — add SyncEngine usage (if example exists)

---

## Appendix A — Usage Example

> Shows the **final API** (post-Phase 3.3). The Phase 3.1 engine has no
> `retryPolicy` parameter and no `conflicts` stream — see the API
> staging note in Section 3.

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
  // NOTE: event.operation is a PRE-TRANSITION snapshot — its status
  // reflects fetch-time state (e.g., pending), not deadLetter.
  engine.conflicts.listen((event) {
    print('Conflict on ${event.operation.entityType}/${event.operation.entityId}');
    print('Operation status at fetch time: ${event.operation.status}');
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
    if (report.storageErrors > 0) {
      print('Storage transition failures (state unknown): ${report.storageErrors}');
    }
    if (report.skippedOffline) {
      // isSuccess may be true here — but NO operations were synchronized.
      print('Cycle skipped: device offline (no operations synchronized)');
    }
    // isSuccess == true means "no failure occurred", NOT "sync happened".
    // Check report.succeeded > 0 for actual synchronization evidence.
  });

  // Start engine (recovers in-flight once, begins auto-sync)
  await engine.start();

  // Manual sync — requires start() to have completed
  final report = await engine.syncNow();
  print('Manual sync: $report');

  // ... app runs ...

  // Cleanup — sets disposal-requested synchronously, awaits startup
  // and active cycle reservation (no timeout), then closes streams
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
| `SyncConflictEvent.operation` is pre-transition snapshot | Adapter does not mutate in-memory SyncOperation; event reflects fetch-time state |
| No `markSucceeded` as conflict resolution | Semantic mismatch; documented as known limitation; not a supported workflow |
| Recovery once on `start()`, not per cycle | Prevents interfering with other engine instances' in-flight ops |
| `start()` idempotent when already started | Recovery executes only once per successful startup lifecycle |
| `syncNow()` requires `start()` | Ensures recovery completes before any sync cycle |
| `_started = true` before scheduling auto-sync | Auto-trigger cannot run while startup is incomplete |
| Single-flight via synchronous `_CycleSlot` reservation | Reservation token created/assigned before any await; no reliance on Future identity or async timing |
| `_CycleSlot` holds `Completer<SyncCycleReport>` | Cycle executor completes it with the immutable report; callers share `completer.future` |
| Single `finally` cleanup clears only own reservation | `identical(_activeCycleSlot, slot)` prevents stale cleanup; covers all exits including skipped-offline |
| No `Completer` timing assumptions | Slot exists before cycle starts; no window where cycle completes before "Future" assigned |
| Fixed eligibility cutoff per cycle | Prevents same-cycle retries even with short retry delays |
| `attemptedOperationIds` defensive set | Additional guard against same-cycle duplicate attempts |
| Per-entity key uses `jsonEncode([entityType, entityId])` | Collision-safe; prevents delimiter collisions |
| Entity grouping + sort + select earliest | Fixes unreachable `_isEarlier` comparison; deterministic tiebreak |
| Entity blocked after failure/conflict/storage-error | Prevents later ops for same entity after uncertain outcome |
| Entity unblocked after successful `markSucceeded` | Allows next op for entity in later batch |
| All `markInFlight` before any push | Prevents pushing unclaimed operations |
| `markInFlight` failure aborts cycle, no pushes | Cannot safely determine if operation was claimed; sets `cycleError` |
| Storage transition failure → persisted state unknown | Storage method throwing does not prove rollback |
| `storageErrors` counter for post-push transition failures | Report cannot indicate success when operation-level storage failure occurred; `isSuccess` forced false |
| Outcome counters increment only after successful transition | `succeeded`/`failed`/`deadLettered`/`conflicted` mutually exclusive with `storageErrors` per operation |
| Cycle-level storage failures use `cycleError`, not `storageErrors` | `markInFlight`/`fetchEligibleOperations` failures abort the cycle; they are not per-operation post-push transitions |
| Immutable `SyncCycleReport` via `_CycleAccumulator` | Mutable accumulator during execution; exactly one immutable report built at completion; same instance returned and emitted |
| `completedAt` always populated | Report constructed only at cycle completion on every exit path |
| `isSuccess` means "no failure occurred" | Not proof of synchronization; `skippedOffline` and zero-fetch cycles also return true; consumers check `succeeded`/`skippedOffline` |
| Post-push storage failures don't abort cycle | Operation state unknown; entity blocked; `storageErrors++`; recovery on next start |
| Idempotency not assumed | `idempotencyKey` field does not automatically prevent duplicate remote effects |
| Crash scenario: push succeeds but `markSucceeded` fails | Operation re-pushed on recovery; depends on backend idempotency enforcement |
| `recoverInFlightOperations` not guaranteed atomic | Partial recovery possible; startup retry re-runs recovery; documented limitation |
| Dispose sets `_disposeRequested` synchronously before any await | Prevents new reservations, triggers, and startup continuation immediately |
| Dispose awaits startup, then slot completer, then closes streams | Guarantees no emit after close; no timeout (no cancellation in contracts) |
| Dispose shares one Future for concurrent calls | Every caller awaits completed cleanup |
| Auto-trigger/dispose race guarded by `_disposeRequested` | Pending triggers get StateError (caught) or await existing slot; no new work after flag set |
| Connectivity events buffered during startup | `_initializing` and buffer set BEFORE subscribing; sync+async events buffered |
| `retryAfter` as minimum, not override | Server delay is a floor; `maxDelay` caps local backoff only |
| `maxDelay` does not cap `retryAfter` | Server-provided delay larger than maxDelay is preserved |
| RNG owned by `ExponentialBackoffRetryPolicy` only | Single ownership; `SyncEngine` does not accept `Random` |
| RetryPolicy must support concurrent calls in one isolate | May be stateful but must not corrupt shared state |
| `jitterFactor = 1.0` clamped to positive <= maxDelay | Prevents zero-delay tight retry loops; never exceeds maxDelay |
| Overflow-safe saturating backoff | No hard-coded bit-width threshold; platform-agnostic |
| Result `operationId` validation → immediate dead-letter | Protocol violation is non-retryable; never apply wrong result |
| `conflicted` is subset of `deadLettered` | Every conflict is dead-lettered; dead-lettered includes other causes |
| `fetched` counts raw adapter results | Before filtering; not unique operations selected for pushing |
| No auto-sync on enqueue (watchPendingCount) | Avoids sync storms; apps can call syncNow() explicitly |
| One engine per outbox recommended | Current contract lacks atomic claiming; idempotency enforcement is adapter/backend-dependent |
| `SyncEngine` accepts `SyncEngineConfig?` (nullable) | `SyncEngineConfig` has runtime validation → non-const constructor → cannot be a default parameter value; engine resolves default internally |
| RetryPolicy exceptions abort the cycle | Policy fault affects all ops identically; engine cannot determine correct transition; op stays `inFlight` → recovered next `start()`; consistent with `markInFlight` abort policy |
| Phases 3.1–3.6 sequential and non-overlapping | 3.1 foundation only; retry/conflict/FIFO verification land in their own phases |
