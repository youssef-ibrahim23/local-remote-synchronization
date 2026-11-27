import 'dart:async';
import 'dart:math' as math;

import 'package:meta/meta.dart';

import '../contracts/connectivity_provider.dart';
import '../contracts/local_storage_adapter.dart';
import '../contracts/remote_sync_adapter.dart';
import '../enums/sync_error_code.dart';
import '../models/sync_error.dart';
import '../models/sync_operation.dart';
import '../models/sync_result.dart';
import 'exponential_backoff_retry_policy.dart';
import 'retry_policy.dart';
import 'sync_engine_config.dart';

part 'sync_cycle_report.dart';

/// Push-only synchronization engine that orchestrates the durable
/// outbox.
///
/// Depends only on Core contracts. Does not know about specific
/// databases or remote backends.
///
/// Phase 3.2 scope adds retry scheduling via [RetryPolicy], operation-
/// level storage failure reporting, a fixed per-cycle eligibility
/// cutoff, and same-cycle attempt deduplication. Conflict handling and
/// per-entity ordering guards are added in later phases.
final class SyncEngine {
  /// Creates a [SyncEngine] with the given adapters and configuration.
  ///
  /// [localStorage] — durable outbox adapter (Sqflite, Drift, Hive, ...).
  /// [remoteSync] — remote push adapter (Supabase, REST, ...).
  /// [connectivity] — connectivity provider for auto-sync triggers.
  /// [config] — engine configuration. When `null`, a default
  ///   [SyncEngineConfig] is created internally.
  /// [clock] — injectable time source, primarily for testing. Defaults
  ///   to [DateTime.now].
  /// [retryPolicy] — retry strategy for failed pushes. When `null`, an
  ///   [ExponentialBackoffRetryPolicy] with default settings is used.
  SyncEngine({
    required LocalStorageAdapter localStorage,
    required RemoteSyncAdapter remoteSync,
    required ConnectivityProvider connectivity,
    SyncEngineConfig? config,
    RetryPolicy? retryPolicy,
    DateTime Function()? clock,
  })  : _localStorage = localStorage,
        _remoteSync = remoteSync,
        _connectivity = connectivity,
        _config = config ?? SyncEngineConfig(),
        _retryPolicy = retryPolicy ?? ExponentialBackoffRetryPolicy(),
        _clock = clock ?? DateTime.now;

  final LocalStorageAdapter _localStorage;
  final RemoteSyncAdapter _remoteSync;
  final ConnectivityProvider _connectivity;
  final SyncEngineConfig _config;
  final RetryPolicy _retryPolicy;
  final DateTime Function() _clock;

  bool _started = false;
  Future<void>? _startupFuture;
  bool _disposeRequested = false;
  Future<void>? _disposeFuture;
  _CycleSlot? _activeCycleSlot;

  bool _isConnected = false;
  bool _wasConnected = false;
  bool _initializing = false;
  List<bool>? _pendingConnectivityEvents;
  StreamSubscription<bool>? _connectivitySub;

  final StreamController<SyncCycleReport> _reportController =
      StreamController<SyncCycleReport>.broadcast();

  /// Whether [start] has completed successfully and [dispose] has not
  /// been requested.
  bool get isRunning => _started && !_disposeRequested;

  /// Stream of [SyncCycleReport]s emitted after each completed cycle.
  Stream<SyncCycleReport> get reports => _reportController.stream;

  /// Starts the engine.
  ///
  /// Recovers stale `inFlight` operations once, initializes
  /// connectivity monitoring (subscription before sampling, with
  /// buffering of events received during initialization), then marks
  /// the engine started and schedules an initial sync when configured.
  ///
  /// Idempotent once started; concurrent calls share one startup.
  /// Throws [StateError] if called after [dispose] has been requested.
  Future<void> start() {
    if (_disposeRequested) {
      throw StateError('SyncEngine is disposed.');
    }
    if (_started) {
      return Future<void>.value();
    }
    final inFlightStartup = _startupFuture;
    if (inFlightStartup != null) {
      return inFlightStartup;
    }
    final startup = _performStartup();
    _startupFuture = startup;
    return startup;
  }

  /// Manually triggers a sync cycle.
  ///
  /// Requires [start] to have completed successfully. Throws
  /// [StateError] if called before [start] or after [dispose] has been
  /// requested.
  ///
  /// If a cycle is already in progress, returns the in-flight cycle's
  /// report future (deduplication — no overlapping cycles).
  Future<SyncCycleReport> syncNow() {
    if (_disposeRequested) {
      throw StateError('SyncEngine is disposed.');
    }
    if (!_started) {
      throw StateError('SyncEngine.start() must complete first.');
    }
    final existing = _activeCycleSlot;
    if (existing != null) {
      return existing.completer.future;
    }
    // Synchronous reservation — no async gap before assignment.
    final slot = _CycleSlot();
    _activeCycleSlot = slot;
    unawaited(_executeCycle(slot));
    return slot.completer.future;
  }

  /// Releases all resources.
  ///
  /// Sets the disposal-requested flag synchronously, awaits any
  /// in-progress startup and the active cycle to completion, cancels
  /// the connectivity subscription, and closes internal stream
  /// controllers. Does not close the adapters — the application owns
  /// their lifecycles.
  ///
  /// Idempotent: concurrent calls share one disposal future.
  Future<void> dispose() {
    final existing = _disposeFuture;
    if (existing != null) {
      return existing;
    }
    _disposeRequested = true;
    final disposal = _performDispose();
    _disposeFuture = disposal;
    return disposal;
  }

  Future<void> _performStartup() async {
    try {
      // Step 1: Recover in-flight operations (once per startup).
      await _localStorage.recoverInFlightOperations();
      if (_startupAborted()) return;

      // Step 2: Initialize connectivity monitoring. The buffer and the
      // initializing flag are set BEFORE subscribing so no transition
      // can be missed.
      _pendingConnectivityEvents = <bool>[];
      _initializing = true;
      _connectivitySub = _connectivity.onConnectivityChanged.listen(
        _onConnectivityEvent,
        onError: (_) {},
      );
      if (_startupAborted()) return;

      // Step 3: Sample initial connectivity.
      final sampledConnected = await _connectivity.isConnected;
      if (_startupAborted()) return;

      _initializing = false;
      _isConnected = sampledConnected;
      _wasConnected = sampledConnected;

      // Step 4: Reconcile buffered events — the latest buffered event
      // wins over the sample because it is newer.
      final pending = _pendingConnectivityEvents;
      if (pending != null && pending.isNotEmpty) {
        final latest = pending.last;
        pending.clear();
        _isConnected = latest;
        _wasConnected = latest;
      }
      _pendingConnectivityEvents = null;

      if (_startupAborted()) return;

      // Step 5: Mark started BEFORE scheduling auto-sync so the
      // auto-trigger's syncNow() passes the started check.
      _started = true;
      _startupFuture = null;

      if (_config.syncOnStart && _isConnected && !_disposeRequested) {
        unawaited(_triggerAutoSync());
      }
    } catch (_) {
      _startupFuture = null;
      _started = false;
      _initializing = false;
      _pendingConnectivityEvents?.clear();
      _pendingConnectivityEvents = null;
      final sub = _connectivitySub;
      _connectivitySub = null;
      if (sub != null) {
        try {
          await sub.cancel();
        } catch (_) {}
      }
      rethrow;
    }
  }

  /// Returns `true` when disposal was requested mid-startup, performing
  /// abort cleanup.
  bool _startupAborted() {
    if (!_disposeRequested) {
      return false;
    }
    _initializing = false;
    _startupFuture = null;
    _pendingConnectivityEvents = null;
    return true;
  }

  void _onConnectivityEvent(bool online) {
    if (_disposeRequested) {
      return;
    }
    if (_initializing) {
      _pendingConnectivityEvents?.add(online);
      return;
    }
    _applyConnectivityEvent(online);
  }

  void _applyConnectivityEvent(bool online) {
    final wasConnected = _wasConnected;
    _isConnected = online;
    _wasConnected = online;
    if (_config.syncOnConnectivityRestore && !wasConnected && online) {
      unawaited(_triggerAutoSync());
    }
  }

  Future<void> _triggerAutoSync() async {
    if (_disposeRequested || !_started) {
      return;
    }
    try {
      await syncNow();
    } catch (_) {
      // Auto-triggered cycles are best-effort. Includes StateError from
      // syncNow() when disposal was requested between the guard check
      // and the call.
    }
  }

  Future<void> _performDispose() async {
    final startup = _startupFuture;
    if (startup != null) {
      try {
        await startup;
      } catch (_) {}
    }
    final sub = _connectivitySub;
    _connectivitySub = null;
    if (sub != null) {
      await sub.cancel();
    }
    final slot = _activeCycleSlot;
    if (slot != null) {
      try {
        await slot.completer.future;
      } catch (_) {}
    }
    await _reportController.close();
  }

  Future<void> _executeCycle(_CycleSlot slot) async {
    final startedAt = _clock();
    // Phase 3.2: one immutable eligibility cutoff for the whole cycle —
    // every fetch uses this same timestamp so an operation whose
    // nextRetryAt becomes due mid-cycle is not retried in this cycle.
    final accumulator = _CycleAccumulator(
      startedAt: startedAt,
      eligibilityCutoff: startedAt,
    );
    try {
      try {
        if (!_config.syncWhenOffline && !_isConnected) {
          accumulator.skippedOffline = true;
        } else {
          await _runFetchLoop(accumulator);
        }
      } catch (e, st) {
        accumulator.cycleError ??= SyncError(
          code: SyncErrorCode.unknown,
          message: 'Unexpected cycle error: $e',
          isRetryable: true,
          cause: e,
          stackTrace: st,
        );
      }

      // Exactly one immutable report per cycle; completedAt always
      // populated. build() is a pure constructor call and cannot throw.
      final report = accumulator.build(_clock());

      // Emit before completing the completer. Guarded so a failure can
      // never prevent completer completion.
      try {
        _reportController.add(report);
      } catch (_) {}

      slot.completer.complete(report);
    } finally {
      // Single cleanup path — clears only THIS cycle's reservation.
      if (identical(_activeCycleSlot, slot)) {
        _activeCycleSlot = null;
      }
    }
  }

  Future<void> _runFetchLoop(_CycleAccumulator accumulator) async {
    // Phase 3.2: defensive same-cycle deduplication. An ID is added only
    // after its markInFlight claim succeeds; filtered results that are
    // entirely already-attempted terminate the loop safely.
    final attemptedOperationIds = <String>{};

    while (true) {
      // Phase 3.2: every fetch uses the same immutable eligibility
      // cutoff captured at cycle start.
      final List<SyncOperation> fetchedOperations;
      try {
        fetchedOperations = await _localStorage.fetchEligibleOperations(
          now: accumulator.eligibilityCutoff,
          limit: _config.batchSize,
          preservePerEntityOrder: true,
        );
      } catch (e, st) {
        accumulator.cycleError = SyncError(
          code: SyncErrorCode.storage,
          message: 'fetchEligibleOperations failed (state unknown): $e',
          isRetryable: true,
          cause: e,
          stackTrace: st,
        );
        return;
      }

      if (fetchedOperations.isEmpty) {
        return;
      }
      // fetched counts raw adapter results, before dedup filtering.
      accumulator.fetched += fetchedOperations.length;

      // Filter operations already attempted this cycle. If nothing new
      // remains, the cycle is done — this also prevents an infinite
      // loop when an adapter keeps returning already-attempted IDs.
      final operations = fetchedOperations
          .where((op) => !attemptedOperationIds.contains(op.operationId))
          .toList(growable: false);
      if (operations.isEmpty) {
        return;
      }

      for (var start = 0;
          start < operations.length;
          start += _config.maxConcurrency) {
        final end = math.min(start + _config.maxConcurrency, operations.length);
        final chunk = operations.sublist(start, end);

        // Claim ALL operations in the chunk BEFORE any push begins.
        // If any markInFlight fails, abort the cycle — no push in this
        // chunk is started. Previously claimed ops stay inFlight (or in
        // an unknown state) and are recovered on next start.
        final claimed = <SyncOperation>[];
        var claimFailed = false;
        for (final op in chunk) {
          try {
            await _localStorage.markInFlight(op.operationId);
            claimed.add(op);
            attemptedOperationIds.add(op.operationId);
          } catch (e, st) {
            accumulator.cycleError = SyncError(
              code: SyncErrorCode.storage,
              message: 'markInFlight failed (state unknown): $e',
              isRetryable: true,
              cause: e,
              stackTrace: st,
            );
            claimFailed = true;
            break;
          }
        }
        if (claimFailed) {
          return;
        }

        // Pushes run concurrently. _pushAndHandle lets RetryPolicy
        // exceptions and the Phase 3.1-interim conflict abort propagate;
        // Future.wait waits for the whole chunk to settle before the
        // error reaches the cycle boundary.
        await Future.wait(
          claimed.map((op) => _pushAndHandle(op, accumulator)),
        );
      }

      if (operations.length < _config.batchSize) {
        return;
      }
    }
  }

  Future<void> _pushAndHandle(
    SyncOperation op,
    _CycleAccumulator accumulator,
  ) async {
    SyncResult result;
    try {
      result = await _remoteSync.push(op);
    } catch (e, st) {
      result = SyncFailure(
        operationId: op.operationId,
        error: SyncError(
          code: SyncErrorCode.unknown,
          message: 'Unexpected push error: $e',
          isRetryable: true,
          cause: e,
          stackTrace: st,
        ),
      );
    }

    // operationId validation BEFORE any storage transition.
    //
    // Phase 3.2 final: a mismatch is a non-retryable protocol violation.
    // Never call markSucceeded/markFailed. markDeadLetter is attempted;
    // deadLettered increments only after it succeeds, storageErrors if
    // it throws. The cycle is NOT aborted for this operation-level
    // outcome.
    if (result.operationId != op.operationId) {
      final error = SyncError(
        code: SyncErrorCode.unknown,
        message: 'Protocol violation: result.operationId '
            '(${result.operationId}) != pushed operationId '
            '(${op.operationId})',
        isRetryable: false,
      );
      try {
        await _localStorage.markDeadLetter(
          op.operationId,
          attemptCount: op.attemptCount + 1,
          error: error,
        );
        accumulator.deadLettered++;
      } catch (_) {
        // Persisted state is UNKNOWN — may be inFlight, pending,
        // deadLetter, or unchanged. Never assume a transition applied.
        accumulator.storageErrors++;
      }
      return;
    }

    if (result is SyncSuccess) {
      try {
        await _localStorage.markSucceeded(op.operationId);
        accumulator.succeeded++;
      } catch (_) {
        // Phase 3.2: operation-level storage failure. Persisted state
        // is UNKNOWN — it may be inFlight, pending, or already removed.
        // The cycle continues; no outcome counter is incremented.
        accumulator.storageErrors++;
      }
      return;
    }

    // Phase 3.1 interim preserved until Phase 3.3: a SyncConflict has
    // no transition path yet, so it remains a cycle-level abort. It is
    // never treated as a retryable SyncFailure.
    if (result is SyncConflict) {
      accumulator.cycleError = SyncError(
        code: SyncErrorCode.conflict,
        message: 'Remote conflict received; conflict handling is not '
            'available in this version.',
        isRetryable: false,
      );
      throw const _CycleAbortException();
    }

    // result is SyncFailure — schedule a retry or dead-letter via the
    // configured RetryPolicy.
    final error = (result as SyncFailure).error;
    final newAttemptCount = op.attemptCount + 1;

    // Policy calls are intentionally NOT wrapped: a throw propagates
    // through Future.wait (which lets the rest of the chunk settle)
    // to the cycle error boundary. No transition is applied and no
    // outcome counter is incremented for this operation.
    if (_retryPolicy.shouldDeadLetter(newAttemptCount, error)) {
      try {
        await _localStorage.markDeadLetter(
          op.operationId,
          attemptCount: newAttemptCount,
          error: error,
        );
        accumulator.deadLettered++;
      } catch (_) {
        accumulator.storageErrors++;
      }
    } else {
      final delay = _retryPolicy.computeDelay(newAttemptCount, error);
      try {
        await _localStorage.markFailed(
          op.operationId,
          attemptCount: newAttemptCount,
          nextRetryAt: _clock().add(delay),
          error: error,
        );
        accumulator.failed++;
      } catch (_) {
        accumulator.storageErrors++;
      }
    }
  }
}

/// Private reservation token for the single-flight cycle.
final class _CycleSlot {
  final Completer<SyncCycleReport> completer = Completer<SyncCycleReport>();
}

/// Private signal that aborts the current cycle after the in-flight
/// chunk settles. The specific [SyncError] is stored on the accumulator
/// before throwing.
final class _CycleAbortException implements Exception {
  const _CycleAbortException();
}

/// Mutable per-cycle statistics accumulator. Exactly one immutable
/// [SyncCycleReport] is built from it when the cycle completes.
final class _CycleAccumulator {
  _CycleAccumulator({
    required this.startedAt,
    required this.eligibilityCutoff,
  });

  final DateTime startedAt;

  /// Immutable per-cycle eligibility cutoff. Every
  /// `fetchEligibleOperations` call in the cycle uses this timestamp.
  final DateTime eligibilityCutoff;

  int fetched = 0;
  int succeeded = 0;
  int failed = 0;
  int conflicted = 0;
  int deadLettered = 0;
  int storageErrors = 0;
  SyncError? cycleError;
  bool skippedOffline = false;

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
