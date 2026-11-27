import 'dart:async';

import 'package:local_remote_synchronization/local_remote_synchronization.dart';
import 'package:test/test.dart';

/// Deterministic in-memory [LocalStorageAdapter] for engine tests.
///
/// Models the outbox semantics the engine relies on: `fetchEligible`
/// returns `pending` operations (or `failed` ones whose `nextRetryAt`
/// is due), `markInFlight` removes them from eligibility, and
/// `markSucceeded` removes them from the outbox entirely.
final class FakeLocalStorageAdapter implements LocalStorageAdapter {
  FakeLocalStorageAdapter({List<String>? events}) : _events = events;

  /// Optional shared event log for cross-adapter ordering assertions.
  final List<String>? _events;

  /// All seeded operations, including non-eligible ones.
  final List<SyncOperation> operations = <SyncOperation>[];

  /// Lifecycle overrides applied by transition calls, keyed by
  /// operationId. Falls back to the operation's own `status`.
  final Map<String, SyncOperationStatus> _statusOverrides =
      <String, SyncOperationStatus>{};

  int recoverCalls = 0;
  int recoveredCount = 0;
  Error? recoverError;

  /// When set, `recoverInFlightOperations` waits on this completer.
  Completer<void>? recoverGate;

  int fetchCalls = 0;
  final List<DateTime> fetchNows = <DateTime>[];
  Error? fetchError;

  final List<String> inFlightCalls = <String>[];
  final List<String> succeededCalls = <String>[];
  final List<String> failedCalls = <String>[];
  final List<String> deadLetterCalls = <String>[];

  /// operationIds for which `markInFlight` throws.
  final Set<String> markInFlightFailures = <String>{};

  /// operationIds for which `markSucceeded` throws.
  final Set<String> markSucceededFailures = <String>{};

  SyncOperationStatus statusOf(String operationId) {
    final op = operations.firstWhere(
      (o) => o.operationId == operationId,
      orElse: () => throw StateError('Unknown operation: $operationId'),
    );
    return _statusOverrides[operationId] ?? op.status;
  }

  bool _isEligible(SyncOperation op, DateTime now) {
    final status = _statusOverrides[op.operationId] ?? op.status;
    switch (status) {
      case SyncOperationStatus.pending:
        return true;
      case SyncOperationStatus.failed:
        final nextRetryAt = op.nextRetryAt;
        return nextRetryAt == null || !nextRetryAt.isAfter(now);
      case SyncOperationStatus.inFlight:
      case SyncOperationStatus.deadLetter:
        return false;
    }
  }

  @override
  Future<T> runInTransaction<T>(
    Future<T> Function(OutboxWriter outbox) action,
  ) =>
      throw UnimplementedError('Not used by SyncEngine.');

  @override
  Future<int> recoverInFlightOperations() async {
    recoverCalls++;
    _events?.add('recoverInFlightOperations');
    final gate = recoverGate;
    if (gate != null) {
      await gate.future;
    }
    if (recoverError != null) {
      throw recoverError!;
    }
    var recovered = 0;
    for (final op in operations) {
      if ((_statusOverrides[op.operationId] ?? op.status) ==
          SyncOperationStatus.inFlight) {
        _statusOverrides[op.operationId] = SyncOperationStatus.pending;
        recovered++;
      }
    }
    recoveredCount = recovered;
    return recovered;
  }

  @override
  Future<List<SyncOperation>> fetchEligibleOperations({
    required DateTime now,
    required int limit,
    bool preservePerEntityOrder = true,
  }) async {
    fetchCalls++;
    fetchNows.add(now);
    _events?.add('fetchEligibleOperations');
    if (fetchError != null) {
      throw fetchError!;
    }
    return operations
        .where((op) => _isEligible(op, now))
        .take(limit)
        .toList(growable: false);
  }

  @override
  Future<void> markInFlight(String operationId) async {
    inFlightCalls.add(operationId);
    _events?.add('markInFlight:$operationId');
    if (markInFlightFailures.contains(operationId)) {
      throw StateError('markInFlight storage failure');
    }
    _statusOverrides[operationId] = SyncOperationStatus.inFlight;
  }

  @override
  Future<void> markSucceeded(String operationId) async {
    succeededCalls.add(operationId);
    _events?.add('markSucceeded:$operationId');
    if (markSucceededFailures.contains(operationId)) {
      throw StateError('markSucceeded storage failure');
    }
    operations.removeWhere((op) => op.operationId == operationId);
    _statusOverrides.remove(operationId);
  }

  @override
  Future<void> markFailed(
    String operationId, {
    required int attemptCount,
    required DateTime nextRetryAt,
    required SyncError error,
  }) async {
    failedCalls.add(operationId);
    _events?.add('markFailed:$operationId');
    _statusOverrides[operationId] = SyncOperationStatus.failed;
  }

  @override
  Future<void> markDeadLetter(
    String operationId, {
    required int attemptCount,
    required SyncError error,
  }) async {
    deadLetterCalls.add(operationId);
    _events?.add('markDeadLetter:$operationId');
    _statusOverrides[operationId] = SyncOperationStatus.deadLetter;
  }

  @override
  Stream<int> watchPendingCount() => const Stream<int>.empty();
}

/// Deterministic [RemoteSyncAdapter] for engine tests.
final class FakeRemoteSyncAdapter implements RemoteSyncAdapter {
  FakeRemoteSyncAdapter({List<String>? events}) : _events = events;

  /// Optional shared event log for cross-adapter ordering assertions.
  final List<String>? _events;

  final List<SyncOperation> pushed = <SyncOperation>[];
  int pushCalls = 0;

  /// Optional per-operation gates: push awaits the completer before
  /// producing a result.
  final Map<String, Completer<SyncResult>> gates =
      <String, Completer<SyncResult>>{};

  /// Produces the result for an operation. Defaults to [SyncSuccess].
  SyncResult Function(SyncOperation operation)? resultFor;

  /// Throws for an operation when non-null.
  Error? Function(SyncOperation operation)? throwFor;

  @override
  Future<SyncResult> push(SyncOperation operation) async {
    pushCalls++;
    pushed.add(operation);
    _events?.add('push:${operation.operationId}');
    final gate = gates[operation.operationId];
    if (gate != null) {
      await gate.future;
    }
    final error = throwFor?.call(operation);
    if (error != null) {
      throw error;
    }
    final result = resultFor?.call(operation);
    if (result != null) {
      return result;
    }
    return SyncSuccess(operationId: operation.operationId);
  }
}

/// Deterministic [ConnectivityProvider] for engine tests.
final class FakeConnectivityProvider implements ConnectivityProvider {
  FakeConnectivityProvider({bool isConnected = true, List<String>? events})
      : _isConnected = isConnected,
        _events = events {
    _controller = StreamController<bool>.broadcast(
      onListen: () => listenCount++,
      onCancel: () => cancelCount++,
    );
  }

  /// Optional shared event log for ordering assertions.
  final List<String>? _events;

  /// Number of active stream subscriptions.
  int listenCount = 0;

  /// Number of cancelled stream subscriptions.
  int cancelCount = 0;

  /// When set, the `onConnectivityChanged` getter throws it.
  Error? subscribeError;

  /// When set, the `isConnected` getter throws it.
  Error? sampleError;

  late final StreamController<bool> _controller;

  bool _isConnected;

  /// When set, the `isConnected` getter waits on this completer.
  Completer<bool>? isConnectedGate;

  @override
  Future<bool> get isConnected {
    _events?.add('isConnectedSample');
    final error = sampleError;
    if (error != null) {
      throw error;
    }
    final gate = isConnectedGate;
    if (gate != null) {
      return gate.future;
    }
    return Future<bool>.value(_isConnected);
  }

  @override
  Stream<bool> get onConnectivityChanged {
    _events?.add('connectivitySubscribe');
    final error = subscribeError;
    if (error != null) {
      throw error;
    }
    return _controller.stream;
  }

  /// Emits a connectivity transition and updates the sampled value.
  void emit(bool online) {
    _isConnected = online;
    _controller.add(online);
  }

  Future<void> close() => _controller.close();
}

/// Builds a minimal valid [SyncOperation] for tests.
SyncOperation makeOperation(
  String operationId, {
  SyncOperationType type = SyncOperationType.update,
  String entityType = 'task',
  String? entityId,
  DateTime? createdAt,
}) =>
    SyncOperation(
      operationId: operationId,
      idempotencyKey: 'idem-$operationId',
      entityType: entityType,
      entityId: entityId ?? 'entity-$operationId',
      type: type,
      createdAt: createdAt ?? DateTime.utc(2024, 1, 1),
      payload: type == SyncOperationType.delete
          ? null
          : <String, dynamic>{'value': operationId},
    );

/// Pumps the event loop until [condition] holds or [maxIterations]
/// microtask/event-queue turns elapse, then asserts it held.
Future<void> waitFor(bool Function() condition) async {
  for (var i = 0; i < 200 && !condition(); i++) {
    await Future<void>.delayed(Duration.zero);
  }
  expect(condition(), isTrue, reason: 'Condition was not met in time.');
}
