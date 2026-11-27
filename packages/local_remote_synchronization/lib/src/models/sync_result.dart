import 'package:collection/collection.dart';
import 'package:meta/meta.dart';

import '../utils/deep_freeze.dart';
import 'sync_error.dart';

/// Outcome returned by `RemoteSyncAdapter.push` for a single `SyncOperation`.
@immutable
sealed class SyncResult {
  /// Creates a [SyncResult] for [operationId].
  SyncResult({required this.operationId}) {
    if (operationId.trim().isEmpty) {
      throw ArgumentError.value(
        operationId,
        'operationId',
        'Must not be empty.',
      );
    }
  }

  /// Identifier of the `SyncOperation` that produced this result.
  final String operationId;
}

/// Indicates the operation was successfully applied by the remote backend.
@immutable
final class SyncSuccess extends SyncResult {
  /// Creates a [SyncSuccess] result and deeply freezes [syncedData].
  SyncSuccess({
    required super.operationId,
    this.remoteVersion,
    DateTime? serverTimestamp,
    Map<String, dynamic>? syncedData,
  }) : serverTimestamp = serverTimestamp?.toUtc(),
       syncedData = syncedData == null
           ? null
           : deepFreezeMap(syncedData, fieldName: 'syncedData');

  static const DeepCollectionEquality _equality = DeepCollectionEquality();

  /// Optional version or ETag assigned by the remote backend.
  final String? remoteVersion;

  /// Optional UTC timestamp recorded by the remote backend.
  final DateTime? serverTimestamp;

  /// Optional server-acknowledged entity data (for example, server timestamps).
  final Map<String, dynamic>? syncedData;

  @override
  bool operator ==(Object other) =>
      identical(this, other) ||
      other is SyncSuccess &&
          runtimeType == other.runtimeType &&
          operationId == other.operationId &&
          remoteVersion == other.remoteVersion &&
          serverTimestamp == other.serverTimestamp &&
          _equality.equals(syncedData, other.syncedData);

  @override
  int get hashCode => Object.hash(
    operationId,
    remoteVersion,
    serverTimestamp,
    _equality.hash(syncedData),
  );
}

/// Indicates the remote backend rejected the operation due to a state or
/// version conflict.
@immutable
final class SyncConflict extends SyncResult {
  /// Creates a [SyncConflict] result and deeply freezes [remoteSnapshot].
  SyncConflict({
    required super.operationId,
    required Map<String, dynamic> remoteSnapshot,
    this.remoteVersion,
    DateTime? serverTimestamp,
  }) : remoteSnapshot = deepFreezeMap(
         remoteSnapshot,
         fieldName: 'remoteSnapshot',
       ),
       serverTimestamp = serverTimestamp?.toUtc();

  static const DeepCollectionEquality _equality = DeepCollectionEquality();

  /// Authoritative remote state returned by the backend.
  final Map<String, dynamic> remoteSnapshot;

  /// Optional remote version or ETag associated with [remoteSnapshot].
  final String? remoteVersion;

  /// Optional UTC timestamp of [remoteSnapshot].
  final DateTime? serverTimestamp;

  @override
  bool operator ==(Object other) =>
      identical(this, other) ||
      other is SyncConflict &&
          runtimeType == other.runtimeType &&
          operationId == other.operationId &&
          _equality.equals(remoteSnapshot, other.remoteSnapshot) &&
          remoteVersion == other.remoteVersion &&
          serverTimestamp == other.serverTimestamp;

  @override
  int get hashCode => Object.hash(
    operationId,
    _equality.hash(remoteSnapshot),
    remoteVersion,
    serverTimestamp,
  );
}

/// Indicates the remote push failed with a structured [SyncError].
@immutable
final class SyncFailure extends SyncResult {
  /// Creates a [SyncFailure] result.
  SyncFailure({required super.operationId, required this.error});

  /// Structured error describing why the push failed.
  final SyncError error;

  @override
  bool operator ==(Object other) =>
      identical(this, other) ||
      other is SyncFailure &&
          runtimeType == other.runtimeType &&
          operationId == other.operationId &&
          error == other.error;

  @override
  int get hashCode => Object.hash(operationId, error);
}
