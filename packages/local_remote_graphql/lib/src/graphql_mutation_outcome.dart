import 'package:local_remote_synchronization/local_remote_synchronization.dart';

/// Typed outcome produced by a [GraphQLResponseParser] for a successful
/// GraphQL mutation execution (HTTP success, no GraphQL errors).
///
/// The adapter converts outcomes into Core [SyncResult] values:
/// [GraphQLMutationSuccess] → [SyncSuccess],
/// [GraphQLMutationConflict] → [SyncConflict],
/// [GraphQLMutationFailure] → [SyncFailure].
sealed class GraphQLMutationOutcome {
  const GraphQLMutationOutcome();
}

/// The mutation was applied successfully by the backend.
final class GraphQLMutationSuccess extends GraphQLMutationOutcome {
  /// Creates a success outcome.
  const GraphQLMutationSuccess({
    this.remoteVersion,
    this.serverTimestamp,
    this.syncedData,
  });

  /// Optional version or ETag assigned by the backend.
  final String? remoteVersion;

  /// Optional UTC timestamp recorded by the backend.
  final DateTime? serverTimestamp;

  /// Optional server-acknowledged entity data.
  final Map<String, dynamic>? syncedData;
}

/// The backend reports a state or version conflict.
///
/// [remoteSnapshot] is the authoritative remote state used by conflict
/// resolution; it may be empty when the backend cannot provide one.
final class GraphQLMutationConflict extends GraphQLMutationOutcome {
  /// Creates a conflict outcome.
  const GraphQLMutationConflict({
    required this.remoteSnapshot,
    this.remoteVersion,
    this.serverTimestamp,
  });

  /// Authoritative remote state returned by the backend.
  final Map<String, dynamic> remoteSnapshot;

  /// Optional remote version associated with [remoteSnapshot].
  final String? remoteVersion;

  /// Optional UTC timestamp of [remoteSnapshot].
  final DateTime? serverTimestamp;
}

/// The backend reports an application-level failure.
///
/// [error] must already be a fully-formed [SyncError]; the adapter wraps
/// it in a [SyncFailure] preserving the original `operationId`.
final class GraphQLMutationFailure extends GraphQLMutationOutcome {
  /// Creates a failure outcome.
  const GraphQLMutationFailure(this.error);

  /// Structured error describing the failure.
  final SyncError error;
}

/// Interprets a successful GraphQL mutation response.
///
/// Receives the original [SyncOperation] and the response `data` map
/// (the top-level `data` object of the GraphQL response). Implementations
/// navigate the backend-specific response shape and return a typed
/// [GraphQLMutationOutcome]. Throwing is treated as a non-retryable
/// serialization failure.
typedef GraphQLResponseParser = GraphQLMutationOutcome Function(
  SyncOperation operation,
  Map<String, dynamic> data,
);
