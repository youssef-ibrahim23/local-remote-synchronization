import '../models/sync_operation.dart';
import '../models/sync_result.dart';

/// Pushes a single [SyncOperation] to a remote backend.
///
/// Expected network, backend, and conflict failures should be returned as
/// [SyncFailure] or [SyncConflict] rather than thrown. Unexpected adapter or
/// invariant errors may throw.
abstract interface class RemoteSyncAdapter {
  /// Applies [operation] to the remote backend using
  /// [SyncOperation.idempotencyKey] to prevent duplicate execution on retries.
  Future<SyncResult> push(SyncOperation operation);
}
