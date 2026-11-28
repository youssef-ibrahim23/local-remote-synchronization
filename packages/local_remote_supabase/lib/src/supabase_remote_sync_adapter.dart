import 'package:local_remote_synchronization/local_remote_synchronization.dart';
import 'package:supabase_flutter/supabase_flutter.dart';

import 'supabase_error_mapper.dart';
import 'supabase_table_config.dart';
import 'sync_validation_exception.dart';

/// [RemoteSyncAdapter] implementation that applies local mutations to a
/// Supabase backend through PostgREST.
///
/// The adapter is push-only: it never pulls remote changes and never
/// performs conflict resolution — a detected conflict is reported as
/// [SyncConflict] and Core persists it as a dead-letter.
///
/// Delivery is **at-least-once**. Retried operations are deduplicated
/// per row through the configured `idempotencyColumn`, which stores the
/// `SyncOperation.idempotencyKey` of the last applied operation. This
/// does not make delivery exactly-once: side effects outside the row
/// (triggers, functions, concurrent independent clients) are not covered,
/// and a `markSucceeded` failure after a successful push can cause a
/// duplicate remote effect on the next cycle.
///
/// Row-level security is entirely delegated to the supplied
/// [SupabaseClient]: the adapter uses the client's existing session and
/// never requires or handles service-role credentials. The idempotency
/// column must exist on every mapped table and be readable/writable
/// under the application's RLS policies.
///
/// Run **one sync engine per outbox**. Core provides no cross-instance
/// atomic claiming, so two engines sharing an outbox can push the same
/// operation concurrently.
final class SupabaseRemoteSyncAdapter implements RemoteSyncAdapter {
  /// Creates an adapter bound to [client] and an explicit
  /// `entityType` → [SupabaseTableConfig] mapping.
  ///
  /// The mapping must be non-empty and must not contain blank entity
  /// types. There is no fallback to arbitrary table names: an
  /// unmapped `entityType` produces a non-retryable validation
  /// [SyncFailure].
  SupabaseRemoteSyncAdapter({
    required SupabaseClient client,
    required Map<String, SupabaseTableConfig> tableMapping,
  })  : _client = client,
        _tableMapping = _validatedMapping(tableMapping);

  final SupabaseClient _client;
  final Map<String, SupabaseTableConfig> _tableMapping;

  static Map<String, SupabaseTableConfig> _validatedMapping(
    Map<String, SupabaseTableConfig> mapping,
  ) {
    if (mapping.isEmpty) {
      throw ArgumentError.value(
        mapping,
        'tableMapping',
        'Must contain at least one entityType mapping.',
      );
    }
    final result = <String, SupabaseTableConfig>{};
    for (final entry in mapping.entries) {
      if (entry.key.trim().isEmpty) {
        throw ArgumentError.value(
          entry.key,
          'tableMapping',
          'Entity type keys must not be blank.',
        );
      }
      result[entry.key] = entry.value;
    }
    return Map.unmodifiable(result);
  }

  @override
  Future<SyncResult> push(SyncOperation operation) async {
    final config = _tableMapping[operation.entityType];
    if (config == null) {
      return SyncFailure(
        operationId: operation.operationId,
        error: SyncError(
          code: SyncErrorCode.validation,
          message: 'No Supabase table mapping configured for entityType '
              "'${operation.entityType}'.",
          isRetryable: false,
        ),
      );
    }

    try {
      return switch (operation.type) {
        SyncOperationType.create => await _pushCreate(operation, config),
        SyncOperationType.update => await _pushUpdate(operation, config),
        SyncOperationType.delete => await _pushDelete(operation, config),
      };
    } catch (error, stackTrace) {
      final failure = SupabaseErrorMapper.tryMap(operation, error, stackTrace);
      if (failure != null) return failure;
      rethrow;
    }
  }

  // ------------------------------------------------------------------
  // Create
  // ------------------------------------------------------------------

  Future<SyncResult> _pushCreate(
    SyncOperation operation,
    SupabaseTableConfig config,
  ) async {
    final row = _writePayload(operation, config);
    row[config.primaryKeyColumn] = operation.entityId;
    row[config.idempotencyColumn] = operation.idempotencyKey;

    try {
      final inserted =
          await _client.from(config.tableName).insert(row).select();
      final acknowledged = inserted.isEmpty ? null : inserted.first;
      return SyncSuccess(
        operationId: operation.operationId,
        remoteVersion: _remoteVersion(config, acknowledged),
        syncedData: acknowledged,
      );
    } on PostgrestException catch (error) {
      if (error.code != '23505') rethrow;
      return _resolveDuplicateCreate(operation, config, error);
    }
  }

  /// A `23505` unique violation may have come from the primary key, the
  /// idempotency column, or any other unique constraint. Only a row
  /// fetched by the configured primary key whose idempotency column
  /// matches this operation's key is treated as an already-applied
  /// retry.
  Future<SyncResult> _resolveDuplicateCreate(
    SyncOperation operation,
    SupabaseTableConfig config,
    PostgrestException error,
  ) async {
    final existing = await _fetchRow(config, operation.entityId);
    if (existing == null) {
      return SyncFailure(
        operationId: operation.operationId,
        error: SyncError(
          code: SyncErrorCode.conflict,
          message: 'Insert rejected by a unique constraint that does not match '
              'the target entity (code: ${error.code ?? 'unknown'}).',
          isRetryable: false,
          cause: error,
        ),
      );
    }
    if (_idempotencyKeyOf(config, existing) == operation.idempotencyKey) {
      return SyncSuccess(
        operationId: operation.operationId,
        remoteVersion: _remoteVersion(config, existing),
        syncedData: existing,
      );
    }
    return SyncConflict(
      operationId: operation.operationId,
      remoteSnapshot: existing,
      remoteVersion: _remoteVersion(config, existing),
    );
  }

  // ------------------------------------------------------------------
  // Update
  // ------------------------------------------------------------------

  Future<SyncResult> _pushUpdate(
    SyncOperation operation,
    SupabaseTableConfig config,
  ) async {
    // Validate the caller payload locally before any network call.
    final body = _writePayload(operation, config);
    body[config.idempotencyColumn] = operation.idempotencyKey;

    final existing = await _fetchRow(config, operation.entityId);
    if (existing == null) {
      return SyncFailure(
        operationId: operation.operationId,
        error: SyncError(
          code: SyncErrorCode.notFound,
          message: 'Cannot update entity ${operation.entityId}: no remote row '
              'exists in table ${config.tableName}.',
          isRetryable: false,
        ),
      );
    }

    // The idempotency column tracks the last applied operation key.
    // A retry of an already-applied update sees its own key and is a
    // no-op success.
    if (_idempotencyKeyOf(config, existing) == operation.idempotencyKey) {
      return SyncSuccess(
        operationId: operation.operationId,
        remoteVersion: _remoteVersion(config, existing),
        syncedData: existing,
      );
    }

    final expectedVersion = _expectedVersion(operation, config);
    if (expectedVersion != null &&
        config.versionColumn != null &&
        existing[config.versionColumn] != expectedVersion) {
      return SyncConflict(
        operationId: operation.operationId,
        remoteSnapshot: existing,
        remoteVersion: _remoteVersion(config, existing),
      );
    }

    var query = _client
        .from(config.tableName)
        .update(body)
        .eq(config.primaryKeyColumn, operation.entityId);
    final versionColumn = config.versionColumn;
    if (expectedVersion != null && versionColumn != null) {
      // Conditional update: protects against a version change between
      // the read above and this write.
      query = query.eq(versionColumn, expectedVersion);
    }
    final updated = await query.select();

    if (updated.isEmpty) {
      // Either the row vanished or the version predicate lost a race.
      final latest = await _fetchRow(config, operation.entityId);
      if (latest == null) {
        return SyncFailure(
          operationId: operation.operationId,
          error: SyncError(
            code: SyncErrorCode.notFound,
            message: 'Update target ${operation.entityId} no longer exists in '
                'table ${config.tableName}.',
            isRetryable: false,
          ),
        );
      }
      return SyncConflict(
        operationId: operation.operationId,
        remoteSnapshot: latest,
        remoteVersion: _remoteVersion(config, latest),
      );
    }

    return SyncSuccess(
      operationId: operation.operationId,
      remoteVersion: _remoteVersion(config, updated.first),
      syncedData: updated.first,
    );
  }

  // ------------------------------------------------------------------
  // Delete
  // ------------------------------------------------------------------

  Future<SyncResult> _pushDelete(
    SyncOperation operation,
    SupabaseTableConfig config,
  ) async {
    final expectedVersion = _expectedVersion(operation, config);
    final versionColumn = config.versionColumn;

    // With optimistic concurrency we must inspect the row first; without
    // it a single conditional delete is enough.
    if (expectedVersion != null && versionColumn != null) {
      final existing = await _fetchRow(config, operation.entityId);
      if (existing == null) {
        // Already absent — repeated deletes are idempotent successes.
        return SyncSuccess(operationId: operation.operationId);
      }
      if (existing[versionColumn] != expectedVersion) {
        return SyncConflict(
          operationId: operation.operationId,
          remoteSnapshot: existing,
          remoteVersion: _remoteVersion(config, existing),
        );
      }
    }

    var query = _client
        .from(config.tableName)
        .delete()
        .eq(config.primaryKeyColumn, operation.entityId);
    if (expectedVersion != null && versionColumn != null) {
      query = query.eq(versionColumn, expectedVersion);
    }
    final deleted = await query.select();

    if (deleted.isEmpty) {
      // PostgREST cannot distinguish "row absent" from "row hidden by
      // RLS" — both produce an empty filtered response. An
      // already-absent entity is an idempotent success; a row the
      // caller cannot see may still exist remotely.
      if (expectedVersion != null && versionColumn != null) {
        final latest = await _fetchRow(config, operation.entityId);
        if (latest != null) {
          return SyncConflict(
            operationId: operation.operationId,
            remoteSnapshot: latest,
            remoteVersion: _remoteVersion(config, latest),
          );
        }
      }
      return SyncSuccess(operationId: operation.operationId);
    }

    return SyncSuccess(
      operationId: operation.operationId,
      remoteVersion: _remoteVersion(config, deleted.first),
      syncedData: deleted.first,
    );
  }

  // ------------------------------------------------------------------
  // Shared helpers
  // ------------------------------------------------------------------

  /// Returns a mutable copy of the operation payload stripped of
  /// adapter-managed columns after validating caller-supplied values.
  ///
  /// - The primary key, when present, must equal `entityId`.
  /// - The idempotency column, when present, must equal
  ///   `idempotencyKey`.
  /// - The version column is always removed (server-managed).
  Map<String, dynamic> _writePayload(
    SyncOperation operation,
    SupabaseTableConfig config,
  ) {
    final payload = operation.payload;
    if (payload == null) {
      throw ArgumentError(
        'Payload is required for ${operation.type.name} operations.',
      );
    }
    final row = Map<String, dynamic>.from(payload);

    final pkValue = row[config.primaryKeyColumn];
    if (pkValue != null && pkValue != operation.entityId) {
      throw SyncValidationException(
        'Payload ${config.primaryKeyColumn} ($pkValue) does not match '
        'entityId (${operation.entityId}).',
      );
    }
    row.remove(config.primaryKeyColumn);

    final idemValue = row[config.idempotencyColumn];
    if (idemValue != null && idemValue != operation.idempotencyKey) {
      throw SyncValidationException(
        'Payload ${config.idempotencyColumn} must not override the '
        'adapter-managed idempotency key.',
      );
    }
    row.remove(config.idempotencyColumn);

    final versionColumn = config.versionColumn;
    if (versionColumn != null) {
      row.remove(versionColumn);
    }
    return row;
  }

  Future<PostgrestMap?> _fetchRow(
    SupabaseTableConfig config,
    String entityId,
  ) =>
      _client
          .from(config.tableName)
          .select()
          .eq(config.primaryKeyColumn, entityId)
          .limit(1)
          .maybeSingle();

  Object? _expectedVersion(
    SyncOperation operation,
    SupabaseTableConfig config,
  ) {
    if (config.versionColumn == null) return null;
    return operation.metadata[config.expectedVersionKey];
  }

  Object? _idempotencyKeyOf(
    SupabaseTableConfig config,
    PostgrestMap row,
  ) =>
      row[config.idempotencyColumn];

  String? _remoteVersion(SupabaseTableConfig config, PostgrestMap? row) {
    final column = config.versionColumn;
    if (column == null || row == null) return null;
    final value = row[column];
    return value?.toString();
  }
}
