/// Immutable configuration mapping a `SyncOperation.entityType` to a
/// Supabase table and its adapter-managed columns.
///
/// The adapter manages three columns on each mapped table:
///
/// - [primaryKeyColumn] — identifies the remote row. `SyncOperation
///   .entityId` is written to this column on create and used as the only
///   row filter on update/delete.
/// - [idempotencyColumn] — stores the stable
///   `SyncOperation.idempotencyKey` of the last applied operation so
///   retried pushes can be recognised. **The application must create
///   this column** (see README); the adapter never alters schema.
/// - [versionColumn] — optional optimistic-concurrency column compared
///   against `SyncOperation.metadata[expectedVersionKey]` when supplied.
final class SupabaseTableConfig {
  /// Creates a validated [SupabaseTableConfig].
  ///
  /// Throws [ArgumentError] when any name is blank or when the managed
  /// columns collide.
  SupabaseTableConfig({
    required this.tableName,
    this.primaryKeyColumn = 'id',
    this.idempotencyColumn = '_sync_operation_id',
    this.versionColumn,
    this.expectedVersionKey = 'baseVersion',
  }) {
    _requireName('tableName', tableName);
    _requireName('primaryKeyColumn', primaryKeyColumn);
    _requireName('idempotencyColumn', idempotencyColumn);
    _requireName('expectedVersionKey', expectedVersionKey);
    final version = versionColumn;
    if (version != null) {
      _requireName('versionColumn', version);
      if (version == primaryKeyColumn || version == idempotencyColumn) {
        throw ArgumentError.value(
          versionColumn,
          'versionColumn',
          'Must differ from primaryKeyColumn and idempotencyColumn.',
        );
      }
    }
    if (primaryKeyColumn == idempotencyColumn) {
      throw ArgumentError.value(
        idempotencyColumn,
        'idempotencyColumn',
        'Must differ from primaryKeyColumn.',
      );
    }
  }

  static void _requireName(String field, String value) {
    if (value.trim().isEmpty) {
      throw ArgumentError.value(value, field, 'Must not be blank.');
    }
  }

  /// PostgREST table or view name exposed to the `public` schema (or the
  /// schema configured on the `SupabaseClient`).
  final String tableName;

  /// Column holding the entity's remote primary key. Defaults to `id`.
  final String primaryKeyColumn;

  /// Column storing the idempotency key of the last applied operation.
  /// Defaults to `_sync_operation_id`.
  final String idempotencyColumn;

  /// Optional column used for optimistic concurrency checks. When
  /// `null`, no version comparison is performed.
  final String? versionColumn;

  /// Key inside `SyncOperation.metadata` that carries the expected
  /// remote version. Defaults to `baseVersion`.
  final String expectedVersionKey;

  @override
  bool operator ==(Object other) =>
      identical(this, other) ||
      other is SupabaseTableConfig &&
          runtimeType == other.runtimeType &&
          tableName == other.tableName &&
          primaryKeyColumn == other.primaryKeyColumn &&
          idempotencyColumn == other.idempotencyColumn &&
          versionColumn == other.versionColumn &&
          expectedVersionKey == other.expectedVersionKey;

  @override
  int get hashCode => Object.hash(
        tableName,
        primaryKeyColumn,
        idempotencyColumn,
        versionColumn,
        expectedVersionKey,
      );
}
