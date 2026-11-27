import 'package:sqflite/sqflite.dart';

/// SQLite schema definition and identifier validation for the Sqflite outbox.
final class SqfliteOutboxSchema {
  SqfliteOutboxSchema._();

  /// Default SQLite table name used by `SqfliteLocalStorageAdapter`.
  static const String defaultTableName = 'sync_outbox';

  static final RegExp _identifierPattern = RegExp(r'^[a-zA-Z_][a-zA-Z0-9_]*$');

  /// Validates that [tableName] is a safe SQLite identifier to prevent SQL
  /// injection.
  static String validateTableName(String tableName) {
    if (!_identifierPattern.hasMatch(tableName)) {
      throw ArgumentError.value(
        tableName,
        'tableName',
        'Invalid SQLite identifier. Must match ^[a-zA-Z_][a-zA-Z0-9_]*\$.',
      );
    }
    return tableName;
  }

  /// Creates the outbox table and indexes on [executor] if they do not exist.
  ///
  /// Safe to call repeatedly (idempotent) or from `onCreate` / `onUpgrade`.
  static Future<void> createSchema(
    DatabaseExecutor executor, {
    String tableName = defaultTableName,
  }) async {
    final table = validateTableName(tableName);

    await executor.execute('''
      CREATE TABLE IF NOT EXISTS $table (
        sequence_number INTEGER PRIMARY KEY AUTOINCREMENT,
        operation_id TEXT NOT NULL UNIQUE,
        idempotency_key TEXT NOT NULL,
        entity_type TEXT NOT NULL,
        entity_id TEXT NOT NULL,
        operation_type TEXT NOT NULL,
        payload_json TEXT,
        metadata_json TEXT NOT NULL,
        status TEXT NOT NULL,
        attempt_count INTEGER NOT NULL DEFAULT 0,
        created_at_ms INTEGER NOT NULL,
        occurred_at_ms INTEGER,
        updated_at_ms INTEGER NOT NULL,
        next_retry_at_ms INTEGER,
        last_error_json TEXT
      )
    ''');

    await executor.execute('''
      CREATE INDEX IF NOT EXISTS idx_${table}_status_retry
      ON $table (status, next_retry_at_ms, created_at_ms, sequence_number)
    ''');

    await executor.execute('''
      CREATE INDEX IF NOT EXISTS idx_${table}_entity_seq
      ON $table (entity_type, entity_id, sequence_number)
    ''');
  }
}
