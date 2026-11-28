import 'dart:async';

import 'package:local_remote_sqflite/local_remote_sqflite.dart';
import 'package:local_remote_synchronization/local_remote_synchronization.dart';
import 'package:sqflite/sqflite.dart';

import '../domain/todo.dart';

/// Application-owned repository for the Todo domain.
///
/// The repository owns:
/// - The `todos` table schema and all CRUD operations on it.
/// - Atomic transactional writes that persist both a domain change and
///   its corresponding [SyncOperation] in the outbox within the same
///   native Sqflite transaction via
///   [SqfliteLocalStorageAdapter.runWithTransaction].
///
/// The sync packages own nothing about Todos. They only see the opaque
/// JSON payloads carried inside [LocalMutation] and [SyncOperation].
final class TodoRepository {
  TodoRepository({
    required Database database,
    required SqfliteLocalStorageAdapter storageAdapter,
  })  : _db = database,
        _storage = storageAdapter;

  static const String _table = 'todos';

  final Database _db;
  final SqfliteLocalStorageAdapter _storage;

  // -------------------------------------------------------------------------
  // Schema
  // -------------------------------------------------------------------------

  /// Creates the `todos` table if it does not already exist.
  ///
  /// Call this once during database initialisation, alongside
  /// [SqfliteOutboxSchema.createSchema].
  static Future<void> createSchema(DatabaseExecutor executor) async {
    await executor.execute('''
      CREATE TABLE IF NOT EXISTS $_table (
        id        TEXT PRIMARY KEY,
        title     TEXT NOT NULL,
        completed INTEGER NOT NULL DEFAULT 0
      )
    ''');
  }

  // -------------------------------------------------------------------------
  // Queries
  // -------------------------------------------------------------------------

  /// Returns all todos ordered by rowid (insertion order).
  Future<List<Todo>> getAll() async {
    final rows = await _db.query(_table, orderBy: 'rowid ASC');
    return rows.map(_fromRow).toList(growable: false);
  }

  // -------------------------------------------------------------------------
  // Mutations — each persists the domain change and enqueues the outbox
  // operation in the same native Sqflite transaction.
  // -------------------------------------------------------------------------

  /// Inserts [todo] into the local `todos` table and enqueues a `create`
  /// [SyncOperation] in the outbox, atomically.
  ///
  /// Returns the persisted [SyncOperation]. Throws if the transaction fails;
  /// neither the domain row nor the outbox row will have been written.
  Future<SyncOperation> create(Todo todo) =>
      _storage.runWithTransaction((txn, outbox) async {
        await txn.insert(
          _table,
          _toRow(todo),
          conflictAlgorithm: ConflictAlgorithm.replace,
        );
        return outbox.enqueue(
          LocalMutation(
            entityType: 'todos',
            entityId: todo.id,
            type: SyncOperationType.create,
            payload: todo.toPayload(),
            occurredAt: DateTime.now(),
          ),
        );
      });

  /// Updates [todo] in the local `todos` table and enqueues an `update`
  /// [SyncOperation] in the outbox, atomically.
  Future<SyncOperation> update(Todo todo) =>
      _storage.runWithTransaction((txn, outbox) async {
        await txn.update(
          _table,
          _toRow(todo),
          where: 'id = ?',
          whereArgs: <Object?>[todo.id],
        );
        return outbox.enqueue(
          LocalMutation(
            entityType: 'todos',
            entityId: todo.id,
            type: SyncOperationType.update,
            payload: todo.toPayload(),
            occurredAt: DateTime.now(),
          ),
        );
      });

  /// Deletes the todo identified by [id] from the local `todos` table and
  /// enqueues a `delete` [SyncOperation] in the outbox, atomically.
  Future<SyncOperation> delete(String id) =>
      _storage.runWithTransaction((txn, outbox) async {
        await txn.delete(
          _table,
          where: 'id = ?',
          whereArgs: <Object?>[id],
        );
        return outbox.enqueue(
          LocalMutation(
            entityType: 'todos',
            entityId: id,
            type: SyncOperationType.delete,
            // payload is optional for deletes
            occurredAt: DateTime.now(),
          ),
        );
      });

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  static Map<String, Object?> _toRow(Todo todo) => <String, Object?>{
        'id': todo.id,
        'title': todo.title,
        'completed': todo.completed ? 1 : 0,
      };

  static Todo _fromRow(Map<String, Object?> row) => Todo(
        id: row['id']! as String,
        title: row['title']! as String,
        completed: (row['completed'] as int? ?? 0) != 0,
      );
}
