/// Immutable configuration mapping a `SyncOperation.entityType` to a
/// Firestore collection.
///
/// [collectionPath] is a Firestore collection path — a single segment
/// (`'notes'`) or a nested odd-length path (`'users/user_1/todos'`).
/// `entityType` is never treated as a collection name; every entity type
/// requires an explicit mapping.
///
/// [versionField], when set, names an application-managed integer field
/// used for optimistic concurrency and version progression. This is
/// **not** Firestore `updateTime`-based concurrency: the adapter reads,
/// compares, increments, and writes this field inside a transaction, and
/// the application owns its semantics.
final class FirestoreEntityConfig {
  /// Creates a validated, immutable entity configuration.
  FirestoreEntityConfig({
    required String collectionPath,
    String? versionField,
  })  : collectionPath = validateCollectionPath(collectionPath),
        versionField = versionField == null
            ? null
            : _validateFieldName(versionField, 'versionField');

  /// Firestore collection path (normalized, no leading/trailing slashes).
  final String collectionPath;

  /// Optional application-managed integer version field name.
  final String? versionField;

  /// Validates a Firestore collection path: non-blank, odd number of
  /// non-empty segments, no `.`/`..` segments.
  ///
  /// Shared with the adapter's idempotency-ledger path validation.
  static String validateCollectionPath(String path) {
    final normalized = path.trim().replaceAll(RegExp(r'^/+|/+$'), '');
    if (normalized.isEmpty) {
      throw ArgumentError.value(path, 'collectionPath', 'Must not be blank.');
    }
    final segments = normalized.split('/');
    if (segments.length.isEven) {
      throw ArgumentError.value(
        path,
        'collectionPath',
        'Must have an odd number of segments (collection[/doc/collection…]).',
      );
    }
    for (final segment in segments) {
      if (segment.isEmpty || segment == '.' || segment == '..') {
        throw ArgumentError.value(
          path,
          'collectionPath',
          'Contains an empty or dot path segment.',
        );
      }
    }
    return normalized;
  }

  static String _validateFieldName(String name, String argumentName) {
    final trimmed = name.trim();
    if (trimmed.isEmpty) {
      throw ArgumentError.value(name, argumentName, 'Must not be blank.');
    }
    return trimmed;
  }

  @override
  bool operator ==(Object other) =>
      identical(this, other) ||
      other is FirestoreEntityConfig &&
          runtimeType == other.runtimeType &&
          collectionPath == other.collectionPath &&
          versionField == other.versionField;

  @override
  int get hashCode => Object.hash(collectionPath, versionField);
}
