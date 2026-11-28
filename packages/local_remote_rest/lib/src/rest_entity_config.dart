/// HTTP verb used for update operations against an entity's item endpoint.
enum RestUpdateMethod {
  /// `PATCH {itemPath}` — partial update.
  patch,

  /// `PUT {itemPath}` — full replacement.
  put,
}

/// Immutable configuration mapping a `SyncOperation.entityType` to a REST
/// endpoint.
///
/// [collectionPath] is the collection endpoint used for creates, for
/// example `'notes'` or `'api/v1/notes'`. It is resolved relative to the
/// adapter's `baseUrl`.
///
/// [itemPathTemplate] targets a single item for updates and deletes. It
/// defaults to `'{collectionPath}/{id}'` and must contain the literal
/// `{id}` placeholder, which is replaced by the URI-encoded
/// `SyncOperation.entityId`.
///
/// [updateMethod] selects `PATCH` (default) or `PUT` for updates.
///
/// [idField], when set, names the request-body field that carries the
/// entity ID (for APIs that expect it in the payload rather than only in
/// the URL). The adapter injects `operation.entityId` into that field and
/// rejects payloads that supply a conflicting value.
final class RestEntityConfig {
  /// Creates a validated, immutable entity configuration.
  RestEntityConfig({
    required String collectionPath,
    String? itemPathTemplate,
    this.updateMethod = RestUpdateMethod.patch,
    this.idField,
  })  : collectionPath = _validateCollectionPath(collectionPath),
        itemPathTemplate = itemPathTemplate == null
            ? null
            : _validateItemPathTemplate(itemPathTemplate);

  /// Collection endpoint path, normalized without leading/trailing slashes.
  final String collectionPath;

  /// Optional custom item path template containing `{id}`.
  final String? itemPathTemplate;

  /// HTTP method used for updates.
  final RestUpdateMethod updateMethod;

  /// Optional request-body field carrying the entity ID.
  final String? idField;

  /// The effective item path template (`itemPathTemplate` or the default
  /// `'{collectionPath}/{id}'`).
  String get itemPath => itemPathTemplate ?? '$collectionPath/{id}';

  static String _validateCollectionPath(String path) {
    final normalized = path.trim().replaceAll(RegExp(r'^/+|/+$'), '');
    if (normalized.isEmpty) {
      throw ArgumentError.value(
        path,
        'collectionPath',
        'Must not be blank.',
      );
    }
    if (normalized.contains('?') || normalized.contains('#')) {
      throw ArgumentError.value(
        path,
        'collectionPath',
        'Must not contain query or fragment delimiters.',
      );
    }
    return normalized;
  }

  static String _validateItemPathTemplate(String template) {
    final normalized = template.trim();
    if (normalized.isEmpty) {
      throw ArgumentError.value(
        template,
        'itemPathTemplate',
        'Must not be blank.',
      );
    }
    if (normalized.startsWith('/')) {
      throw ArgumentError.value(
        template,
        'itemPathTemplate',
        'Must be relative to baseUrl (no leading slash).',
      );
    }
    if (!normalized.contains('{id}')) {
      throw ArgumentError.value(
        template,
        'itemPathTemplate',
        'Must contain the {id} placeholder.',
      );
    }
    return normalized;
  }

  @override
  bool operator ==(Object other) =>
      identical(this, other) ||
      other is RestEntityConfig &&
          runtimeType == other.runtimeType &&
          collectionPath == other.collectionPath &&
          itemPathTemplate == other.itemPathTemplate &&
          updateMethod == other.updateMethod &&
          idField == other.idField;

  @override
  int get hashCode => Object.hash(
        collectionPath,
        itemPathTemplate,
        updateMethod,
        idField,
      );
}
