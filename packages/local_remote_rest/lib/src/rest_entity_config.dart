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
    final trimmed = path.trim();
    // Validate before stripping slashes: a leading '//' denotes an
    // authority component, not a relative path.
    _rejectUnsafePath(trimmed, 'collectionPath');
    final normalized = trimmed.replaceAll(RegExp(r'^/+|/+$'), '');
    if (normalized.isEmpty) {
      throw ArgumentError.value(
        path,
        'collectionPath',
        'Must not be blank.',
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
    _rejectUnsafePath(normalized, 'itemPathTemplate');
    return normalized;
  }

  /// Rejects endpoint paths that could escape the configured `baseUrl`
  /// origin or alter URI resolution: absolute URLs, scheme/authority
  /// components, query/fragment delimiters, and `.`/`..` path segments.
  static void _rejectUnsafePath(String path, String argumentName) {
    final parsed = Uri.tryParse(path);
    if (parsed != null &&
        (parsed.scheme.isNotEmpty || parsed.host.isNotEmpty)) {
      throw ArgumentError.value(
        path,
        argumentName,
        'Must be a relative path — absolute URLs and authority '
        'components are not allowed.',
      );
    }
    if (path.contains('?') || path.contains('#')) {
      throw ArgumentError.value(
        path,
        argumentName,
        'Must not contain query or fragment delimiters.',
      );
    }
    for (final segment in path.split('/')) {
      // Check the decoded segment: percent-encoded dots (%2E) decode to
      // '.'/'..' during URI resolution and would enable traversal.
      String decoded;
      try {
        decoded = Uri.decodeComponent(segment);
      } catch (_) {
        decoded = segment;
      }
      if (decoded == '.' || decoded == '..') {
        throw ArgumentError.value(
          path,
          argumentName,
          'Must not contain "." or ".." path segments.',
        );
      }
    }
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
