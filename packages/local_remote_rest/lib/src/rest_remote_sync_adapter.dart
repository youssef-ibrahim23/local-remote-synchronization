import 'dart:async';
import 'dart:convert';
import 'dart:typed_data';

import 'package:http/http.dart' as http;
import 'package:local_remote_synchronization/local_remote_synchronization.dart';

import 'rest_entity_config.dart';
import 'rest_error_mapper.dart';
import 'rest_validation_exception.dart';

/// [RemoteSyncAdapter] implementation that applies local mutations to a
/// generic REST backend over HTTP.
///
/// The adapter is push-only: it never pulls remote changes and never
/// performs conflict resolution — a detected conflict is reported as
/// [SyncConflict] and Core persists it as a dead-letter.
///
/// Delivery is **at-least-once**. The stable
/// `SyncOperation.idempotencyKey` is sent on every request through the
/// configured idempotency header (default `Idempotency-Key`), but the
/// header alone does not deduplicate anything: actual deduplication
/// depends on backend support for idempotent request handling. This
/// adapter never generates a new key and never claims exactly-once
/// delivery.
///
/// Optimistic concurrency is opt-in through ETag-style conditional
/// requests: when `operation.metadata[expectedVersionKey]` (default
/// `baseVersion`) is present it is sent verbatim in the configured
/// request header (default `If-Match`), and the configured response
/// header (default `ETag`) is surfaced as `remoteVersion`. This only
/// protects against stale writes when the backend actually enforces
/// conditional-request semantics; `412 Precondition Failed` and `409
/// Conflict` are reported as [SyncConflict].
///
/// Authentication is entirely the application's responsibility: supply
/// tokens through `defaultHeaders` or an injected `httpClient`. The
/// adapter never stores, refreshes, or logs credentials, and error
/// messages never include request headers.
///
/// Run **one sync engine per outbox**. Core provides no cross-instance
/// atomic claiming, so two engines sharing an outbox can push the same
/// operation concurrently.
final class RestRemoteSyncAdapter implements RemoteSyncAdapter {
  /// Creates an adapter bound to [baseUrl] and an explicit
  /// `entityType` → [RestEntityConfig] mapping.
  ///
  /// [baseUrl] must be an absolute `http`/`https` URI; entity paths are
  /// resolved relative to it. The mapping must be non-empty with no
  /// blank entity types — there is no fallback to arbitrary endpoints,
  /// and an unmapped `entityType` produces a non-retryable validation
  /// [SyncFailure].
  ///
  /// When [httpClient] is omitted the adapter creates and owns a
  /// default `http.Client`; call [close] to release it. An injected
  /// client is never closed by the adapter.
  RestRemoteSyncAdapter({
    required String baseUrl,
    required Map<String, RestEntityConfig> entityMapping,
    http.Client? httpClient,
    Map<String, String> defaultHeaders = const {},
    this.requestTimeout = const Duration(seconds: 30),
    this.idempotencyHeader = 'Idempotency-Key',
    this.versionRequestHeader = 'If-Match',
    this.versionResponseHeader = 'ETag',
    this.expectedVersionKey = 'baseVersion',
  })  : _baseUri = _parseBaseUrl(baseUrl),
        _ownsClient = httpClient == null,
        _client = httpClient ?? http.Client(),
        _defaultHeaders = Map.unmodifiable(defaultHeaders),
        _entityMapping = _validatedMapping(entityMapping);

  final Uri _baseUri;
  final bool _ownsClient;
  final http.Client _client;
  final Map<String, String> _defaultHeaders;
  final Map<String, RestEntityConfig> _entityMapping;

  /// Maximum time to wait for a single HTTP response.
  final Duration requestTimeout;

  /// Header carrying `SyncOperation.idempotencyKey`.
  final String idempotencyHeader;

  /// Header carrying the expected version for conditional requests.
  final String versionRequestHeader;

  /// Response header surfacing the remote version/ETag.
  final String versionResponseHeader;

  /// `SyncOperation.metadata` key holding the expected version.
  final String expectedVersionKey;

  static Uri _parseBaseUrl(String baseUrl) {
    final uri = Uri.tryParse(baseUrl.trim());
    if (uri == null ||
        (uri.scheme != 'http' && uri.scheme != 'https') ||
        uri.host.isEmpty) {
      throw ArgumentError.value(
        baseUrl,
        'baseUrl',
        'Must be an absolute http:// or https:// URI.',
      );
    }
    var path = uri.path;
    if (!path.endsWith('/')) path = '$path/';
    return uri.replace(path: path);
  }

  static Map<String, RestEntityConfig> _validatedMapping(
    Map<String, RestEntityConfig> mapping,
  ) {
    if (mapping.isEmpty) {
      throw ArgumentError.value(
        mapping,
        'entityMapping',
        'Must contain at least one entityType mapping.',
      );
    }
    final result = <String, RestEntityConfig>{};
    for (final entry in mapping.entries) {
      if (entry.key.trim().isEmpty) {
        throw ArgumentError.value(
          entry.key,
          'entityMapping',
          'Entity type keys must not be blank.',
        );
      }
      result[entry.key] = entry.value;
    }
    return Map.unmodifiable(result);
  }

  /// Closes the underlying HTTP client only when the adapter created it.
  /// Injected clients remain owned by the caller.
  void close() {
    if (_ownsClient) _client.close();
  }

  @override
  Future<SyncResult> push(SyncOperation operation) async {
    final config = _entityMapping[operation.entityType];
    if (config == null) {
      return SyncFailure(
        operationId: operation.operationId,
        error: SyncError(
          code: SyncErrorCode.validation,
          message: 'No REST endpoint mapping configured for entityType '
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
      final failure = RestErrorMapper.tryMap(operation, error, stackTrace);
      if (failure != null) return failure;
      rethrow;
    }
  }

  // ------------------------------------------------------------------
  // Create
  // ------------------------------------------------------------------

  Future<SyncResult> _pushCreate(
    SyncOperation operation,
    RestEntityConfig config,
  ) async {
    final body = _writePayload(operation, config);
    final response = await _send(
      'POST',
      _uri(config.collectionPath),
      body: body,
      operation: operation,
    );
    return RestErrorMapper.mapHttpResponse(
      operation,
      response,
      versionResponseHeader: versionResponseHeader,
    );
  }

  // ------------------------------------------------------------------
  // Update
  // ------------------------------------------------------------------

  Future<SyncResult> _pushUpdate(
    SyncOperation operation,
    RestEntityConfig config,
  ) async {
    final body = _writePayload(operation, config);
    final method =
        config.updateMethod == RestUpdateMethod.put ? 'PUT' : 'PATCH';
    final response = await _send(
      method,
      _itemUri(config, operation.entityId),
      body: body,
      operation: operation,
    );
    return RestErrorMapper.mapHttpResponse(
      operation,
      response,
      versionResponseHeader: versionResponseHeader,
    );
  }

  // ------------------------------------------------------------------
  // Delete
  // ------------------------------------------------------------------

  Future<SyncResult> _pushDelete(
    SyncOperation operation,
    RestEntityConfig config,
  ) async {
    final response = await _send(
      'DELETE',
      _itemUri(config, operation.entityId),
      operation: operation,
    );
    // A 404 on DELETE means the target is already absent. Generic REST
    // cannot distinguish "never existed" from "hidden by authorization",
    // so absent-on-delete is treated as an idempotent success — the same
    // limitation documented for row-level security in other adapters.
    if (response.statusCode == 404) {
      return SyncSuccess(operationId: operation.operationId);
    }
    return RestErrorMapper.mapHttpResponse(
      operation,
      response,
      versionResponseHeader: versionResponseHeader,
    );
  }

  // ------------------------------------------------------------------
  // HTTP plumbing
  // ------------------------------------------------------------------

  Uri _uri(String path) => _baseUri.resolve(path);

  Uri _itemUri(RestEntityConfig config, String entityId) =>
      _uri(config.itemPath.replaceAll('{id}', Uri.encodeComponent(entityId)));

  Map<String, String> _headers(SyncOperation operation) {
    final headers = <String, String>{..._defaultHeaders};
    headers[idempotencyHeader] = operation.idempotencyKey;
    final expected = operation.metadata[expectedVersionKey];
    if (expected != null) {
      headers[versionRequestHeader] = expected.toString();
    }
    return headers;
  }

  Future<http.Response> _send(
    String method,
    Uri uri, {
    required SyncOperation operation,
    Map<String, dynamic>? body,
  }) async {
    final headers = _headers(operation);
    if (body != null) {
      headers['Content-Type'] = 'application/json';
    }

    final request = http.Request(method, uri)..headers.addAll(headers);
    if (body != null) {
      // bodyBytes (unlike body) never overrides the Content-Type header.
      request.bodyBytes = Uint8List.fromList(
        utf8.encode(jsonEncode(body)),
      );
    }

    final streamed = await _client.send(request).timeout(requestTimeout,
        onTimeout: () => throw TimeoutException(
              'Request to $method $uri exceeded $requestTimeout',
            ));
    return http.Response.fromStream(streamed);
  }

  /// Returns a mutable copy of the operation payload after validating
  /// caller-supplied values.
  ///
  /// When [RestEntityConfig.idField] is configured the entity ID is
  /// injected into the body under that field; a payload value that
  /// differs from `operation.entityId` is rejected rather than silently
  /// overridden.
  Map<String, dynamic> _writePayload(
    SyncOperation operation,
    RestEntityConfig config,
  ) {
    final payload = operation.payload;
    if (payload == null) {
      throw RestValidationException(
        'Payload is required for ${operation.type.name} operations.',
      );
    }
    final body = Map<String, dynamic>.from(payload);

    final idField = config.idField;
    if (idField != null) {
      final supplied = body[idField];
      if (supplied != null && supplied != operation.entityId) {
        throw RestValidationException(
          'Payload $idField ($supplied) does not match '
          'entityId (${operation.entityId}).',
        );
      }
      body[idField] = operation.entityId;
    }
    return body;
  }
}
