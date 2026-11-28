import 'dart:async';
import 'dart:convert';

import 'package:http/http.dart' as http;
import 'package:local_remote_synchronization/local_remote_synchronization.dart';

import 'rest_validation_exception.dart';

/// Maps exceptions thrown while talking to a REST backend into Core
/// [SyncFailure] results.
///
/// Returns `null` for errors that are not expected backend/transport
/// failures so the caller can rethrow them as programming errors.
abstract final class RestErrorMapper {
  /// Attempts to convert [error] into a [SyncFailure] for [operation].
  static SyncFailure? tryMap(
    SyncOperation operation,
    Object error,
    StackTrace stackTrace,
  ) {
    if (error is RestValidationException) {
      return SyncFailure(
        operationId: operation.operationId,
        error: SyncError(
          code: SyncErrorCode.validation,
          message: error.message,
          isRetryable: false,
          cause: error,
          stackTrace: stackTrace,
        ),
      );
    }
    if (error is TimeoutException) {
      return _failure(
        operation,
        code: SyncErrorCode.timeout,
        message: 'Request timed out.',
        isRetryable: true,
        cause: error,
        stackTrace: stackTrace,
      );
    }
    if (error is http.ClientException) {
      return _failure(
        operation,
        code: SyncErrorCode.network,
        message: 'Network failure: ${error.message}',
        isRetryable: true,
        cause: error,
        stackTrace: stackTrace,
      );
    }
    if (error is FormatException) {
      return _failure(
        operation,
        code: SyncErrorCode.serialization,
        message: 'Malformed response: ${error.message}',
        isRetryable: false,
        cause: error,
        stackTrace: stackTrace,
      );
    }
    return null;
  }

  /// Maps a non-2xx [http.Response] to a [SyncResult].
  ///
  /// `409`/`412` produce [SyncConflict] when the response body is a JSON
  /// object (used as the remote snapshot); otherwise an empty snapshot is
  /// reported because generic REST responses do not guarantee entity
  /// state.
  static SyncResult mapHttpResponse(
    SyncOperation operation,
    http.Response response, {
    required String versionResponseHeader,
  }) {
    final status = response.statusCode;
    final body = response.body;

    if (status >= 200 && status < 300) {
      return _success(operation, response, body, versionResponseHeader);
    }

    final message = _errorMessage(response);
    final version = _version(response, versionResponseHeader);

    if (status == 409 || status == 412) {
      final snapshot = _jsonObject(body);
      return SyncConflict(
        operationId: operation.operationId,
        remoteSnapshot: snapshot ?? const <String, dynamic>{},
        remoteVersion: version,
      );
    }

    final (code, retryable) = switch (status) {
      400 || 422 => (SyncErrorCode.validation, false),
      401 => (SyncErrorCode.unauthorized, false),
      403 => (SyncErrorCode.forbidden, false),
      404 => (SyncErrorCode.notFound, false),
      408 => (SyncErrorCode.timeout, true),
      429 => (SyncErrorCode.rateLimited, true),
      >= 500 => (SyncErrorCode.serverError, true),
      _ => (SyncErrorCode.unknown, false),
    };

    return SyncFailure(
      operationId: operation.operationId,
      error: SyncError(
        code: code,
        message: message,
        isRetryable: retryable,
        retryAfter: _retryAfter(response),
        statusCode: status,
      ),
    );
  }

  static SyncResult _success(
    SyncOperation operation,
    http.Response response,
    String body,
    String versionResponseHeader,
  ) {
    Map<String, dynamic>? syncedData;
    if (body.trim().isNotEmpty) {
      final decoded = jsonDecode(body);
      if (decoded is! Map<String, dynamic>) {
        // A JSON array/scalar is valid JSON but not an entity snapshot.
        return SyncFailure(
          operationId: operation.operationId,
          error: SyncError(
            code: SyncErrorCode.serialization,
            message: 'Expected a JSON object response body, got '
                '${decoded.runtimeType}.',
            isRetryable: false,
            statusCode: response.statusCode,
          ),
        );
      }
      syncedData = decoded;
    }
    return SyncSuccess(
      operationId: operation.operationId,
      remoteVersion: _version(response, versionResponseHeader),
      syncedData: syncedData,
    );
  }

  static Map<String, dynamic>? _jsonObject(String body) {
    try {
      final decoded = jsonDecode(body);
      return decoded is Map<String, dynamic> ? decoded : null;
    } catch (_) {
      return null;
    }
  }

  /// Extracts a human-readable message from an error response without
  /// exposing headers or credentials.
  static String _errorMessage(http.Response response) {
    final body = response.body.trim();
    if (body.isNotEmpty) {
      try {
        final decoded = jsonDecode(body);
        if (decoded is Map) {
          for (final key in const [
            'message',
            'error',
            'detail',
            'title',
          ]) {
            final value = decoded[key];
            if (value is String && value.trim().isNotEmpty) {
              return value;
            }
          }
        }
      } catch (_) {
        // Non-JSON error body — fall through to the raw text.
      }
      final excerpt = body.length > 200 ? '${body.substring(0, 200)}…' : body;
      return 'HTTP ${response.statusCode}: $excerpt';
    }
    return 'HTTP ${response.statusCode} ${response.reasonPhrase}'.trim();
  }

  /// HTTP header names are case-insensitive; `http.Response.headers`
  /// preserves the server's original casing, so lookups must be.
  static String? _header(http.Response response, String name) {
    final lower = name.toLowerCase();
    for (final entry in response.headers.entries) {
      if (entry.key.toLowerCase() == lower) return entry.value;
    }
    return null;
  }

  static String? _version(http.Response response, String header) {
    final value = _header(response, header);
    return (value == null || value.isEmpty) ? null : value;
  }

  static Duration? _retryAfter(http.Response response) {
    final value = _header(response, 'retry-after');
    if (value == null) return null;
    final seconds = int.tryParse(value.trim());
    if (seconds != null && seconds >= 0) {
      return Duration(seconds: seconds);
    }
    return null;
  }

  static SyncFailure _failure(
    SyncOperation operation, {
    required SyncErrorCode code,
    required String message,
    required bool isRetryable,
    Object? cause,
    StackTrace? stackTrace,
  }) =>
      SyncFailure(
        operationId: operation.operationId,
        error: SyncError(
          code: code,
          message: message,
          isRetryable: isRetryable,
          cause: cause,
          stackTrace: stackTrace,
        ),
      );
}
