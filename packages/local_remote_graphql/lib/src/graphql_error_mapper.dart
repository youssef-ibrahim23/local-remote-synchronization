import 'dart:async';
import 'dart:io';

import 'package:graphql/client.dart';
import 'package:http/http.dart' as http;
import 'package:local_remote_synchronization/local_remote_synchronization.dart';

import 'graphql_validation_exception.dart';

/// Maps GraphQL transport/execution failures to Core [SyncResult] values.
///
/// Returns `null` for errors that are not expected remote failures so the
/// caller can rethrow unexpected programming errors.
abstract final class GraphQLErrorMapper {
  /// Attempts to map [error] to a [SyncResult] for [operation].
  static SyncResult? tryMap(
    SyncOperation operation,
    Object error,
    StackTrace stackTrace,
  ) {
    // Dart `Error` subtypes (StateError, TypeError, ...) signal
    // programming bugs, not remote failures — let them propagate.
    if (error is Error && error is! Exception) {
      return null;
    }

    if (error is GraphQLValidationException) {
      return _failure(
        operation,
        SyncErrorCode.validation,
        error.message,
        retryable: false,
        cause: error,
        stackTrace: stackTrace,
      );
    }

    if (error is OperationException) {
      return _mapOperationException(operation, error, stackTrace);
    }

    if (error is ServerException) {
      return _mapServerException(operation, error, stackTrace);
    }

    if (error is LinkException) {
      return _mapLinkException(operation, error, stackTrace);
    }

    if (error is TimeoutException) {
      return _failure(
        operation,
        SyncErrorCode.timeout,
        'GraphQL mutation timed out.',
        retryable: true,
        cause: error,
        stackTrace: stackTrace,
      );
    }

    if (error is SocketException ||
        error is HandshakeException ||
        error is http.ClientException) {
      return _failure(
        operation,
        SyncErrorCode.network,
        'Network failure while executing GraphQL mutation.',
        retryable: true,
        cause: error,
        stackTrace: stackTrace,
      );
    }

    return null;
  }

  static SyncResult? _mapOperationException(
    SyncOperation operation,
    OperationException exception,
    StackTrace stackTrace,
  ) {
    final linkException = exception.linkException;
    if (linkException != null) {
      final mapped = tryMap(operation, linkException, stackTrace);
      // A null result means the link failure was a programming error;
      // propagate it instead of disguising it as a sync failure.
      return mapped ??
          (linkException.originalException is Error &&
                  linkException.originalException is! Exception
              ? null
              : _failure(
                  operation,
                  SyncErrorCode.unknown,
                  'GraphQL link failure: '
                  '${linkException.originalException}',
                  retryable: false,
                  cause: exception,
                  stackTrace: stackTrace,
                ));
    }

    if (exception.graphqlErrors.isNotEmpty) {
      return _mapGraphQLError(
          operation, exception.graphqlErrors.first, stackTrace);
    }

    return _failure(
      operation,
      SyncErrorCode.unknown,
      'GraphQL operation failed without error details.',
      retryable: false,
      cause: exception,
      stackTrace: stackTrace,
    );
  }

  static SyncResult _mapServerException(
    SyncOperation operation,
    ServerException exception,
    StackTrace stackTrace,
  ) {
    final statusCode = exception.statusCode;
    if (statusCode != null) {
      return _mapHttpStatus(
        operation,
        statusCode,
        exception,
        stackTrace,
        retryAfter: _retryAfter(exception),
      );
    }

    final parsedErrors = exception.parsedResponse?.errors;
    if (parsedErrors != null && parsedErrors.isNotEmpty) {
      return _mapGraphQLError(operation, parsedErrors.first, stackTrace);
    }

    return _failure(
      operation,
      SyncErrorCode.serverError,
      'GraphQL server failure.',
      retryable: true,
      cause: exception,
      stackTrace: stackTrace,
    );
  }

  static SyncResult? _mapLinkException(
    SyncOperation operation,
    LinkException exception,
    StackTrace stackTrace,
  ) {
    final original = exception.originalException;
    if (original is Error && original is! Exception) {
      return null;
    }
    if (original != null) {
      final mapped = tryMap(
        operation,
        original,
        exception.originalStackTrace ?? stackTrace,
      );
      if (mapped != null) return mapped;
    }

    return _failure(
      operation,
      SyncErrorCode.unknown,
      'GraphQL link failure: $original',
      retryable: false,
      cause: exception,
      stackTrace: stackTrace,
    );
  }

  /// Maps an HTTP status code carried by a [ServerException].
  static SyncResult _mapHttpStatus(
    SyncOperation operation,
    int statusCode,
    ServerException exception,
    StackTrace stackTrace, {
    Duration? retryAfter,
  }) {
    final (code, retryable, message) = switch (statusCode) {
      401 => (
          SyncErrorCode.unauthorized,
          false,
          'GraphQL request was not '
              'authenticated (HTTP 401).'
        ),
      403 => (
          SyncErrorCode.forbidden,
          false,
          'GraphQL request was forbidden '
              '(HTTP 403).'
        ),
      404 => (
          SyncErrorCode.notFound,
          false,
          'GraphQL endpoint or resource '
              'was not found (HTTP 404).'
        ),
      409 || 412 => (
          SyncErrorCode.conflict,
          false,
          'GraphQL request '
              'conflicted with remote state (HTTP $statusCode).'
        ),
      429 => (
          SyncErrorCode.rateLimited,
          true,
          'GraphQL request was rate '
              'limited (HTTP 429).'
        ),
      >= 500 => (
          SyncErrorCode.serverError,
          true,
          'GraphQL server error '
              '(HTTP $statusCode).'
        ),
      _ => (
          SyncErrorCode.unknown,
          false,
          'Unexpected HTTP status $statusCode '
              'from GraphQL endpoint.'
        ),
    };

    return _failure(
      operation,
      code,
      message,
      retryable: retryable,
      retryAfter: retryAfter,
      statusCode: statusCode,
      cause: exception,
      stackTrace: stackTrace,
    );
  }

  /// Maps a single [GraphQLError] using `extensions.code` when present.
  static SyncResult _mapGraphQLError(
    SyncOperation operation,
    GraphQLError error,
    StackTrace stackTrace,
  ) {
    final extensions = error.extensions;
    final rawCode = extensions?['code'];
    final code = rawCode is String ? rawCode.toUpperCase() : '';
    final message =
        error.message.isNotEmpty ? error.message : 'GraphQL mutation failed.';

    SyncFailure failure(SyncErrorCode errorCode, {required bool retryable}) =>
        _failure(
          operation,
          errorCode,
          message,
          retryable: retryable,
          cause: error,
          stackTrace: stackTrace,
        );

    return switch (code) {
      'UNAUTHENTICATED' ||
      'UNAUTHORIZED' =>
        failure(SyncErrorCode.unauthorized, retryable: false),
      'FORBIDDEN' ||
      'PERMISSION_DENIED' =>
        failure(SyncErrorCode.forbidden, retryable: false),
      'NOT_FOUND' => failure(SyncErrorCode.notFound, retryable: false),
      'CONFLICT' || 'VERSION_CONFLICT' => _conflictFromError(
          operation,
          error,
          message,
          stackTrace,
        ),
      'BAD_USER_INPUT' ||
      'VALIDATION_ERROR' ||
      'INVALID_ARGUMENT' ||
      'GRAPHQL_VALIDATION_FAILED' =>
        failure(SyncErrorCode.validation, retryable: false),
      'RATE_LIMITED' ||
      'TOO_MANY_REQUESTS' ||
      'THROTTLED' =>
        failure(SyncErrorCode.rateLimited, retryable: true),
      'SERVICE_UNAVAILABLE' ||
      'UNAVAILABLE' =>
        failure(SyncErrorCode.network, retryable: true),
      'DEADLINE_EXCEEDED' ||
      'TIMEOUT' ||
      'TIMED_OUT' =>
        failure(SyncErrorCode.timeout, retryable: true),
      'INTERNAL_SERVER_ERROR' ||
      'INTERNAL' =>
        failure(SyncErrorCode.serverError, retryable: true),
      'CANCELLED' ||
      'CANCELED' =>
        failure(SyncErrorCode.cancelled, retryable: false),
      _ => failure(SyncErrorCode.unknown, retryable: false),
    };
  }

  /// Builds a [SyncConflict] when the error extensions carry a usable
  /// remote snapshot; otherwise falls back to a non-retryable conflict
  /// [SyncFailure].
  static SyncResult _conflictFromError(
    SyncOperation operation,
    GraphQLError error,
    String message,
    StackTrace stackTrace,
  ) {
    final extensions = error.extensions;
    final snapshot = extensions?['remoteSnapshot'];
    if (snapshot is Map) {
      final version = extensions?['remoteVersion'] ?? extensions?['version'];
      return SyncConflict(
        operationId: operation.operationId,
        remoteSnapshot: Map<String, dynamic>.from(snapshot),
        remoteVersion: version?.toString(),
      );
    }

    return _failure(
      operation,
      SyncErrorCode.conflict,
      message,
      retryable: false,
      cause: error,
      stackTrace: stackTrace,
    );
  }

  /// Extracts a `Retry-After` hint (seconds) from an
  /// [HttpLinkServerException]'s underlying HTTP response.
  static Duration? _retryAfter(ServerException exception) {
    final response =
        exception is HttpLinkServerException ? exception.response : null;
    final value = response?.headers['retry-after'];
    if (value == null) return null;
    final seconds = int.tryParse(value.trim());
    return seconds == null || seconds < 0 ? null : Duration(seconds: seconds);
  }

  static SyncFailure _failure(
    SyncOperation operation,
    SyncErrorCode code,
    String message, {
    required bool retryable,
    Duration? retryAfter,
    int? statusCode,
    Object? cause,
    StackTrace? stackTrace,
  }) =>
      SyncFailure(
        operationId: operation.operationId,
        error: SyncError(
          code: code,
          message: message,
          isRetryable: retryable,
          retryAfter: retryAfter,
          statusCode: statusCode,
          cause: cause,
          stackTrace: stackTrace,
        ),
      );
}
