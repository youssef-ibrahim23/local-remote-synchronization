import 'dart:async';

import 'package:http/http.dart' as http;
import 'package:local_remote_synchronization/local_remote_synchronization.dart';
import 'package:supabase_flutter/supabase_flutter.dart';

import 'sync_validation_exception.dart';

/// Converts Supabase/PostgREST/network exceptions into structured Core
/// [SyncFailure] values.
///
/// Returns `null` for errors that are not recognised backend failures so
/// the caller can let genuine programming errors propagate instead of
/// disguising them as retryable network failures.
abstract final class SupabaseErrorMapper {
  /// Maps [error] to a [SyncFailure] for [operation], or returns `null`
  /// when the error is not a recognised, mappable backend failure.
  static SyncFailure? tryMap(
    SyncOperation operation,
    Object error,
    StackTrace stackTrace,
  ) {
    final SyncErrorCode? code;
    final bool retryable;
    final int? statusCode;
    final String message;

    if (error is SyncValidationException) {
      code = SyncErrorCode.validation;
      retryable = false;
      statusCode = null;
      message = error.message;
    } else if (error is TimeoutException) {
      code = SyncErrorCode.timeout;
      retryable = true;
      statusCode = null;
      message = 'Supabase request timed out: ${error.message}';
    } else if (error is AuthRetryableFetchException) {
      code = SyncErrorCode.network;
      retryable = true;
      statusCode = _parseStatus(error.statusCode);
      message = 'Supabase authentication request failed: ${error.message}';
    } else if (error is AuthException) {
      final mapped = _mapAuthException(error);
      code = mapped.code;
      retryable = mapped.retryable;
      statusCode = mapped.statusCode;
      message = 'Supabase authentication failed: ${error.message}';
    } else if (error is RequestAbortedException) {
      // Checked before ClientException: RequestAbortedException is a
      // ClientException subtype.
      code = SyncErrorCode.cancelled;
      retryable = false;
      statusCode = null;
      message = 'Supabase request was aborted.';
    } else if (error is http.ClientException) {
      code = SyncErrorCode.network;
      retryable = true;
      statusCode = null;
      message = 'Supabase network request failed: ${error.message}';
    } else if (error is PostgrestException) {
      final mapped = _mapPostgrest(error);
      code = mapped.code;
      retryable = mapped.retryable;
      statusCode = mapped.statusCode;
      message =
          error.message.isEmpty ? 'Supabase request failed.' : error.message;
    } else if (error is FormatException || error is TypeError) {
      code = SyncErrorCode.serialization;
      retryable = false;
      statusCode = null;
      message = 'Failed to parse Supabase response: ${error.runtimeType}';
    } else {
      return null;
    }

    return SyncFailure(
      operationId: operation.operationId,
      error: SyncError(
        code: code,
        message: message,
        isRetryable: retryable,
        statusCode: statusCode,
        cause: error,
        stackTrace: stackTrace,
      ),
    );
  }

  static int? _parseStatus(String? statusCode) {
    if (statusCode == null) return null;
    return int.tryParse(statusCode);
  }

  /// Maps [AuthException] by its HTTP status code. Auth errors without a
  /// recognisable status (including `null`) are `unknown` and
  /// non-retryable — only `AuthRetryableFetchException` (handled
  /// earlier) is treated as a retryable transport failure.
  static ({SyncErrorCode code, bool retryable, int? statusCode})
      _mapAuthException(AuthException error) {
    final status = _parseStatus(error.statusCode);
    return switch (status) {
      401 => (
          code: SyncErrorCode.unauthorized,
          retryable: false,
          statusCode: status,
        ),
      403 => (
          code: SyncErrorCode.forbidden,
          retryable: false,
          statusCode: status,
        ),
      429 => (
          code: SyncErrorCode.rateLimited,
          retryable: true,
          statusCode: status,
        ),
      400 || 422 => (
          code: SyncErrorCode.validation,
          retryable: false,
          statusCode: status,
        ),
      _ when status != null && status >= 500 && status <= 599 => (
          code: SyncErrorCode.serverError,
          retryable: true,
          statusCode: status,
        ),
      _ => (
          code: SyncErrorCode.unknown,
          retryable: false,
          statusCode: status,
        ),
    };
  }

  static ({SyncErrorCode code, bool retryable, int? statusCode}) _mapPostgrest(
    PostgrestException error,
  ) {
    final rawCode = error.code;

    // Non-JSON error bodies surface the HTTP status code as a numeric
    // string (e.g. '500', '429'). JSON bodies carry a SQLSTATE or PGRST
    // code instead — SQLSTATEs are five digits (e.g. '23505', '42501')
    // and must not be mistaken for HTTP statuses, so only three-digit
    // codes in the valid HTTP range take this branch.
    final status = int.tryParse(rawCode ?? '');
    if (status != null && status >= 100 && status <= 599) {
      return switch (status) {
        400 || 422 => (
            code: SyncErrorCode.validation,
            retryable: false,
            statusCode: status,
          ),
        401 => (
            code: SyncErrorCode.unauthorized,
            retryable: false,
            statusCode: status,
          ),
        403 => (
            code: SyncErrorCode.forbidden,
            retryable: false,
            statusCode: status,
          ),
        404 => (
            code: SyncErrorCode.notFound,
            retryable: false,
            statusCode: status,
          ),
        405 || 409 || 410 => (
            code: SyncErrorCode.conflict,
            retryable: false,
            statusCode: status,
          ),
        406 => (
            code: SyncErrorCode.validation,
            retryable: false,
            statusCode: status,
          ),
        // A 2xx status with an unparseable body surfaces as a
        // PostgrestException whose code is the status string.
        >= 200 && <= 299 => (
            code: SyncErrorCode.serialization,
            retryable: false,
            statusCode: status,
          ),
        429 => (
            code: SyncErrorCode.rateLimited,
            retryable: true,
            statusCode: status,
          ),
        >= 500 && <= 599 => (
            code: SyncErrorCode.serverError,
            retryable: true,
            statusCode: status,
          ),
        _ => (
            code: SyncErrorCode.unknown,
            retryable: false,
            statusCode: status,
          ),
      };
    }

    return switch (rawCode) {
      // SQLSTATE classes.
      '23505' => (
          code: SyncErrorCode.conflict,
          retryable: false,
          statusCode: null,
        ),
      '42501' => (
          code: SyncErrorCode.forbidden,
          retryable: false,
          statusCode: null,
        ),
      '40001' || '40P01' => (
          code: SyncErrorCode.serverError,
          retryable: true,
          statusCode: null,
        ),
      // Integrity-constraint, data-exception, syntax, and undefined-object
      // violations are schema/contract problems — never retryable.
      'PGRST000' => (
          code: SyncErrorCode.network,
          retryable: true,
          statusCode: null,
        ),
      'PGRST003' => (
          code: SyncErrorCode.serverError,
          retryable: true,
          statusCode: null,
        ),
      'PGRST116' => (
          code: SyncErrorCode.notFound,
          retryable: false,
          statusCode: null,
        ),
      'PGRST301' || 'PGRST302' => (
          code: SyncErrorCode.unauthorized,
          retryable: false,
          statusCode: null,
        ),
      _ when _isClientSideSqlState(rawCode) => (
          code: SyncErrorCode.validation,
          retryable: false,
          statusCode: null,
        ),
      _ when rawCode != null && rawCode.startsWith('PGRST') => (
          code: SyncErrorCode.validation,
          retryable: false,
          statusCode: null,
        ),
      _ => (
          code: SyncErrorCode.unknown,
          retryable: false,
          statusCode: null,
        ),
    };
  }

  /// SQLSTATE classes that indicate a malformed request or schema
  /// mismatch (class 22 data exception, 23 integrity constraint, 42
  /// syntax/access-rule) — always a client-side, non-retryable problem.
  static bool _isClientSideSqlState(String? code) {
    if (code == null || code.length != 5) return false;
    final cls = code.substring(0, 2);
    return cls == '22' || cls == '23' || cls == '42';
  }
}
