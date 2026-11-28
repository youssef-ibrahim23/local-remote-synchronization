import 'dart:async';
import 'dart:io';

import 'package:firebase_core/firebase_core.dart';
import 'package:local_remote_synchronization/local_remote_synchronization.dart';

import 'firestore_validation_exception.dart';

/// Maps exceptions thrown while talking to Cloud Firestore into Core
/// [SyncFailure] results.
///
/// Returns `null` for errors that are not expected Firestore/transport
/// failures so the caller can rethrow them as programming errors.
abstract final class FirestoreErrorMapper {
  /// Attempts to convert [error] into a [SyncFailure] for [operation].
  static SyncFailure? tryMap(
    SyncOperation operation,
    Object error,
    StackTrace stackTrace,
  ) {
    if (error is FirestoreValidationException) {
      return _failure(
        operation,
        code: SyncErrorCode.validation,
        message: error.message,
        isRetryable: false,
        cause: error,
        stackTrace: stackTrace,
      );
    }
    if (error is TimeoutException) {
      return _failure(
        operation,
        code: SyncErrorCode.timeout,
        message: 'Firestore request timed out.',
        isRetryable: true,
        cause: error,
        stackTrace: stackTrace,
      );
    }
    if (error is FirebaseException) {
      return _mapFirebase(operation, error, stackTrace);
    }
    if (error is SocketException || error is HandshakeException) {
      return _failure(
        operation,
        code: SyncErrorCode.network,
        message: 'Network failure: $error',
        isRetryable: true,
        cause: error,
        stackTrace: stackTrace,
      );
    }
    return null;
  }

  static SyncFailure _mapFirebase(
    SyncOperation operation,
    FirebaseException error,
    StackTrace stackTrace,
  ) {
    final (code, retryable) = switch (error.code) {
      'unauthenticated' => (SyncErrorCode.unauthorized, false),
      'permission-denied' => (SyncErrorCode.forbidden, false),
      'not-found' => (SyncErrorCode.notFound, false),
      'already-exists' => (SyncErrorCode.conflict, false),
      'aborted' => (SyncErrorCode.conflict, false),
      'failed-precondition' => (SyncErrorCode.conflict, false),
      'invalid-argument' => (SyncErrorCode.validation, false),
      'resource-exhausted' => (SyncErrorCode.rateLimited, true),
      'unavailable' => (SyncErrorCode.network, true),
      'deadline-exceeded' => (SyncErrorCode.timeout, true),
      'cancelled' => (SyncErrorCode.cancelled, false),
      'internal' || 'data-loss' || 'unknown' => (
          SyncErrorCode.serverError,
          true,
        ),
      _ => (SyncErrorCode.unknown, false),
    };
    return _failure(
      operation,
      code: code,
      message: error.message ?? 'Firestore error (${error.code}).',
      isRetryable: retryable,
      cause: error,
      stackTrace: stackTrace,
    );
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
