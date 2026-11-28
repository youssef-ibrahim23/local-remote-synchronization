import 'dart:async';

import 'package:flutter_test/flutter_test.dart';
import 'package:http/http.dart' as http;
import 'package:local_remote_supabase/src/supabase_error_mapper.dart';
import 'package:local_remote_supabase/src/sync_validation_exception.dart';
import 'package:local_remote_synchronization/local_remote_synchronization.dart';
import 'package:supabase_flutter/supabase_flutter.dart';

import 'support/supabase_test_harness.dart';

void main() {
  final operation = makeOperation(payload: {'title': 'x'});
  final stackTrace = StackTrace.current;

  SyncFailure? map(Object error) =>
      SupabaseErrorMapper.tryMap(operation, error, stackTrace);

  SyncError errorOf(Object e) {
    final result = map(e);
    expect(result, isNotNull, reason: '$e should be mappable');
    return result!.error;
  }

  group('transport errors', () {
    test('TimeoutException → timeout, retryable', () {
      final error = errorOf(TimeoutException('too slow'));
      expect(error.code, SyncErrorCode.timeout);
      expect(error.isRetryable, isTrue);
    });

    test('ClientException → network, retryable', () {
      final error = errorOf(
        http.ClientException('socket closed', Uri.parse('https://x.co')),
      );
      expect(error.code, SyncErrorCode.network);
      expect(error.isRetryable, isTrue);
    });

    test('RequestAbortedException → cancelled (not network)', () {
      final error = errorOf(RequestAbortedException());
      expect(error.code, SyncErrorCode.cancelled);
      expect(error.isRetryable, isFalse);
    });

    test('AuthRetryableFetchException → network, retryable', () {
      final error =
          errorOf(AuthRetryableFetchException(message: 'fetch failed'));
      expect(error.code, SyncErrorCode.network);
      expect(error.isRetryable, isTrue);
    });
  });

  group('auth errors', () {
    for (final (status, expected, retryable) in [
      ('401', SyncErrorCode.unauthorized, false),
      ('403', SyncErrorCode.forbidden, false),
      ('429', SyncErrorCode.rateLimited, true),
      ('500', SyncErrorCode.serverError, true),
      ('503', SyncErrorCode.serverError, true),
      ('400', SyncErrorCode.validation, false),
      ('422', SyncErrorCode.validation, false),
      ('418', SyncErrorCode.unknown, false),
      (null, SyncErrorCode.unknown, false),
      ('not-a-status', SyncErrorCode.unknown, false),
    ]) {
      test('AuthException status "$status" → ${expected.name}', () {
        final error =
            errorOf(AuthException('auth failure', statusCode: status));
        expect(error.code, expected);
        expect(error.isRetryable, retryable);
        expect(error.statusCode, int.tryParse(status ?? ''));
      });
    }

    test('AuthApiException follows the same status mapping', () {
      final forbidden = errorOf(
        const AuthApiException('denied', statusCode: '403'),
      );
      expect(forbidden.code, SyncErrorCode.forbidden);

      final rateLimited = errorOf(
        const AuthApiException('slow down', statusCode: '429'),
      );
      expect(rateLimited.code, SyncErrorCode.rateLimited);
      expect(rateLimited.isRetryable, isTrue);
    });
  });

  group('PostgrestException — HTTP status codes (non-JSON bodies)', () {
    for (final (status, code, retryable) in [
      ('400', SyncErrorCode.validation, false),
      ('401', SyncErrorCode.unauthorized, false),
      ('403', SyncErrorCode.forbidden, false),
      ('404', SyncErrorCode.notFound, false),
      ('409', SyncErrorCode.conflict, false),
      ('429', SyncErrorCode.rateLimited, true),
      ('500', SyncErrorCode.serverError, true),
      ('503', SyncErrorCode.serverError, true),
      ('200', SyncErrorCode.serialization, false),
      ('418', SyncErrorCode.unknown, false),
    ]) {
      test('"$status" → ${code.name}, retryable=$retryable', () {
        final error = errorOf(
          PostgrestException(message: 'err', code: status),
        );
        expect(error.code, code);
        expect(error.isRetryable, retryable);
        expect(error.statusCode, int.tryParse(status));
      });
    }
  });

  group('PostgrestException — SQLSTATE / PGRST codes', () {
    for (final (code, expected, retryable) in [
      ('23505', SyncErrorCode.conflict, false),
      ('23503', SyncErrorCode.validation, false),
      ('23502', SyncErrorCode.validation, false),
      ('23514', SyncErrorCode.validation, false),
      ('42501', SyncErrorCode.forbidden, false),
      ('40001', SyncErrorCode.serverError, true),
      ('40P01', SyncErrorCode.serverError, true),
      ('22P02', SyncErrorCode.validation, false),
      ('42703', SyncErrorCode.validation, false),
      ('PGRST000', SyncErrorCode.network, true),
      ('PGRST003', SyncErrorCode.serverError, true),
      ('PGRST116', SyncErrorCode.notFound, false),
      ('PGRST204', SyncErrorCode.validation, false),
      ('PGRST301', SyncErrorCode.unauthorized, false),
      ('PGRST999', SyncErrorCode.validation, false),
      ('XX000', SyncErrorCode.unknown, false),
    ]) {
      test('"$code" → ${expected.name}, retryable=$retryable', () {
        final error = errorOf(
          PostgrestException(message: 'err', code: code),
        );
        expect(error.code, expected);
        expect(error.isRetryable, retryable);
        expect(error.statusCode, isNull);
      });
    }

    test('preserves backend message, cause and stack trace', () {
      const cause = PostgrestException(
        message: 'duplicate key value',
        code: '23505',
      );
      final failure = map(cause)!;
      expect(failure.error.message, 'duplicate key value');
      expect(failure.error.cause, same(cause));
      expect(failure.error.stackTrace, same(stackTrace));
      expect(failure.operationId, operation.operationId);
    });
  });

  group('other errors', () {
    test('FormatException → serialization, non-retryable', () {
      final error = errorOf(const FormatException('bad json'));
      expect(error.code, SyncErrorCode.serialization);
      expect(error.isRetryable, isFalse);
    });

    test('TypeError → serialization, non-retryable', () {
      final error = errorOf(TypeError());
      expect(error.code, SyncErrorCode.serialization);
    });

    test('SyncValidationException → validation, non-retryable', () {
      final error = errorOf(SyncValidationException('pk mismatch'));
      expect(error.code, SyncErrorCode.validation);
      expect(error.isRetryable, isFalse);
      expect(error.message, 'pk mismatch');
    });

    test('unrecognised error returns null so caller can rethrow', () {
      expect(map(ArgumentError('bug')), isNull);
      expect(map(StateError('bug')), isNull);
      expect(map(Exception('mystery')), isNull);
    });
  });
}
