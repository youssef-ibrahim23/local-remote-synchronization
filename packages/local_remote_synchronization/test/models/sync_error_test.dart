import 'package:local_remote_synchronization/local_remote_synchronization.dart';
import 'package:test/test.dart';

void main() {
  group('SyncError', () {
    test('validates message and retryAfter', () {
      expect(
        () => SyncError(
          code: SyncErrorCode.network,
          message: '   ',
          isRetryable: true,
        ),
        throwsArgumentError,
      );
      expect(
        () => SyncError(
          code: SyncErrorCode.rateLimited,
          message: 'Too many requests',
          isRetryable: true,
          retryAfter: const Duration(seconds: -1),
        ),
        throwsArgumentError,
      );
    });

    test('serializes persistable fields and excludes diagnostics', () {
      final error = SyncError(
        code: SyncErrorCode.rateLimited,
        message: 'Rate limited',
        isRetryable: true,
        retryAfter: const Duration(seconds: 15),
        statusCode: 429,
        cause: StateError('socket closed'),
        stackTrace: StackTrace.current,
      );

      final json = error.toJson();
      expect(json, <String, dynamic>{
        'code': 'rateLimited',
        'message': 'Rate limited',
        'isRetryable': true,
        'retryAfterMs': 15000,
        'statusCode': 429,
      });
      expect(json.containsKey('cause'), isFalse);
      expect(json.containsKey('stackTrace'), isFalse);

      final decoded = SyncError.fromJson(json);
      expect(decoded, equals(error));
      expect(decoded.hashCode, equals(error.hashCode));
      expect(decoded.cause, isNull);
      expect(decoded.stackTrace, isNull);
    });

    test('equality ignores cause and stackTrace', () {
      final a = SyncError(
        code: SyncErrorCode.serverError,
        message: 'Internal error',
        isRetryable: true,
        statusCode: 500,
        cause: Exception('boom'),
        stackTrace: StackTrace.current,
      );
      final b = SyncError(
        code: SyncErrorCode.serverError,
        message: 'Internal error',
        isRetryable: true,
        statusCode: 500,
      );

      expect(a, equals(b));
      expect(a.hashCode, equals(b.hashCode));
    });

    test('fromJson throws FormatException on invalid JSON', () {
      expect(
        () => SyncError.fromJson(<String, dynamic>{
          'code': 'notARealCode',
          'message': 'Oops',
          'isRetryable': false,
        }),
        throwsFormatException,
      );
      expect(
        () => SyncError.fromJson(<String, dynamic>{
          'code': 'network',
          'message': 123,
          'isRetryable': true,
        }),
        throwsFormatException,
      );
      expect(
        () => SyncError.fromJson(<String, dynamic>{
          'code': 'network',
          'message': 'Offline',
          'isRetryable': 'yes',
        }),
        throwsFormatException,
      );
    });
  });
}
