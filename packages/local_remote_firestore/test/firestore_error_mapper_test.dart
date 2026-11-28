import 'dart:async';
import 'dart:io';

import 'package:firebase_core/firebase_core.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:local_remote_firestore/src/firestore_error_mapper.dart';
import 'package:local_remote_firestore/src/firestore_validation_exception.dart';
import 'package:local_remote_synchronization/local_remote_synchronization.dart';

import 'support/firestore_test_harness.dart';

void main() {
  final operation = makeOperation(payload: {});

  SyncFailure? map(Object error) =>
      FirestoreErrorMapper.tryMap(operation, error, StackTrace.current);

  void expectMapped(
    Object error, {
    required SyncErrorCode code,
    required bool retryable,
  }) {
    final failure = map(error);
    expect(failure, isNotNull, reason: '$error should map');
    expect(failure!.error.code, code, reason: '$error');
    expect(failure.error.isRetryable, retryable, reason: '$error');
    expect(failure.operationId, 'op-1');
  }

  group('FirebaseException codes', () {
    FirebaseException firebase(String code, [String? message]) =>
        FirebaseException(
          plugin: 'cloud_firestore',
          code: code,
          message: message,
        );

    test('unauthenticated → unauthorized, non-retryable', () {
      expectMapped(
        firebase('unauthenticated', 'bad token'),
        code: SyncErrorCode.unauthorized,
        retryable: false,
      );
    });

    test('permission-denied → forbidden, non-retryable', () {
      expectMapped(
        firebase('permission-denied', 'denied'),
        code: SyncErrorCode.forbidden,
        retryable: false,
      );
    });

    test('not-found → notFound, non-retryable', () {
      expectMapped(
        firebase('not-found'),
        code: SyncErrorCode.notFound,
        retryable: false,
      );
    });

    test('already-exists → conflict, non-retryable', () {
      expectMapped(
        firebase('already-exists'),
        code: SyncErrorCode.conflict,
        retryable: false,
      );
    });

    test('aborted → conflict, non-retryable', () {
      expectMapped(
        firebase('aborted'),
        code: SyncErrorCode.conflict,
        retryable: false,
      );
    });

    test('failed-precondition → conflict, non-retryable', () {
      expectMapped(
        firebase('failed-precondition'),
        code: SyncErrorCode.conflict,
        retryable: false,
      );
    });

    test('invalid-argument → validation, non-retryable', () {
      expectMapped(
        firebase('invalid-argument', 'bad field'),
        code: SyncErrorCode.validation,
        retryable: false,
      );
    });

    test('resource-exhausted → rateLimited, retryable', () {
      expectMapped(
        firebase('resource-exhausted'),
        code: SyncErrorCode.rateLimited,
        retryable: true,
      );
    });

    test('unavailable → network, retryable', () {
      expectMapped(
        firebase('unavailable'),
        code: SyncErrorCode.network,
        retryable: true,
      );
    });

    test('deadline-exceeded → timeout, retryable', () {
      expectMapped(
        firebase('deadline-exceeded'),
        code: SyncErrorCode.timeout,
        retryable: true,
      );
    });

    test('cancelled → cancelled, non-retryable', () {
      expectMapped(
        firebase('cancelled'),
        code: SyncErrorCode.cancelled,
        retryable: false,
      );
    });

    test('internal → serverError, retryable', () {
      expectMapped(
        firebase('internal'),
        code: SyncErrorCode.serverError,
        retryable: true,
      );
    });

    test('unknown Firebase code → unknown, non-retryable', () {
      expectMapped(
        firebase('unimplemented'),
        code: SyncErrorCode.unknown,
        retryable: false,
      );
    });
  });

  group('transport and validation errors', () {
    test('TimeoutException → timeout, retryable', () {
      expectMapped(
        TimeoutException('slow'),
        code: SyncErrorCode.timeout,
        retryable: true,
      );
    });

    test('SocketException → network, retryable', () {
      expectMapped(
        const SocketException('no route'),
        code: SyncErrorCode.network,
        retryable: true,
      );
    });

    test('FirestoreValidationException → validation, non-retryable', () {
      expectMapped(
        FirestoreValidationException('bad input'),
        code: SyncErrorCode.validation,
        retryable: false,
      );
    });

    test('unexpected errors return null (propagate)', () {
      expect(map(StateError('bug')), isNull);
    });

    test('diagnostic fields preserve cause and message', () {
      final error = FirebaseException(
        plugin: 'cloud_firestore',
        code: 'permission-denied',
        message: 'Missing or insufficient permissions.',
      );
      final failure = map(error)!;
      expect(failure.error.message, contains('permissions'));
      expect(failure.error.cause, same(error));
      expect(failure.error.stackTrace, isNotNull);
    });
  });
}
