import 'dart:async';

import 'package:flutter_test/flutter_test.dart';
import 'package:http/http.dart' as http;
import 'package:local_remote_supabase/local_remote_supabase.dart';
import 'package:local_remote_synchronization/local_remote_synchronization.dart';

import 'support/supabase_test_harness.dart';

void main() {
  late SupabaseTestHarness harness;

  SupabaseTableConfig versionedConfig() => SupabaseTableConfig(
        tableName: 'notes',
        versionColumn: 'version',
      );

  Map<String, dynamic> noteRow({
    String id = 'entity-1',
    // Deliberately different from makeOperation's default idempotencyKey
    // ('idem-1') so updates are not short-circuited as already-applied.
    String idem = 'stored-idem',
    int version = 3,
    String title = 'Hello',
  }) =>
      {
        'id': id,
        'title': title,
        '_sync_operation_id': idem,
        'version': version,
      };

  setUp(() {
    harness = SupabaseTestHarness.create();
  });

  tearDown(() {
    harness.dispose();
  });

  group('configuration', () {
    test('accepts a valid mapping and pushes to the mapped table', () async {
      harness.http.enqueueJson(201, [noteRow()]);
      final result = await harness.adapter.push(
        makeOperation(payload: {'title': 'Hello'}),
      );
      expect(result, isA<SyncSuccess>());
      expect(harness.http.requests.single.method, 'POST');
      expect(harness.http.requests.single.url.path, '/rest/v1/notes');
    });

    test('supports multiple entity types mapped to different tables', () async {
      final h = SupabaseTestHarness.create(
        tableMapping: {
          'note': SupabaseTableConfig(tableName: 'notes'),
          'todo': SupabaseTableConfig(
            tableName: 'todos',
            primaryKeyColumn: 'todo_id',
            idempotencyColumn: 'sync_key',
          ),
        },
      );
      addTearDown(h.dispose);
      h.http.enqueueJson(201, [
        {'todo_id': 't-1', 'sync_key': 'idem-1'},
      ]);
      final result = await h.adapter.push(
        makeOperation(
          entityType: 'todo',
          entityId: 't-1',
          payload: {'title': 'x'},
        ),
      );
      expect(result, isA<SyncSuccess>());
      expect(h.http.requests.single.url.path, '/rest/v1/todos');
      final body = h.http.requests.single.jsonBody!;
      expect(body['todo_id'], 't-1');
      expect(body['sync_key'], 'idem-1');
      expect(body.containsKey('id'), isFalse);
    });

    test('unknown entityType returns non-retryable validation failure',
        () async {
      final result = await harness.adapter.push(
        makeOperation(entityType: 'unknown', payload: {}),
      );
      expect(result, isA<SyncFailure>());
      final failure = result as SyncFailure;
      expect(failure.error.code, SyncErrorCode.validation);
      expect(failure.error.isRetryable, isFalse);
      expect(failure.operationId, 'op-1');
      expect(harness.http.requests, isEmpty);
    });

    test('blank entityType mapping key throws ArgumentError', () {
      expect(
        () => SupabaseRemoteSyncAdapter(
          client: harness.client,
          tableMapping: {'  ': SupabaseTableConfig(tableName: 't')},
        ),
        throwsArgumentError,
      );
    });

    test('empty mapping throws ArgumentError', () {
      expect(
        () => SupabaseRemoteSyncAdapter(
          client: harness.client,
          tableMapping: const {},
        ),
        throwsArgumentError,
      );
    });
  });

  group('SupabaseTableConfig validation', () {
    test('rejects blank names', () {
      expect(() => SupabaseTableConfig(tableName: ' '), throwsArgumentError);
      expect(
        () => SupabaseTableConfig(
          tableName: 't',
          primaryKeyColumn: '',
        ),
        throwsArgumentError,
      );
      expect(
        () => SupabaseTableConfig(
          tableName: 't',
          idempotencyColumn: ' ',
        ),
        throwsArgumentError,
      );
      expect(
        () => SupabaseTableConfig(
          tableName: 't',
          versionColumn: '',
        ),
        throwsArgumentError,
      );
      expect(
        () => SupabaseTableConfig(
          tableName: 't',
          expectedVersionKey: '',
        ),
        throwsArgumentError,
      );
    });

    test('rejects colliding managed columns', () {
      expect(
        () => SupabaseTableConfig(
          tableName: 't',
          idempotencyColumn: 'id',
        ),
        throwsArgumentError,
      );
      expect(
        () => SupabaseTableConfig(
          tableName: 't',
          versionColumn: 'id',
        ),
        throwsArgumentError,
      );
      expect(
        () => SupabaseTableConfig(
          tableName: 't',
          versionColumn: '_sync_operation_id',
        ),
        throwsArgumentError,
      );
    });

    test('value equality', () {
      expect(
        SupabaseTableConfig(tableName: 'a'),
        SupabaseTableConfig(tableName: 'a'),
      );
      expect(
        SupabaseTableConfig(tableName: 'a'),
        isNot(SupabaseTableConfig(tableName: 'b')),
      );
    });
  });

  group('create', () {
    test('inserts payload with injected PK and idempotency key', () async {
      harness.http.enqueueJson(201, [
        noteRow(),
      ]);
      final result = await harness.adapter.push(
        makeOperation(
          payload: {'title': 'Hello', 'body': 'World'},
        ),
      );

      expect(result, isA<SyncSuccess>());
      final success = result as SyncSuccess;
      expect(success.operationId, 'op-1');
      expect(success.syncedData, containsPair('id', 'entity-1'));

      final request = harness.http.requests.single;
      expect(request.method, 'POST');
      expect(request.url.path, '/rest/v1/notes');
      final body = request.jsonBody!;
      expect(body['id'], 'entity-1');
      expect(body['_sync_operation_id'], 'idem-1');
      expect(body['title'], 'Hello');
      expect(body['body'], 'World');
      expect(request.headers['apikey'], 'anon-key');
      final authHeader = request.headers.entries
          .firstWhere((e) => e.key.toLowerCase() == 'authorization')
          .value;
      expect(authHeader, 'Bearer test-access-token');
    });

    test('payload PK matching entityId is accepted', () async {
      harness.http.enqueueJson(201, [noteRow()]);
      final result = await harness.adapter.push(
        makeOperation(payload: {'id': 'entity-1', 'title': 'x'}),
      );
      expect(result, isA<SyncSuccess>());
      expect(harness.http.requests.single.jsonBody!['id'], 'entity-1');
    });

    test('payload PK mismatch is rejected without a request', () async {
      final result = await harness.adapter.push(
        makeOperation(payload: {'id': 'other-id', 'title': 'x'}),
      );
      expect(result, isA<SyncFailure>());
      final failure = result as SyncFailure;
      expect(failure.error.code, SyncErrorCode.validation);
      expect(failure.error.isRetryable, isFalse);
      expect(harness.http.requests, isEmpty);
    });

    test('payload idempotency mismatch is rejected', () async {
      final result = await harness.adapter.push(
        makeOperation(payload: {'_sync_operation_id': 'forged'}),
      );
      expect(result, isA<SyncFailure>());
      expect((result as SyncFailure).error.code, SyncErrorCode.validation);
      expect(harness.http.requests, isEmpty);
    });

    test('null payload cannot reach the adapter (Core invariant)', () {
      // SyncOperation's constructor requires a payload for create and
      // update operations, so _writePayload's null guard is
      // unreachable defense-in-depth: it throws SyncValidationException,
      // which SupabaseErrorMapper maps to a non-retryable validation
      // SyncFailure (covered by the mapper tests).
      expect(makeOperation, throwsArgumentError);
      expect(harness.http.requests, isEmpty);
    });

    test('duplicate PK with matching idempotency key returns success',
        () async {
      harness.http
        ..enqueuePostgrestError(
          statusCode: 409,
          code: '23505',
          message: 'duplicate key value violates unique constraint',
          details: 'Key (id)=(entity-1) already exists.',
        )
        ..enqueueJson(200, [noteRow(idem: 'idem-1')]);

      final result = await harness.adapter.push(
        makeOperation(payload: {'title': 'x'}),
      );

      expect(result, isA<SyncSuccess>());
      expect(harness.http.requests, hasLength(2));
      expect(harness.http.requests[1].method, 'GET');
      expect(
        harness.http.requests[1].url.queryParameters['id'],
        'eq.entity-1',
      );
    });

    test('duplicate PK with different idempotency key returns conflict',
        () async {
      harness.http
        ..enqueuePostgrestError(
          statusCode: 409,
          code: '23505',
          message: 'duplicate key value',
        )
        ..enqueueJson(200, [noteRow(idem: 'other-op-key', version: 7)]);

      final adapter = SupabaseRemoteSyncAdapter(
        client: harness.client,
        tableMapping: {'note': versionedConfig()},
      );
      final result = await adapter.push(
        makeOperation(payload: {'title': 'x'}),
      );

      expect(result, isA<SyncConflict>());
      final conflict = result as SyncConflict;
      expect(conflict.operationId, 'op-1');
      expect(conflict.remoteSnapshot['id'], 'entity-1');
      expect(conflict.remoteVersion, '7');
    });

    test('unique violation without a fetchable row is a conflict failure',
        () async {
      harness.http
        ..enqueuePostgrestError(
          statusCode: 409,
          code: '23505',
          message: 'duplicate key value',
        )
        // Row hidden by RLS or violation on another unique constraint.
        ..enqueueJson(200, []);

      final result = await harness.adapter.push(
        makeOperation(payload: {'title': 'x'}),
      );

      expect(result, isA<SyncFailure>());
      final failure = result as SyncFailure;
      expect(failure.error.code, SyncErrorCode.conflict);
      expect(failure.error.isRetryable, isFalse);
    });

    test('non-unique PostgREST error maps to structured failure', () async {
      harness.http.enqueuePostgrestError(
        statusCode: 403,
        code: '42501',
        message: 'new row violates row-level security policy',
      );
      final result = await harness.adapter.push(
        makeOperation(payload: {'title': 'x'}),
      );
      expect(result, isA<SyncFailure>());
      final failure = result as SyncFailure;
      expect(failure.error.code, SyncErrorCode.forbidden);
      expect(failure.error.isRetryable, isFalse);
      expect(failure.error.message, contains('row-level security'));
    });
  });

  group('update', () {
    test('fetches row then applies conditional update', () async {
      final versioned = SupabaseTestHarness.create(
        tableMapping: {'note': versionedConfig()},
      );
      addTearDown(versioned.dispose);
      versioned.http
        ..enqueueJson(200, [noteRow()])
        ..enqueueJson(200, [
          noteRow(version: 4, title: 'Updated'),
        ]);

      final result = await versioned.adapter.push(
        makeOperation(
          type: SyncOperationType.update,
          payload: {'title': 'Updated'},
          metadata: {'baseVersion': 3},
        ),
      );

      expect(result, isA<SyncSuccess>());
      // remoteVersion is whatever the server returned — the adapter does
      // not compute or increment it. Real version progression requires a
      // database trigger (see README); these tests verify only the
      // client-side conditional request/response contract.
      expect((result as SyncSuccess).remoteVersion, '4');
      final requests = versioned.http.requests;
      expect(requests, hasLength(2));
      expect(requests[0].method, 'GET');
      expect(requests[1].method, 'PATCH');
      expect(requests[1].url.path, '/rest/v1/notes');
      expect(requests[1].url.queryParameters['id'], 'eq.entity-1');
      expect(requests[1].url.queryParameters['version'], 'eq.3');
      final body = requests[1].jsonBody!;
      expect(body['title'], 'Updated');
      expect(body['_sync_operation_id'], 'idem-1');
      expect(body.containsKey('id'), isFalse);
      expect(body.containsKey('version'), isFalse);
    });

    test('existing idempotency key returns success without reapplying',
        () async {
      harness.http.enqueueJson(200, [noteRow(idem: 'idem-1')]);
      final result = await harness.adapter.push(
        makeOperation(
          type: SyncOperationType.update,
          payload: {'title': 'x'},
        ),
      );
      expect(result, isA<SyncSuccess>());
      expect(harness.http.requests, hasLength(1));
      expect(harness.http.requests.single.method, 'GET');
    });

    test('version mismatch returns conflict with remote snapshot', () async {
      final versioned = SupabaseTestHarness.create(
        tableMapping: {'note': versionedConfig()},
      );
      addTearDown(versioned.dispose);
      versioned.http.enqueueJson(200, [noteRow(version: 9)]);

      final result = await versioned.adapter.push(
        makeOperation(
          type: SyncOperationType.update,
          payload: {'title': 'x'},
          metadata: {'baseVersion': 3},
        ),
      );

      expect(result, isA<SyncConflict>());
      final conflict = result as SyncConflict;
      expect(conflict.remoteSnapshot['version'], 9);
      expect(conflict.remoteVersion, '9');
      // Only the fetch happened — no PATCH was issued.
      expect(versioned.http.requests, hasLength(1));
    });

    test('conditional update race returns conflict', () async {
      final versioned = SupabaseTestHarness.create(
        tableMapping: {'note': versionedConfig()},
      );
      addTearDown(versioned.dispose);
      versioned.http
        ..enqueueJson(200, [noteRow()])
        // Conditional update matched no row.
        ..enqueueJson(200, <Map<String, dynamic>>[])
        // Re-fetch returns the newer remote state.
        ..enqueueJson(200, [noteRow(version: 4, idem: 'other-key')]);

      final result = await versioned.adapter.push(
        makeOperation(
          type: SyncOperationType.update,
          payload: {'title': 'x'},
          metadata: {'baseVersion': 3},
        ),
      );

      expect(result, isA<SyncConflict>());
      expect((result as SyncConflict).remoteVersion, '4');
      expect(versioned.http.requests, hasLength(3));
    });

    // NOTE: an empty SELECT response is indistinguishable from an
    // RLS-hidden row — PostgREST returns [] for both. This test verifies
    // the documented behavior: update reports notFound for an empty
    // response and issues no PATCH. It does NOT prove the row is absent;
    // the session may simply lack SELECT visibility (see README).
    test('update on missing row returns notFound', () async {
      harness.http.enqueueJson(200, <Map<String, dynamic>>[]);
      final result = await harness.adapter.push(
        makeOperation(
          type: SyncOperationType.update,
          payload: {'title': 'x'},
        ),
      );
      expect(result, isA<SyncFailure>());
      final failure = result as SyncFailure;
      expect(failure.error.code, SyncErrorCode.notFound);
      expect(failure.error.isRetryable, isFalse);
      expect(harness.http.requests, hasLength(1));
    });

    test('no expected version means no optimistic concurrency', () async {
      final versioned = SupabaseTestHarness.create(
        tableMapping: {'note': versionedConfig()},
      );
      addTearDown(versioned.dispose);
      versioned.http
        ..enqueueJson(200, [noteRow()])
        ..enqueueJson(200, [noteRow(version: 4)]);

      final result = await versioned.adapter.push(
        makeOperation(
          type: SyncOperationType.update,
          payload: {'title': 'x'},
          // No baseVersion metadata.
        ),
      );

      expect(result, isA<SyncSuccess>());
      final patch = versioned.http.requests[1];
      expect(patch.url.queryParameters.containsKey('version'), isFalse);
    });

    test('payload cannot overwrite PK or idempotency column', () async {
      final result = await harness.adapter.push(
        makeOperation(
          type: SyncOperationType.update,
          payload: {'id': 'different'},
        ),
      );
      expect(result, isA<SyncFailure>());
      expect((result as SyncFailure).error.code, SyncErrorCode.validation);
      expect(harness.http.requests, isEmpty);
    });

    test('payload idempotency override is rejected', () async {
      final result = await harness.adapter.push(
        makeOperation(
          type: SyncOperationType.update,
          payload: {'_sync_operation_id': 'forged'},
        ),
      );
      expect(result, isA<SyncFailure>());
      expect(harness.http.requests, isEmpty);
    });

    test('null payload cannot reach the adapter (Core invariant)', () {
      // Same Core-level invariant as create: a null-payload update
      // operation cannot be constructed, so the adapter's
      // SyncValidationException guard is defense-in-depth only.
      expect(
        () => makeOperation(type: SyncOperationType.update),
        throwsArgumentError,
      );
      expect(harness.http.requests, isEmpty);
    });
  });

  group('delete', () {
    test('deletes by primary key only', () async {
      harness.http.enqueueJson(200, [
        noteRow(),
      ]);
      final result = await harness.adapter.push(
        makeOperation(
          type: SyncOperationType.delete,
        ),
      );

      expect(result, isA<SyncSuccess>());
      final request = harness.http.requests.single;
      expect(request.method, 'DELETE');
      expect(request.url.path, '/rest/v1/notes');
      expect(request.url.queryParameters['id'], 'eq.entity-1');
      // Payload is never used as a delete filter.
      expect(request.url.queryParameters.keys, isNot(contains('title')));
    });

    test('already-absent row is an idempotent success', () async {
      harness.http.enqueueJson(200, <Map<String, dynamic>>[]);
      final result = await harness.adapter.push(
        makeOperation(type: SyncOperationType.delete),
      );
      expect(result, isA<SyncSuccess>());
    });

    // Delete idempotency semantics (Option A — documented, not ledgered):
    // the row is gone after a successful delete, so its idempotency key
    // cannot be retained. A retried delete of the same entityId is a
    // no-op success because the row is absent — this is NOT proof that a
    // delete idempotency key was persisted. Entity IDs must never be
    // reused while an old delete could still be retried; see README.
    test('retried delete after success is an idempotent no-op', () async {
      final operation = makeOperation(type: SyncOperationType.delete);
      harness.http
        // First attempt deletes the row.
        ..enqueueJson(200, [noteRow()])
        // Retry finds nothing left to delete.
        ..enqueueJson(200, <Map<String, dynamic>>[]);

      final first = await harness.adapter.push(operation);
      final second = await harness.adapter.push(operation);

      expect(first, isA<SyncSuccess>());
      expect(second, isA<SyncSuccess>());
      expect(second.operationId, operation.operationId);
      expect(harness.http.requests, hasLength(2));
      expect(harness.http.requests[1].method, 'DELETE');
    });

    test('versioned delete enforces expected version', () async {
      final versioned = SupabaseTestHarness.create(
        tableMapping: {'note': versionedConfig()},
      );
      addTearDown(versioned.dispose);
      versioned.http
        ..enqueueJson(200, [noteRow(version: 5)])
        ..enqueueJson(200, <Map<String, dynamic>>[])
        ..enqueueJson(200, [noteRow(version: 6, idem: 'other')]);

      final result = await versioned.adapter.push(
        makeOperation(
          type: SyncOperationType.delete,
          metadata: {'baseVersion': 4},
        ),
      );

      // Pre-fetch shows version 5 != expected 4 → conflict, no DELETE.
      expect(result, isA<SyncConflict>());
      expect(versioned.http.requests, hasLength(1));
    });

    test('versioned delete of absent row succeeds idempotently', () async {
      final versioned = SupabaseTestHarness.create(
        tableMapping: {'note': versionedConfig()},
      );
      addTearDown(versioned.dispose);
      versioned.http.enqueueJson(200, <Map<String, dynamic>>[]);

      final result = await versioned.adapter.push(
        makeOperation(
          type: SyncOperationType.delete,
          metadata: {'baseVersion': 4},
        ),
      );
      expect(result, isA<SyncSuccess>());
      expect(versioned.http.requests, hasLength(1));
    });
  });

  group('error mapping through HTTP', () {
    // Transport-level failures are exercised through create (POST)
    // operations: the Supabase SDK retries failed GET/HEAD requests with
    // real backoff delays, which would make tests slow and
    // timing-dependent. POST/PATCH/DELETE are never SDK-retried.
    SyncOperation failingOp() => makeOperation(payload: {'title': 'x'});

    test('transport failure maps to retryable network error', () async {
      harness.http.enqueueThrow(
        http.ClientException(
            'Connection refused', Uri.parse('https://test.supabase.co')),
      );
      final result = await harness.adapter.push(failingOp());
      expect(result, isA<SyncFailure>());
      final failure = result as SyncFailure;
      expect(failure.error.code, SyncErrorCode.network);
      expect(failure.error.isRetryable, isTrue);
    });

    test('timeout maps to retryable timeout error', () async {
      harness.http.enqueueThrow(
        TimeoutException('request timed out'),
      );
      final result = await harness.adapter.push(failingOp());
      final failure = result as SyncFailure;
      expect(failure.error.code, SyncErrorCode.timeout);
      expect(failure.error.isRetryable, isTrue);
    });

    test('HTTP 401 maps to unauthorized', () async {
      harness.http.enqueueRawError(401, 'Unauthorized');
      final result = await harness.adapter.push(failingOp());
      final failure = result as SyncFailure;
      expect(failure.error.code, SyncErrorCode.unauthorized);
      expect(failure.error.isRetryable, isFalse);
      expect(failure.error.statusCode, 401);
    });

    test('PGRST301 JWT error maps to unauthorized', () async {
      harness.http.enqueuePostgrestError(
        statusCode: 401,
        code: 'PGRST301',
        message: 'Expected 3 parts in JWT; got 1',
      );
      final result = await harness.adapter.push(failingOp());
      final failure = result as SyncFailure;
      expect(failure.error.code, SyncErrorCode.unauthorized);
      expect(failure.error.isRetryable, isFalse);
    });

    test('HTTP 429 maps to retryable rateLimited', () async {
      harness.http.enqueueRawError(429, 'Too Many Requests');
      final result = await harness.adapter.push(failingOp());
      final failure = result as SyncFailure;
      expect(failure.error.code, SyncErrorCode.rateLimited);
      expect(failure.error.isRetryable, isTrue);
      expect(failure.error.retryAfter, isNull);
    });

    test('HTTP 500 maps to retryable serverError', () async {
      harness.http.enqueueRawError(500, 'Internal Server Error');
      final result = await harness.adapter.push(failingOp());
      final failure = result as SyncFailure;
      expect(failure.error.code, SyncErrorCode.serverError);
      expect(failure.error.isRetryable, isTrue);
      expect(failure.error.statusCode, 500);
    });

    test('non-JSON 2xx body maps to serialization failure', () async {
      harness.http.enqueue(
        http.Response('<html>proxy error</html>', 200),
      );
      final result = await harness.adapter.push(failingOp());
      final failure = result as SyncFailure;
      expect(failure.error.code, SyncErrorCode.serialization);
      expect(failure.error.isRetryable, isFalse);
    });

    test('PostgREST validation error maps to non-retryable validation',
        () async {
      harness.http.enqueuePostgrestError(
        statusCode: 400,
        code: '23502',
        message: 'null value in column "title" violates not-null constraint',
      );
      final result = await harness.adapter.push(failingOp());
      final failure = result as SyncFailure;
      expect(failure.error.code, SyncErrorCode.validation);
      expect(failure.error.isRetryable, isFalse);
    });

    test('every failure preserves the operation id', () async {
      harness.http.enqueueRawError(500, 'boom');
      final result = await harness.adapter.push(
        makeOperation(
          operationId: 'op-xyz',
          type: SyncOperationType.update,
          payload: {'title': 'x'},
        ),
      );
      expect(result.operationId, 'op-xyz');
    });
  });

  group('contract', () {
    test('idempotency key is sent verbatim on every operation type', () async {
      // create
      harness.http.enqueueJson(201, [noteRow()]);
      await harness.adapter.push(
        makeOperation(idempotencyKey: 'stable-key', payload: {'a': 1}),
      );
      expect(
        harness.http.requests.single.jsonBody!['_sync_operation_id'],
        'stable-key',
      );

      // update (fetch + patch)
      harness.http
        ..enqueueJson(200, [noteRow(idem: 'other')])
        ..enqueueJson(200, [noteRow()]);
      await harness.adapter.push(
        makeOperation(
          type: SyncOperationType.update,
          idempotencyKey: 'stable-key',
          payload: {'a': 2},
        ),
      );
      expect(
        harness.http.requests.last.jsonBody!['_sync_operation_id'],
        'stable-key',
      );
    });

    test('no raw backend exception escapes push', () async {
      harness.http.enqueuePostgrestError(
        statusCode: 400,
        code: '22P02',
        message: 'invalid input syntax',
      );
      final result = await harness.adapter.push(
        makeOperation(payload: {'title': 'x'}),
      );
      expect(result, isA<SyncFailure>());
    });
  });
}
