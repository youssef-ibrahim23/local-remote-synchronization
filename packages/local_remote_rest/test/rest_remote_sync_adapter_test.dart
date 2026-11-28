import 'dart:async';

import 'package:flutter_test/flutter_test.dart';
import 'package:http/http.dart' as http;
import 'package:local_remote_rest/local_remote_rest.dart';
import 'package:local_remote_synchronization/local_remote_synchronization.dart';

import 'support/fake_http_client.dart';
import 'support/rest_test_harness.dart';

void main() {
  late RestTestHarness harness;

  setUp(() {
    harness = RestTestHarness.create();
  });

  tearDown(() {
    harness.dispose();
  });

  group('configuration', () {
    test('rejects an empty entity mapping', () {
      expect(
        () => RestRemoteSyncAdapter(
          baseUrl: 'https://api.example.com',
          entityMapping: const {},
        ),
        throwsArgumentError,
      );
    });

    test('rejects a blank entityType key', () {
      expect(
        () => RestRemoteSyncAdapter(
          baseUrl: 'https://api.example.com',
          entityMapping: {'  ': RestEntityConfig(collectionPath: 'notes')},
        ),
        throwsArgumentError,
      );
    });

    test('rejects a non-HTTP baseUrl', () {
      expect(
        () => RestRemoteSyncAdapter(
          baseUrl: 'ftp://api.example.com',
          entityMapping: {
            'note': RestEntityConfig(collectionPath: 'notes'),
          },
        ),
        throwsArgumentError,
      );
      expect(
        () => RestRemoteSyncAdapter(
          baseUrl: 'not a url',
          entityMapping: {
            'note': RestEntityConfig(collectionPath: 'notes'),
          },
        ),
        throwsArgumentError,
      );
    });

    test('mapping is snapshotted at construction', () async {
      final mapping = <String, RestEntityConfig>{
        'note': RestEntityConfig(collectionPath: 'notes'),
      };
      final adapter = RestRemoteSyncAdapter(
        baseUrl: 'https://api.example.com',
        entityMapping: mapping,
        httpClient: harness.http,
      );
      addTearDown(adapter.close);
      // Mutating the source map after construction must not affect the
      // adapter's validated copy.
      mapping['todo'] = RestEntityConfig(collectionPath: 'todos');
      final result = await adapter.push(
        makeOperation(entityType: 'todo', payload: {}),
      );
      expect(result, isA<SyncFailure>());
      expect((result as SyncFailure).error.code, SyncErrorCode.validation);
      expect(harness.http.requests, isEmpty);
    });

    test('unknown entityType returns non-retryable validation failure',
        () async {
      final result = await harness.adapter.push(
        makeOperation(entityType: 'unknown', payload: {'title': 'x'}),
      );
      expect(result, isA<SyncFailure>());
      final failure = result as SyncFailure;
      expect(failure.error.code, SyncErrorCode.validation);
      expect(failure.error.isRetryable, isFalse);
      expect(failure.operationId, 'op-1');
      expect(harness.http.requests, isEmpty);
    });
  });

  group('RestEntityConfig validation', () {
    test('rejects blank collectionPath', () {
      expect(
        () => RestEntityConfig(collectionPath: '  '),
        throwsArgumentError,
      );
    });

    test('rejects collectionPath with query delimiters', () {
      expect(
        () => RestEntityConfig(collectionPath: 'notes?x=1'),
        throwsArgumentError,
      );
    });

    test('rejects itemPathTemplate without {id}', () {
      expect(
        () => RestEntityConfig(
          collectionPath: 'notes',
          itemPathTemplate: 'notes/all',
        ),
        throwsArgumentError,
      );
    });

    test('rejects absolute itemPathTemplate', () {
      expect(
        () => RestEntityConfig(
          collectionPath: 'notes',
          itemPathTemplate: '/notes/{id}',
        ),
        throwsArgumentError,
      );
    });

    test('normalizes slashes and exposes the default item path', () {
      final config = RestEntityConfig(collectionPath: '/api/notes/');
      expect(config.collectionPath, 'api/notes');
      expect(config.itemPath, 'api/notes/{id}');
    });
  });

  group('create', () {
    test('POSTs JSON payload to the collection endpoint', () async {
      harness.http.enqueueJson(201, {'id': 'entity-1', 'title': 'Hello'});
      final result = await harness.adapter.push(
        makeOperation(payload: {'title': 'Hello'}),
      );

      expect(result, isA<SyncSuccess>());
      final request = harness.http.requests.single;
      expect(request.method, 'POST');
      expect(request.url.toString(), 'https://api.example.com/notes');
      expect(request.headers['content-type'], 'application/json');
      expect(request.jsonBody, {'title': 'Hello'});
    });

    test('sends the stable idempotency key and default headers', () async {
      final h = RestTestHarness.create(
        defaultHeaders: {'Authorization': 'Bearer token-123'},
      );
      addTearDown(h.dispose);
      h.http.enqueueJson(201, {'id': 'entity-1'});

      await h.adapter.push(
        makeOperation(idempotencyKey: 'stable-key-9', payload: {}),
      );

      final headers = h.http.requests.single.headers;
      expect(headers['idempotency-key'], 'stable-key-9');
      expect(headers['authorization'], 'Bearer token-123');
    });

    test('201 JSON response returns syncedData and ETag remoteVersion',
        () async {
      harness.http.enqueueJson(
        201,
        {'id': 'entity-1', 'title': 'Hello'},
        headers: {'etag': '"v7"'},
      );
      final result = await harness.adapter.push(
        makeOperation(payload: {'title': 'Hello'}),
      );

      final success = result as SyncSuccess;
      expect(success.operationId, 'op-1');
      expect(success.syncedData, {'id': 'entity-1', 'title': 'Hello'});
      expect(success.remoteVersion, '"v7"');
      expect(success.serverTimestamp, isNull);
    });

    test('204 No Content succeeds without syncedData', () async {
      harness.http.enqueue(204);
      final result = await harness.adapter.push(
        makeOperation(payload: {'title': 'Hello'}),
      );

      expect(result, isA<SyncSuccess>());
      final success = result as SyncSuccess;
      expect(success.syncedData, isNull);
      expect(success.remoteVersion, isNull);
    });

    test('202 Accepted succeeds', () async {
      harness.http.enqueueJson(202, {'status': 'accepted'});
      final result = await harness.adapter.push(
        makeOperation(payload: {}),
      );
      expect(result, isA<SyncSuccess>());
    });

    test('empty 200 body succeeds without syncedData', () async {
      harness.http.enqueue(200);
      final result = await harness.adapter.push(
        makeOperation(payload: {}),
      );
      expect(result, isA<SyncSuccess>());
      expect((result as SyncSuccess).syncedData, isNull);
    });

    test('idField injects entityId into the body', () async {
      final h = RestTestHarness.create(
        entityMapping: {
          'note': RestEntityConfig(
            collectionPath: 'notes',
            idField: 'note_id',
          ),
        },
      );
      addTearDown(h.dispose);
      h.http.enqueueJson(201, {'note_id': 'entity-1'});

      await h.adapter.push(
        makeOperation(payload: {'title': 'x'}),
      );

      expect(h.http.requests.single.jsonBody, {
        'title': 'x',
        'note_id': 'entity-1',
      });
    });

    test('idField mismatch is rejected without a request', () async {
      final h = RestTestHarness.create(
        entityMapping: {
          'note': RestEntityConfig(
            collectionPath: 'notes',
            idField: 'note_id',
          ),
        },
      );
      addTearDown(h.dispose);

      final result = await h.adapter.push(
        makeOperation(payload: {'note_id': 'other-id'}),
      );

      expect(result, isA<SyncFailure>());
      expect((result as SyncFailure).error.code, SyncErrorCode.validation);
      expect(h.http.requests, isEmpty);
    });

    test('400 error response maps to non-retryable validation', () async {
      harness.http.enqueueJson(400, {'message': 'title is required'});
      final result = await harness.adapter.push(
        makeOperation(payload: {'bad': true}),
      );

      expect(result, isA<SyncFailure>());
      final failure = result as SyncFailure;
      expect(failure.error.code, SyncErrorCode.validation);
      expect(failure.error.isRetryable, isFalse);
      expect(failure.error.statusCode, 400);
      expect(failure.error.message, 'title is required');
      expect(failure.operationId, 'op-1');
    });
  });

  group('update', () {
    RestEntityConfig mapping() => RestEntityConfig(collectionPath: 'notes');

    test('PATCHes the item endpoint with encoded entityId', () async {
      final h = RestTestHarness.create(entityMapping: {'note': mapping()});
      addTearDown(h.dispose);
      h.http.enqueueJson(200, {'id': 'a/b', 'title': 'New'});

      final result = await h.adapter.push(
        makeOperation(
          type: SyncOperationType.update,
          entityId: 'a/b',
          payload: {'title': 'New'},
        ),
      );

      expect(result, isA<SyncSuccess>());
      final request = h.http.requests.single;
      expect(request.method, 'PATCH');
      expect(request.url.path, '/notes/a%2Fb');
      expect(request.jsonBody, {'title': 'New'});
    });

    test('PUT is used when configured', () async {
      final h = RestTestHarness.create(
        entityMapping: {
          'note': RestEntityConfig(
            collectionPath: 'notes',
            updateMethod: RestUpdateMethod.put,
          ),
        },
      );
      addTearDown(h.dispose);
      h.http.enqueueJson(200, {'id': 'entity-1'});

      await h.adapter.push(
        makeOperation(
          type: SyncOperationType.update,
          payload: {'title': 'New'},
        ),
      );

      expect(h.http.requests.single.method, 'PUT');
    });

    test('custom itemPathTemplate is honored', () async {
      final h = RestTestHarness.create(
        entityMapping: {
          'note': RestEntityConfig(
            collectionPath: 'notes',
            itemPathTemplate: 'v2/items/{id}/detail',
          ),
        },
        baseUrl: 'https://api.example.com/api/',
      );
      addTearDown(h.dispose);
      h.http.enqueueJson(200, {});

      await h.adapter.push(
        makeOperation(
          type: SyncOperationType.update,
          entityId: 'e1',
          payload: {},
        ),
      );

      expect(
        h.http.requests.single.url.toString(),
        'https://api.example.com/api/v2/items/e1/detail',
      );
    });

    test('sends If-Match from metadata expectedVersionKey', () async {
      harness.http
          .enqueueJson(200, {'id': 'entity-1'}, headers: {'etag': 'v8'});
      final result = await harness.adapter.push(
        makeOperation(
          type: SyncOperationType.update,
          payload: {'title': 'New'},
          metadata: {'baseVersion': '"v7"'},
        ),
      );

      expect(result, isA<SyncSuccess>());
      expect(
        (result as SyncSuccess).remoteVersion,
        'v8',
      );
      expect(
        harness.http.requests.single.headers['if-match'],
        '"v7"',
      );
    });

    test('custom version headers and metadata key are honored', () async {
      final h = RestTestHarness.create(
        entityMapping: {'note': mapping()},
        versionRequestHeader: 'X-If-Match',
        versionResponseHeader: 'X-Resource-Version',
        expectedVersionKey: 'etag',
      );
      addTearDown(h.dispose);
      h.http.enqueueJson(
        200,
        {'id': 'entity-1'},
        headers: {'x-resource-version': '42'},
      );

      final result = await h.adapter.push(
        makeOperation(
          type: SyncOperationType.update,
          payload: {'title': 'New'},
          metadata: {'etag': '41'},
        ),
      );

      expect(result, isA<SyncSuccess>());
      expect((result as SyncSuccess).remoteVersion, '42');
      expect(h.http.requests.single.headers['x-if-match'], '41');
      expect(h.http.requests.single.headers.containsKey('if-match'), isFalse);
    });

    test('412 returns SyncConflict with remote snapshot', () async {
      harness.http.enqueueJson(
        412,
        {'id': 'entity-1', 'title': 'Server version'},
        headers: {'etag': 'v9'},
      );
      final result = await harness.adapter.push(
        makeOperation(
          type: SyncOperationType.update,
          payload: {'title': 'Stale'},
          metadata: {'baseVersion': 'v1'},
        ),
      );

      expect(result, isA<SyncConflict>());
      final conflict = result as SyncConflict;
      expect(conflict.operationId, 'op-1');
      expect(conflict.remoteSnapshot['title'], 'Server version');
      expect(conflict.remoteVersion, 'v9');
    });

    test('412 with empty body returns SyncConflict with empty snapshot',
        () async {
      harness.http.enqueue(412);
      final result = await harness.adapter.push(
        makeOperation(
          type: SyncOperationType.update,
          payload: {'title': 'Stale'},
          metadata: {'baseVersion': 'v1'},
        ),
      );

      expect(result, isA<SyncConflict>());
      expect((result as SyncConflict).remoteSnapshot, isEmpty);
    });

    test('409 returns SyncConflict', () async {
      harness.http.enqueueJson(409, {'message': 'version conflict'});
      final result = await harness.adapter.push(
        makeOperation(
          type: SyncOperationType.update,
          payload: {'title': 'x'},
        ),
      );
      expect(result, isA<SyncConflict>());
    });

    test('404 returns notFound failure', () async {
      harness.http.enqueueJson(404, {'message': 'not found'});
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
      expect(failure.error.statusCode, 404);
    });
  });

  group('delete', () {
    test('DELETEs the item endpoint without a body', () async {
      harness.http.enqueue(204);
      final result = await harness.adapter.push(
        makeOperation(type: SyncOperationType.delete),
      );

      expect(result, isA<SyncSuccess>());
      final request = harness.http.requests.single;
      expect(request.method, 'DELETE');
      expect(request.url.toString(), 'https://api.example.com/notes/entity-1');
      expect(request.body, isEmpty);
      expect(request.headers.containsKey('content-type'), isFalse);
      expect(request.headers['idempotency-key'], 'idem-1');
    });

    test('sends conditional version header when supplied', () async {
      harness.http.enqueue(204);
      await harness.adapter.push(
        makeOperation(
          type: SyncOperationType.delete,
          metadata: {'baseVersion': 'v3'},
        ),
      );

      expect(
        harness.http.requests.single.headers['if-match'],
        'v3',
      );
    });

    test('404 on delete is an idempotent success', () async {
      harness.http.enqueueJson(404, {'message': 'gone'});
      final result = await harness.adapter.push(
        makeOperation(type: SyncOperationType.delete),
      );
      expect(result, isA<SyncSuccess>());
      expect(result.operationId, 'op-1');
    });

    test('412 on versioned delete returns SyncConflict', () async {
      harness.http.enqueue(412);
      final result = await harness.adapter.push(
        makeOperation(
          type: SyncOperationType.delete,
          metadata: {'baseVersion': 'stale'},
        ),
      );
      expect(result, isA<SyncConflict>());
    });
  });

  group('error mapping through HTTP', () {
    SyncOperation op() => makeOperation(payload: {});

    test('401 maps to non-retryable unauthorized', () async {
      harness.http.enqueueJson(401, {'message': 'bad token'});
      final failure = (await harness.adapter.push(op())) as SyncFailure;
      expect(failure.error.code, SyncErrorCode.unauthorized);
      expect(failure.error.isRetryable, isFalse);
      expect(failure.error.statusCode, 401);
      expect(failure.error.message, 'bad token');
    });

    test('403 maps to non-retryable forbidden', () async {
      harness.http.enqueueJson(403, {'error': 'denied'});
      final failure = (await harness.adapter.push(op())) as SyncFailure;
      expect(failure.error.code, SyncErrorCode.forbidden);
      expect(failure.error.isRetryable, isFalse);
      expect(failure.error.message, 'denied');
    });

    test('408 maps to retryable timeout', () async {
      harness.http.enqueue(408);
      final failure = (await harness.adapter.push(op())) as SyncFailure;
      expect(failure.error.code, SyncErrorCode.timeout);
      expect(failure.error.isRetryable, isTrue);
    });

    test('429 maps to retryable rateLimited with Retry-After', () async {
      harness.http.enqueue(
        429,
        body: 'slow down',
        headers: {'retry-after': '7'},
      );
      final failure = (await harness.adapter.push(op())) as SyncFailure;
      expect(failure.error.code, SyncErrorCode.rateLimited);
      expect(failure.error.isRetryable, isTrue);
      expect(failure.error.retryAfter, const Duration(seconds: 7));
      expect(failure.error.message, contains('slow down'));
    });

    test('500 maps to retryable serverError', () async {
      harness.http.enqueueJson(500, {'message': 'boom'});
      final failure = (await harness.adapter.push(op())) as SyncFailure;
      expect(failure.error.code, SyncErrorCode.serverError);
      expect(failure.error.isRetryable, isTrue);
    });

    test('503 maps to retryable serverError', () async {
      harness.http.enqueue(503);
      final failure = (await harness.adapter.push(op())) as SyncFailure;
      expect(failure.error.code, SyncErrorCode.serverError);
      expect(failure.error.isRetryable, isTrue);
    });

    test('unknown status maps to non-retryable unknown', () async {
      harness.http.enqueue(418, body: "I'm a teapot");
      final failure = (await harness.adapter.push(op())) as SyncFailure;
      expect(failure.error.code, SyncErrorCode.unknown);
      expect(failure.error.isRetryable, isFalse);
      expect(failure.error.statusCode, 418);
    });

    test('non-JSON error body still produces a safe message', () async {
      harness.http.enqueue(502, body: '<html>Bad Gateway</html>');
      final failure = (await harness.adapter.push(op())) as SyncFailure;
      expect(failure.error.code, SyncErrorCode.serverError);
      expect(failure.error.message, contains('Bad Gateway'));
    });

    test('ClientException maps to retryable network', () async {
      harness.http.enqueueError(
        http.ClientException('connection refused'),
      );
      final failure = (await harness.adapter.push(op())) as SyncFailure;
      expect(failure.error.code, SyncErrorCode.network);
      expect(failure.error.isRetryable, isTrue);
      expect(failure.operationId, 'op-1');
    });

    test('TimeoutException maps to retryable timeout', () async {
      harness.http.enqueueError(
        TimeoutException('timed out'),
      );
      final failure = (await harness.adapter.push(op())) as SyncFailure;
      expect(failure.error.code, SyncErrorCode.timeout);
      expect(failure.error.isRetryable, isTrue);
    });

    test('malformed JSON success body maps to serialization', () async {
      harness.http.enqueue(200, body: 'not json{');
      final failure = (await harness.adapter.push(op())) as SyncFailure;
      expect(failure.error.code, SyncErrorCode.serialization);
      expect(failure.error.isRetryable, isFalse);
    });

    test('JSON array success body maps to serialization', () async {
      harness.http.enqueue(200, body: '[1,2,3]');
      final failure = (await harness.adapter.push(op())) as SyncFailure;
      expect(failure.error.code, SyncErrorCode.serialization);
      expect(failure.error.isRetryable, isFalse);
    });

    test('request timeout maps to retryable timeout', () async {
      final h = RestTestHarness.create(
        requestTimeout: const Duration(milliseconds: 50),
      );
      addTearDown(h.dispose);
      h.http.pending = Completer<void>().future;

      final failure = (await h.adapter.push(
        makeOperation(payload: {}),
      )) as SyncFailure;
      expect(failure.error.code, SyncErrorCode.timeout);
      expect(failure.error.isRetryable, isTrue);
    });

    test('every failure preserves the operation id', () async {
      for (final status in [400, 401, 403, 404, 408, 429, 500, 418]) {
        harness.http.enqueue(status);
        final result = await harness.adapter.push(
          makeOperation(payload: {}),
        );
        expect(result.operationId, 'op-1', reason: 'status $status');
      }
    });

    test('no raw exception escapes push', () async {
      harness.http.enqueueError(StateError('boom'));
      await expectLater(
        harness.adapter.push(makeOperation(payload: {})),
        throwsA(isA<StateError>()),
      );
    });
  });

  group('HTTP client lifecycle', () {
    test('injected client is not closed by the adapter', () async {
      final fake = FakeHttpClient();
      RestRemoteSyncAdapter(
        baseUrl: 'https://api.example.com',
        entityMapping: {
          'note': RestEntityConfig(collectionPath: 'notes'),
        },
        httpClient: fake,
      ).close();

      expect(fake.isClosed, isFalse);
    });

    test('adapter-owned client is closed by close()', () async {
      final adapter = RestRemoteSyncAdapter(
        baseUrl: 'https://api.example.com',
        entityMapping: {
          'note': RestEntityConfig(collectionPath: 'notes'),
        },
      );
      expect(adapter.close, returnsNormally);
      expect(adapter.close, returnsNormally);
    });
  });

  group('contract', () {
    test('idempotency key is sent verbatim on every operation type', () async {
      for (final type in SyncOperationType.values) {
        harness.http.enqueue(200, body: '{}');
        await harness.adapter.push(
          makeOperation(
            type: type,
            payload: type == SyncOperationType.delete ? null : {},
            idempotencyKey: 'key-${type.name}',
          ),
        );
        expect(
          harness.http.requests.last.headers['idempotency-key'],
          'key-${type.name}',
        );
      }
    });
  });
}
