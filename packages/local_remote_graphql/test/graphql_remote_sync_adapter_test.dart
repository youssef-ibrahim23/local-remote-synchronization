import 'dart:async';
import 'dart:io';

import 'package:gql/ast.dart';
import 'package:gql/language.dart';
import 'package:graphql/client.dart';
import 'package:http/http.dart' as http;
import 'package:local_remote_graphql/local_remote_graphql.dart';
import 'package:local_remote_synchronization/local_remote_synchronization.dart';
import 'package:test/test.dart';

import 'support/fake_graphql_link.dart';

void main() {
  late GraphQLTestHarness harness;

  setUp(() {
    harness = GraphQLTestHarness.create();
  });

  group('configuration', () {
    test('rejects an empty entity mapping', () {
      expect(
        () => GraphQLRemoteSyncAdapter(
          client: harness.client,
          entityMapping: const {},
        ),
        throwsArgumentError,
      );
    });

    test('rejects a blank entity type key', () {
      expect(
        () => GraphQLRemoteSyncAdapter(
          client: harness.client,
          entityMapping: {
            ' ': GraphQLEntityConfig(
              create: GraphQLOperationConfig(
                document: _doc('mutation { a }'),
                variables: (_) => const {},
              ),
            ),
          },
        ),
        throwsArgumentError,
      );
    });

    test('unmapped entityType returns non-retryable validation failure',
        () async {
      final result = await harness.adapter.push(
        makeOperation(entityType: 'unknown', payload: {}),
      );
      expect(result, isA<SyncFailure>());
      final failure = result as SyncFailure;
      expect(failure.error.code, SyncErrorCode.validation);
      expect(failure.error.isRetryable, isFalse);
      expect(failure.operationId, 'op-1');
      expect(harness.link.requests, isEmpty);
    });

    test('entity config without the requested operation fails validation',
        () async {
      // 'todo' only configures create.
      final result = await harness.adapter.push(
        makeOperation(
          entityType: 'todo',
          type: SyncOperationType.update,
          payload: {},
        ),
      );
      expect(result, isA<SyncFailure>());
      expect((result as SyncFailure).error.code, SyncErrorCode.validation);
      expect(harness.link.requests, isEmpty);
    });

    test('mapping is snapshotted at construction', () async {
      final mapping = <String, GraphQLEntityConfig>{
        'note': GraphQLEntityConfig(
          create: GraphQLOperationConfig(
            document: _doc('mutation { a }'),
            variables: (_) => const {},
          ),
        ),
      };
      final adapter = GraphQLRemoteSyncAdapter(
        client: harness.client,
        entityMapping: mapping,
      );
      mapping['todo'] = GraphQLEntityConfig(
        create: GraphQLOperationConfig(
          document: _doc('mutation { b }'),
          variables: (_) => const {},
        ),
      );
      final result = await adapter.push(
        makeOperation(entityType: 'todo', payload: {}),
      );
      expect(result, isA<SyncFailure>());
      expect((result as SyncFailure).error.code, SyncErrorCode.validation);
    });
  });

  group('CRUD', () {
    test('create executes the configured mutation with built variables',
        () async {
      harness.link.enqueueData({
        'createNote': {'id': 'entity-1', 'version': 1},
      });

      final result = await harness.adapter.push(
        makeOperation(payload: {'title': 'Hello'}),
      );

      expect(result, isA<SyncSuccess>());
      expect(result.operationId, 'op-1');

      expect(harness.link.requests, hasLength(1));
      final request = harness.link.requests.single;
      expect(request.variables['input'], {
        'id': 'entity-1',
        'idempotencyKey': 'idem-1',
        'data': {'title': 'Hello'},
      });
    });

    test('update passes entityId, payload patch, and idempotency key',
        () async {
      harness.link.enqueueData({
        'updateNote': {'id': 'entity-1', 'version': 4},
      });

      final result = await harness.adapter.push(
        makeOperation(
          type: SyncOperationType.update,
          payload: {'title': 'New'},
          idempotencyKey: 'update-key',
        ),
      );

      expect(result, isA<SyncSuccess>());
      final variables = harness.link.requests.single.variables;
      expect(variables['id'], 'entity-1');
      expect(variables['patch'], {'title': 'New'});
      expect(variables['idempotencyKey'], 'update-key');
    });

    test('delete supports a null payload and forwards the key', () async {
      harness.link.enqueueData({
        'deleteNote': true,
      });

      final result = await harness.adapter.push(
        makeOperation(
          type: SyncOperationType.delete,
          idempotencyKey: 'delete-key',
        ),
      );

      expect(result, isA<SyncSuccess>());
      final variables = harness.link.requests.single.variables;
      expect(variables['id'], 'entity-1');
      expect(variables['key'], 'delete-key');
    });

    test('variables builder receives the original operation', () async {
      SyncOperation? seen;
      final h = GraphQLTestHarness.create(
        entityMapping: {
          'note': GraphQLEntityConfig(
            create: GraphQLOperationConfig(
              document: _doc('mutation { c }'),
              variables: (op) {
                seen = op;
                return {'k': op.idempotencyKey};
              },
            ),
          ),
        },
      );
      h.link.enqueueData({'c': true});
      final op = makeOperation(
        operationId: 'op-42',
        idempotencyKey: 'key-42',
        payload: {'a': 1},
      );
      await h.adapter.push(op);
      expect(identical(seen, op), isTrue);
      expect(h.link.requests.single.variables['k'], 'key-42');
    });

    test('variables builder failure returns validation failure', () async {
      final h = GraphQLTestHarness.create(
        entityMapping: {
          'note': GraphQLEntityConfig(
            create: GraphQLOperationConfig(
              document: _doc('mutation { c }'),
              variables: (_) => throw StateError('bad vars'),
            ),
          ),
        },
      );
      final result = await h.adapter.push(
        makeOperation(payload: {}),
      );
      expect(result, isA<SyncFailure>());
      final failure = result as SyncFailure;
      expect(failure.error.code, SyncErrorCode.validation);
      expect(failure.error.isRetryable, isFalse);
      expect(failure.error.cause, isA<StateError>());
      expect(h.link.requests, isEmpty);
    });
  });

  group('idempotency', () {
    test('retries reuse the same idempotency key', () async {
      harness.link
        ..enqueueData({
          'createNote': {'id': 'entity-1'}
        })
        ..enqueueData({
          'createNote': {'id': 'entity-1'}
        });

      final op = makeOperation(
        idempotencyKey: 'stable-key',
        payload: {'title': 'x'},
      );
      await harness.adapter.push(op);
      await harness.adapter.push(op);

      expect(harness.link.requests, hasLength(2));
      for (final request in harness.link.requests) {
        final input = request.variables['input'] as Map<String, dynamic>;
        expect(input['idempotencyKey'], 'stable-key');
      }
    });

    test('adapter does not inject the key — the builder owns variables',
        () async {
      // Documents that the adapter exposes idempotencyKey to the builder
      // but cannot guarantee it appears in the outgoing variables.
      final h = GraphQLTestHarness.create(
        entityMapping: {
          'note': GraphQLEntityConfig(
            create: GraphQLOperationConfig(
              document: _doc('mutation { c }'),
              variables: (_) => const {'unrelated': true},
            ),
          ),
        },
      );
      h.link.enqueueData({'c': true});

      await h.adapter.push(
        makeOperation(idempotencyKey: 'must-not-appear', payload: {}),
      );

      final variables = h.link.requests.single.variables;
      expect(variables, {'unrelated': true});
      expect(variables.containsKey('idempotencyKey'), isFalse);
    });
  });

  group('response interpretation', () {
    test('success outcome maps remoteVersion and syncedData', () async {
      final h = GraphQLTestHarness.create(
        responseParser: (op, data) {
          final note = data['createNote'] as Map<String, dynamic>;
          return GraphQLMutationSuccess(
            remoteVersion: note['version']?.toString(),
            syncedData: note,
          );
        },
      );
      h.link.enqueueData({
        'createNote': {'id': 'entity-1', 'version': 7},
      });

      final result = await h.adapter.push(makeOperation(payload: {}));

      expect(result, isA<SyncSuccess>());
      final success = result as SyncSuccess;
      expect(success.remoteVersion, '7');
      expect(success.syncedData?['id'], 'entity-1');
    });

    test('conflict outcome maps remote snapshot and version', () async {
      final h = GraphQLTestHarness.create(
        responseParser: (op, data) {
          final payload = data['updateNote'] as Map<String, dynamic>;
          if (payload['conflict'] == true) {
            return const GraphQLMutationConflict(
              remoteSnapshot: {'id': 'entity-1', 'title': 'remote'},
              remoteVersion: '9',
            );
          }
          return const GraphQLMutationSuccess();
        },
      );
      h.link.enqueueData({
        'updateNote': {'conflict': true},
      });

      final result = await h.adapter.push(
        makeOperation(
          type: SyncOperationType.update,
          payload: {'title': 'local'},
        ),
      );

      expect(result, isA<SyncConflict>());
      final conflict = result as SyncConflict;
      expect(conflict.remoteSnapshot['title'], 'remote');
      expect(conflict.remoteVersion, '9');
    });

    test('failure outcome maps a structured SyncError', () async {
      final h = GraphQLTestHarness.create(
        responseParser: (op, data) => GraphQLMutationFailure(
          SyncError(
            code: SyncErrorCode.validation,
            message: 'backend rejected input',
            isRetryable: false,
          ),
        ),
      );
      h.link.enqueueData({'x': <String, dynamic>{}});

      final result = await h.adapter.push(makeOperation(payload: {}));
      expect(result, isA<SyncFailure>());
      final failure = result as SyncFailure;
      expect(failure.error.code, SyncErrorCode.validation);
      expect(failure.error.message, 'backend rejected input');
    });

    test('null response data without errors is a serialization failure',
        () async {
      harness.link.enqueueData(null);
      final result = await harness.adapter.push(
        makeOperation(payload: {}),
      );
      expect(result, isA<SyncFailure>());
      final failure = result as SyncFailure;
      expect(failure.error.code, SyncErrorCode.serialization);
      expect(failure.error.isRetryable, isFalse);
    });

    test('empty response data is a serialization failure', () async {
      harness.link.enqueueData(const {});
      final result = await harness.adapter.push(
        makeOperation(payload: {}),
      );
      expect(result, isA<SyncFailure>());
      expect(
        (result as SyncFailure).error.code,
        SyncErrorCode.serialization,
      );
    });

    test('response parser failure is a non-retryable serialization failure',
        () async {
      final h = GraphQLTestHarness.create(
        responseParser: (op, data) => throw const FormatException('bad shape'),
      );
      h.link.enqueueData({'anything': <String, dynamic>{}});

      final result = await h.adapter.push(makeOperation(payload: {}));
      expect(result, isA<SyncFailure>());
      final failure = result as SyncFailure;
      expect(failure.error.code, SyncErrorCode.serialization);
      expect(failure.error.isRetryable, isFalse);
      expect(failure.error.cause, isA<FormatException>());
    });

    test('default parser reports plain success for usable data', () async {
      harness.link.enqueueData({
        'createNote': {'id': 'entity-1'}
      });
      final result = await harness.adapter.push(
        makeOperation(payload: {}),
      );
      expect(result, isA<SyncSuccess>());
      expect((result as SyncSuccess).remoteVersion, isNull);
      expect(result.syncedData, isNull);
    });
  });

  group('GraphQL execution errors', () {
    Future<SyncFailure> pushWithError(GraphQLError error) async {
      harness.link.enqueueData(null, errors: [error]);
      final result = await harness.adapter.push(
        makeOperation(payload: {}),
      );
      return result as SyncFailure;
    }

    GraphQLError gqlError(String code, [String message = 'err']) =>
        GraphQLError(
          message: message,
          extensions: {'code': code},
        );

    test('UNAUTHENTICATED maps to unauthorized, non-retryable', () async {
      final failure = await pushWithError(gqlError('UNAUTHENTICATED'));
      expect(failure.error.code, SyncErrorCode.unauthorized);
      expect(failure.error.isRetryable, isFalse);
    });

    test('FORBIDDEN maps to forbidden, non-retryable', () async {
      final failure = await pushWithError(gqlError('FORBIDDEN'));
      expect(failure.error.code, SyncErrorCode.forbidden);
      expect(failure.error.isRetryable, isFalse);
    });

    test('NOT_FOUND maps to notFound, non-retryable', () async {
      final failure = await pushWithError(gqlError('NOT_FOUND'));
      expect(failure.error.code, SyncErrorCode.notFound);
      expect(failure.error.isRetryable, isFalse);
    });

    test('BAD_USER_INPUT maps to validation, non-retryable', () async {
      final failure = await pushWithError(gqlError('BAD_USER_INPUT'));
      expect(failure.error.code, SyncErrorCode.validation);
      expect(failure.error.isRetryable, isFalse);
    });

    test('RATE_LIMITED maps to rateLimited, retryable', () async {
      final failure = await pushWithError(gqlError('RATE_LIMITED'));
      expect(failure.error.code, SyncErrorCode.rateLimited);
      expect(failure.error.isRetryable, isTrue);
    });

    test('INTERNAL_SERVER_ERROR maps to serverError, retryable', () async {
      final failure = await pushWithError(
        gqlError('INTERNAL_SERVER_ERROR'),
      );
      expect(failure.error.code, SyncErrorCode.serverError);
      expect(failure.error.isRetryable, isTrue);
    });

    test('unknown GraphQL code maps to unknown, non-retryable', () async {
      final failure = await pushWithError(gqlError('SOMETHING_ELSE'));
      expect(failure.error.code, SyncErrorCode.unknown);
      expect(failure.error.isRetryable, isFalse);
    });

    test('CONFLICT without snapshot maps to conflict failure', () async {
      final failure = await pushWithError(gqlError('CONFLICT'));
      expect(failure.error.code, SyncErrorCode.conflict);
      expect(failure.error.isRetryable, isFalse);
    });

    test('VERSION_CONFLICT with remoteSnapshot maps to SyncConflict', () async {
      harness.link.enqueueData(null, errors: const [
        GraphQLError(
          message: 'version mismatch',
          extensions: {
            'code': 'VERSION_CONFLICT',
            'remoteSnapshot': {'id': 'entity-1', 'version': 3},
            'remoteVersion': '3',
          },
        ),
      ]);
      final result = await harness.adapter.push(
        makeOperation(type: SyncOperationType.update, payload: {}),
      );
      expect(result, isA<SyncConflict>());
      final conflict = result as SyncConflict;
      expect(conflict.remoteSnapshot['version'], 3);
      expect(conflict.remoteVersion, '3');
    });

    test('errors take precedence over response data', () async {
      harness.link.enqueueData(
        {
          'createNote': {'id': 'entity-1'}
        },
        errors: [gqlError('FORBIDDEN')],
      );
      final result = await harness.adapter.push(
        makeOperation(payload: {}),
      );
      expect(result, isA<SyncFailure>());
      expect(
        (result as SyncFailure).error.code,
        SyncErrorCode.forbidden,
      );
    });
  });

  group('transport errors', () {
    test('SocketException maps to retryable network failure', () async {
      harness.link.enqueueError(const SocketException('no route'));
      final result = await harness.adapter.push(
        makeOperation(payload: {}),
      );
      expect(result, isA<SyncFailure>());
      final failure = result as SyncFailure;
      expect(failure.error.code, SyncErrorCode.network);
      expect(failure.error.isRetryable, isTrue);
    });

    test('TimeoutException maps to retryable timeout failure', () async {
      harness.link.enqueueError(TimeoutException('slow'));
      final result = await harness.adapter.push(
        makeOperation(payload: {}),
      );
      expect(result, isA<SyncFailure>());
      final failure = result as SyncFailure;
      expect(failure.error.code, SyncErrorCode.timeout);
      expect(failure.error.isRetryable, isTrue);
    });

    test('ServerException with HTTP 401 maps to unauthorized', () async {
      harness.link.enqueueError(
        const ServerException(statusCode: 401),
      );
      final result = await harness.adapter.push(
        makeOperation(payload: {}),
      );
      expect(result, isA<SyncFailure>());
      final failure = result as SyncFailure;
      expect(failure.error.code, SyncErrorCode.unauthorized);
      expect(failure.error.statusCode, 401);
      expect(failure.error.isRetryable, isFalse);
    });

    test('ServerException with HTTP 429 maps to rateLimited', () async {
      harness.link.enqueueError(
        const ServerException(statusCode: 429),
      );
      final result = await harness.adapter.push(
        makeOperation(payload: {}),
      );
      expect(result, isA<SyncFailure>());
      final failure = result as SyncFailure;
      expect(failure.error.code, SyncErrorCode.rateLimited);
      expect(failure.error.isRetryable, isTrue);
    });

    test('ServerException with HTTP 503 maps to retryable serverError',
        () async {
      harness.link.enqueueError(
        const ServerException(statusCode: 503),
      );
      final result = await harness.adapter.push(
        makeOperation(payload: {}),
      );
      expect(result, isA<SyncFailure>());
      final failure = result as SyncFailure;
      expect(failure.error.code, SyncErrorCode.serverError);
      expect(failure.error.isRetryable, isTrue);
      expect(failure.error.statusCode, 503);
    });

    test('LinkException wrapping SocketException maps to network', () async {
      harness.link.enqueueError(
        ResponseFormatException(
          originalException: const SocketException('reset'),
          originalStackTrace: StackTrace.current,
        ),
      );
      final result = await harness.adapter.push(
        makeOperation(payload: {}),
      );
      expect(result, isA<SyncFailure>());
      expect(
        (result as SyncFailure).error.code,
        SyncErrorCode.network,
      );
    });

    test('http.ClientException maps to network failure', () async {
      harness.link.enqueueError(
        http.ClientException('connection closed'),
      );
      final result = await harness.adapter.push(
        makeOperation(payload: {}),
      );
      expect(result, isA<SyncFailure>());
      expect(
        (result as SyncFailure).error.code,
        SyncErrorCode.network,
      );
    });

    test('unexpected programming errors propagate', () async {
      harness.link.enqueueError(StateError('bug'));
      // The GraphQL client wraps link failures in OperationException;
      // the adapter must not disguise the programming error as a
      // routine sync failure.
      await expectLater(
        harness.adapter.push(makeOperation(payload: {})),
        throwsA(isA<OperationException>()),
      );
    });

    ServerException serverError(
      int status, {
      List<GraphQLError>? errors,
    }) =>
        ServerException(
          statusCode: status,
          parsedResponse: Response(errors: errors, response: const {}),
        );

    GraphQLError codedError(String code) => GraphQLError(
          message: 'backend error',
          extensions: {'code': code},
        );

    test('HTTP 400 without GraphQL errors maps to validation', () async {
      harness.link.enqueueError(serverError(400));
      final result = await harness.adapter.push(
        makeOperation(payload: {}),
      );
      expect(result, isA<SyncFailure>());
      final failure = result as SyncFailure;
      expect(failure.error.code, SyncErrorCode.validation);
      expect(failure.error.isRetryable, isFalse);
      expect(failure.error.statusCode, 400);
    });

    test('HTTP 400 with BAD_USER_INPUT maps to validation', () async {
      harness.link.enqueueError(
        serverError(400, errors: [codedError('BAD_USER_INPUT')]),
      );
      final result = await harness.adapter.push(
        makeOperation(payload: {}),
      );
      expect(result, isA<SyncFailure>());
      final failure = result as SyncFailure;
      expect(failure.error.code, SyncErrorCode.validation);
      expect(failure.error.isRetryable, isFalse);
    });

    test('HTTP 408 maps to retryable timeout', () async {
      harness.link.enqueueError(serverError(408));
      final result = await harness.adapter.push(
        makeOperation(payload: {}),
      );
      expect(result, isA<SyncFailure>());
      final failure = result as SyncFailure;
      expect(failure.error.code, SyncErrorCode.timeout);
      expect(failure.error.isRetryable, isTrue);
      expect(failure.error.statusCode, 408);
    });

    test('HTTP 422 maps to validation', () async {
      harness.link.enqueueError(serverError(422));
      final result = await harness.adapter.push(
        makeOperation(payload: {}),
      );
      expect(result, isA<SyncFailure>());
      final failure = result as SyncFailure;
      expect(failure.error.code, SyncErrorCode.validation);
      expect(failure.error.isRetryable, isFalse);
      expect(failure.error.statusCode, 422);
    });

    test('recognized GraphQL code beats generic HTTP status', () async {
      // HTTP 500 would map to serverError, but RATE_LIMITED is more
      // specific.
      harness.link.enqueueError(
        serverError(500, errors: [codedError('RATE_LIMITED')]),
      );
      final result = await harness.adapter.push(
        makeOperation(payload: {}),
      );
      expect(result, isA<SyncFailure>());
      final failure = result as SyncFailure;
      expect(failure.error.code, SyncErrorCode.rateLimited);
      expect(failure.error.isRetryable, isTrue);
    });

    test('unrecognized GraphQL code falls back to HTTP status', () async {
      harness.link.enqueueError(
        serverError(400, errors: [codedError('SOMETHING_ELSE')]),
      );
      final result = await harness.adapter.push(
        makeOperation(payload: {}),
      );
      expect(result, isA<SyncFailure>());
      expect(
        (result as SyncFailure).error.code,
        SyncErrorCode.validation,
      );
    });

    test('unmapped HTTP status without parsed errors is unknown', () async {
      harness.link.enqueueError(serverError(418));
      final result = await harness.adapter.push(
        makeOperation(payload: {}),
      );
      expect(result, isA<SyncFailure>());
      final failure = result as SyncFailure;
      expect(failure.error.code, SyncErrorCode.unknown);
      expect(failure.error.isRetryable, isFalse);
      expect(failure.error.statusCode, 418);
    });

    test('HttpLinkServerException surfaces Retry-After', () async {
      harness.link.enqueueError(
        HttpLinkServerException(
          response: http.Response('{}', 429, headers: {'retry-after': '30'}),
          parsedResponse: const Response(response: {}),
          statusCode: 429,
        ),
      );
      final result = await harness.adapter.push(
        makeOperation(payload: {}),
      );
      expect(result, isA<SyncFailure>());
      final failure = result as SyncFailure;
      expect(failure.error.code, SyncErrorCode.rateLimited);
      expect(failure.error.retryAfter, const Duration(seconds: 30));
    });
  });

  group('variables validation', () {
    GraphQLTestHarness harnessWithVariables(Map<String, dynamic> variables) =>
        GraphQLTestHarness.create(
          entityMapping: {
            'note': GraphQLEntityConfig(
              create: GraphQLOperationConfig(
                document: _doc('mutation { c }'),
                variables: (_) => variables,
              ),
            ),
          },
        );

    Future<SyncResult> pushWithVariables(GraphQLTestHarness h) async {
      h.link.enqueueData({'c': true});
      return h.adapter.push(makeOperation(payload: {'x': 1}));
    }

    test('valid nested JSON variables are accepted and sent', () async {
      final h = harnessWithVariables({
        'input': {
          'id': 'entity-1',
          'tags': ['a', 'b'],
          'count': 3,
          'ratio': 1.5,
          'flag': true,
          'nothing': null,
          'nested': {
            'list': [
              1,
              'two',
              false,
              null,
              {'deep': 4}
            ],
          },
        },
      });

      final result = await pushWithVariables(h);

      expect(result, isA<SyncSuccess>());
      expect(h.link.requests, hasLength(1));
      expect(
        h.link.requests.single.variables['input'],
        isA<Map<String, dynamic>>(),
      );
    });

    test('arbitrary object at the top level is rejected', () async {
      final h = harnessWithVariables({'input': Object()});
      final result = await pushWithVariables(h);

      expect(result, isA<SyncFailure>());
      final failure = result as SyncFailure;
      expect(failure.error.code, SyncErrorCode.validation);
      expect(failure.error.isRetryable, isFalse);
      expect(failure.operationId, 'op-1');
      expect(h.link.requests, isEmpty);
    });

    test('arbitrary object nested in a map is rejected with its path',
        () async {
      final h = harnessWithVariables({
        'input': {
          'data': {'bad': Object()},
        },
      });
      final result = await pushWithVariables(h);

      expect(result, isA<SyncFailure>());
      final failure = result as SyncFailure;
      expect(failure.error.code, SyncErrorCode.validation);
      expect(failure.error.message, contains('variables.input.data.bad'));
      expect(h.link.requests, isEmpty);
    });

    test('non-string map keys are rejected', () async {
      final h = harnessWithVariables({
        'input': <dynamic, dynamic>{1: 'x'},
      });
      final result = await pushWithVariables(h);

      expect(result, isA<SyncFailure>());
      expect((result as SyncFailure).error.code, SyncErrorCode.validation);
      expect(h.link.requests, isEmpty);
    });

    test('NaN is rejected', () async {
      final h = harnessWithVariables({'ratio': double.nan});
      final result = await pushWithVariables(h);

      expect(result, isA<SyncFailure>());
      expect((result as SyncFailure).error.code, SyncErrorCode.validation);
      expect(h.link.requests, isEmpty);
    });

    test('infinities are rejected', () async {
      for (final bad in [double.infinity, double.negativeInfinity]) {
        final h = harnessWithVariables({'ratio': bad});
        final result = await pushWithVariables(h);
        expect(result, isA<SyncFailure>());
        expect(
          (result as SyncFailure).error.code,
          SyncErrorCode.validation,
        );
        expect(h.link.requests, isEmpty);
      }
    });

    test('unsupported values inside lists are rejected', () async {
      final h = harnessWithVariables({
        'items': [1, Object(), 3],
      });
      final result = await pushWithVariables(h);

      expect(result, isA<SyncFailure>());
      final failure = result as SyncFailure;
      expect(failure.error.code, SyncErrorCode.validation);
      expect(failure.error.message, contains('variables.items[1]'));
      expect(h.link.requests, isEmpty);
    });
  });

  group('contract', () {
    test('every result preserves the operation id', () async {
      harness.link
        ..enqueueData({'createNote': <String, dynamic>{}})
        ..enqueueData(null, errors: const [
          GraphQLError(
            message: 'nope',
            extensions: {'code': 'FORBIDDEN'},
          ),
        ])
        ..enqueueError(const SocketException('down'));

      final results = await Future.wait([
        harness.adapter.push(makeOperation(payload: {'a': 1})),
        harness.adapter.push(
          makeOperation(
            operationId: 'op-2',
            idempotencyKey: 'k2',
            payload: {'b': 2},
          ),
        ),
        harness.adapter.push(
          makeOperation(
            operationId: 'op-3',
            idempotencyKey: 'k3',
            payload: {'c': 3},
          ),
        ),
      ]);

      expect(results[0].operationId, 'op-1');
      expect(results[1].operationId, 'op-2');
      expect(results[2].operationId, 'op-3');
    });
  });
}

DocumentNode _doc(String source) => parseString(source);
