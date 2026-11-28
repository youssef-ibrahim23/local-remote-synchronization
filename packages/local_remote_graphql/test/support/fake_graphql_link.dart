import 'dart:async';

import 'package:gql/language.dart';
import 'package:graphql/client.dart';
import 'package:local_remote_graphql/local_remote_graphql.dart';
import 'package:local_remote_synchronization/local_remote_synchronization.dart';

/// A queued outcome for [FakeGraphQLLink].
sealed class FakeGraphQLResponse {
  const FakeGraphQLResponse();
}

/// Returns a GraphQL response with [data] and optional [errors].
final class FakeGraphQLData extends FakeGraphQLResponse {
  const FakeGraphQLData(this.data, {this.errors});

  final Map<String, dynamic>? data;
  final List<GraphQLError>? errors;
}

/// Throws [error] from the link stream.
final class FakeGraphQLThrow extends FakeGraphQLResponse {
  const FakeGraphQLThrow(this.error, [this.stackTrace]);

  final Object error;
  final StackTrace? stackTrace;
}

/// Deterministic [Link] that records requests and replays queued
/// responses/errors. No network access.
final class FakeGraphQLLink extends Link {
  final List<Request> requests = <Request>[];
  final List<FakeGraphQLResponse> _queue = <FakeGraphQLResponse>[];

  void enqueueData(Map<String, dynamic>? data, {List<GraphQLError>? errors}) =>
      _queue.add(FakeGraphQLData(data, errors: errors));

  void enqueueError(Object error, [StackTrace? stackTrace]) =>
      _queue.add(FakeGraphQLThrow(error, stackTrace));

  /// Enqueues a default successful empty-data response.
  void enqueueEmptySuccess() => enqueueData(const {});

  int get pending => _queue.length;

  @override
  Stream<Response> request(Request request, [NextLink? forward]) {
    requests.add(request);
    if (_queue.isEmpty) {
      return Stream.error(
        StateError('FakeGraphQLLink: no queued response for request.'),
      );
    }
    final next = _queue.removeAt(0);
    return switch (next) {
      FakeGraphQLData(:final data, :final errors) => Stream.value(
          Response(data: data, errors: errors, response: const {}),
        ),
      FakeGraphQLThrow(:final error, :final stackTrace) => Stream.fromFuture(
          Future<Response>.error(error, stackTrace ?? StackTrace.current),
        ),
    };
  }
}

/// Builds a [SyncOperation] for adapter tests.
SyncOperation makeOperation({
  String operationId = 'op-1',
  String idempotencyKey = 'idem-1',
  String entityType = 'note',
  String entityId = 'entity-1',
  SyncOperationType type = SyncOperationType.create,
  Map<String, dynamic>? payload,
  Map<String, dynamic> metadata = const {},
}) =>
    SyncOperation(
      operationId: operationId,
      idempotencyKey: idempotencyKey,
      entityType: entityType,
      entityId: entityId,
      type: type,
      createdAt: DateTime.utc(2024),
      payload: payload,
      metadata: metadata,
    );

/// Test harness bundling a [FakeGraphQLLink], a real [GraphQLClient]
/// wired to it, and the adapter under test.
final class GraphQLTestHarness {
  GraphQLTestHarness._({
    required this.link,
    required this.client,
    required this.adapter,
  });

  factory GraphQLTestHarness.create({
    Map<String, GraphQLEntityConfig>? entityMapping,
    GraphQLResponseParser? responseParser,
  }) {
    final link = FakeGraphQLLink();
    final client = GraphQLClient(link: link, cache: GraphQLCache());
    final adapter = GraphQLRemoteSyncAdapter(
      client: client,
      entityMapping: entityMapping ?? _defaultMapping(),
      responseParser: responseParser,
    );
    return GraphQLTestHarness._(link: link, client: client, adapter: adapter);
  }

  static Map<String, GraphQLEntityConfig> _defaultMapping() => {
        'note': GraphQLEntityConfig(
          create: GraphQLOperationConfig(
            document: parseString(
              r'mutation CreateNote($input: NoteInput!) '
              r'{ createNote(input: $input) { id version } }',
            ),
            variables: (op) => {
              'input': {
                'id': op.entityId,
                'idempotencyKey': op.idempotencyKey,
                'data': op.payload,
              },
            },
          ),
          update: GraphQLOperationConfig(
            document: parseString(
              r'mutation UpdateNote($id: ID!, $patch: NotePatch!) '
              r'{ updateNote(id: $id, patch: $patch) { id version } }',
            ),
            variables: (op) => {
              'id': op.entityId,
              'patch': op.payload,
              'idempotencyKey': op.idempotencyKey,
            },
          ),
          delete: GraphQLOperationConfig(
            document: parseString(
              r'mutation DeleteNote($id: ID!, $key: String!) '
              r'{ deleteNote(id: $id, idempotencyKey: $key) }',
            ),
            variables: (op) => {
              'id': op.entityId,
              'key': op.idempotencyKey,
            },
          ),
        ),
        'todo': GraphQLEntityConfig(
          create: GraphQLOperationConfig(
            document: parseString(
              r'mutation CreateTodo($input: TodoInput!) '
              r'{ createTodo(input: $input) { id } }',
            ),
            variables: (op) => {
              'input': {'id': op.entityId, 'payload': op.payload},
            },
          ),
        ),
      };

  final FakeGraphQLLink link;
  final GraphQLClient client;
  final GraphQLRemoteSyncAdapter adapter;
}
