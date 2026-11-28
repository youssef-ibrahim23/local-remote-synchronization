import 'package:graphql/client.dart';
import 'package:local_remote_synchronization/local_remote_synchronization.dart';

import 'graphql_entity_config.dart';
import 'graphql_error_mapper.dart';
import 'graphql_mutation_outcome.dart';
import 'graphql_validation_exception.dart';

/// [RemoteSyncAdapter] implementation that applies local mutations to a
/// GraphQL backend through an application-owned [GraphQLClient].
///
/// The adapter is schema-agnostic and push-only: it executes the
/// application-configured mutation documents, forwards the stable
/// [SyncOperation.idempotencyKey] through the configured variables
/// builders, interprets GraphQL transport/execution errors, and converts
/// mutation outcomes into Core [SyncResult] values.
///
/// The application owns the [GraphQLClient] — its `Link` chain,
/// authentication headers, token refresh, transport, and lifecycle. The
/// adapter never initializes an endpoint, never manages credentials, and
/// never disposes the injected client.
///
/// Idempotency: [SyncOperation.idempotencyKey] is exposed to the
/// configured variables builders, and the adapter never generates a new
/// one — retries of the same operation present the same key. However,
/// the adapter cannot guarantee that an arbitrary variables builder
/// actually includes the key in the returned variables: the application
/// must explicitly map `operation.idempotencyKey` into the mutation
/// variables whenever backend idempotency is required, and the backend
/// must implement deduplication using that key. Sending the key does
/// not itself guarantee deduplication. Delivery is at-least-once: Core
/// retries may execute the same mutation more than once.
final class GraphQLRemoteSyncAdapter implements RemoteSyncAdapter {
  /// Creates an adapter backed by [client] and [entityMapping].
  ///
  /// [entityMapping] keys are Core `entityType` values — they are not
  /// assumed to match GraphQL type or mutation names. [responseParser]
  /// interprets successful mutation `data`; when omitted, every
  /// interpretable response is reported as a plain [SyncSuccess].
  ///
  /// Throws [ArgumentError] when [entityMapping] is empty or contains a
  /// blank entity type key.
  GraphQLRemoteSyncAdapter({
    required GraphQLClient client,
    required Map<String, GraphQLEntityConfig> entityMapping,
    GraphQLResponseParser? responseParser,
  })  : _client = client,
        _entityMapping = Map<String, GraphQLEntityConfig>.unmodifiable(
          entityMapping,
        ),
        _responseParser = responseParser ?? _defaultResponseParser {
    if (entityMapping.isEmpty) {
      throw ArgumentError.value(
        entityMapping,
        'entityMapping',
        'Must contain at least one entity type.',
      );
    }
    for (final key in entityMapping.keys) {
      if (key.trim().isEmpty) {
        throw ArgumentError.value(
          key,
          'entityMapping',
          'Entity type keys must not be blank.',
        );
      }
    }
  }

  final GraphQLClient _client;
  final Map<String, GraphQLEntityConfig> _entityMapping;
  final GraphQLResponseParser _responseParser;

  static GraphQLMutationOutcome _defaultResponseParser(
    SyncOperation operation,
    Map<String, dynamic> data,
  ) =>
      const GraphQLMutationSuccess();

  @override
  Future<SyncResult> push(SyncOperation operation) async {
    final config = _entityMapping[operation.entityType];
    if (config == null) {
      return SyncFailure(
        operationId: operation.operationId,
        error: SyncError(
          code: SyncErrorCode.validation,
          message: 'No GraphQL entity mapping for entityType '
              "'${operation.entityType}'.",
          isRetryable: false,
        ),
      );
    }

    final operationConfig = config.operationFor(operation.type);
    if (operationConfig == null) {
      return SyncFailure(
        operationId: operation.operationId,
        error: SyncError(
          code: SyncErrorCode.validation,
          message: 'Entity type ${operation.entityType} has no '
              '${operation.type.name} mutation configured.',
          isRetryable: false,
        ),
      );
    }

    final Map<String, dynamic> variables;
    try {
      variables = operationConfig.variables(operation);
    } catch (error, stackTrace) {
      return SyncFailure(
        operationId: operation.operationId,
        error: SyncError(
          code: SyncErrorCode.validation,
          message: 'Variables builder for ${operation.type.name} failed: '
              '$error',
          isRetryable: false,
          cause: error,
          stackTrace: stackTrace,
        ),
      );
    }

    try {
      _validateJsonCompatible(variables, 'variables');
    } on GraphQLValidationException catch (error, stackTrace) {
      return SyncFailure(
        operationId: operation.operationId,
        error: SyncError(
          code: SyncErrorCode.validation,
          message: 'Invalid GraphQL variables: ${error.message}',
          isRetryable: false,
          cause: error,
          stackTrace: stackTrace,
        ),
      );
    }

    try {
      final result = await _client.mutate<dynamic>(
        MutationOptions(
          document: operationConfig.document,
          variables: variables,
          // Sync mutations must not read from or write to the
          // application's GraphQL cache.
          fetchPolicy: FetchPolicy.noCache,
        ),
      );
      return _interpret(result, operation);
    } catch (error, stackTrace) {
      final failure = GraphQLErrorMapper.tryMap(operation, error, stackTrace);
      if (failure != null) return failure;
      rethrow;
    }
  }

  /// Converts a completed [QueryResult] into a Core [SyncResult].
  ///
  /// GraphQL errors and link exceptions take precedence over response
  /// data: a mutation that reports errors is never treated as a success.
  SyncResult _interpret(QueryResult<dynamic> result, SyncOperation operation) {
    final exception = result.exception;
    if (exception != null) {
      final mapped = GraphQLErrorMapper.tryMap(
        operation,
        exception,
        StackTrace.current,
      );
      if (mapped != null) return mapped;
      // The mapper declined: the failure is a programming error, not a
      // routine sync failure — propagate it.
      Error.throwWithStackTrace(exception, StackTrace.current);
    }

    final data = result.data;
    if (data == null || data.isEmpty) {
      return SyncFailure(
        operationId: operation.operationId,
        error: SyncError(
          code: SyncErrorCode.serialization,
          message: 'GraphQL mutation returned no usable response data.',
          isRetryable: false,
        ),
      );
    }

    final GraphQLMutationOutcome outcome;
    try {
      outcome = _responseParser(operation, data);
    } catch (error, stackTrace) {
      return SyncFailure(
        operationId: operation.operationId,
        error: SyncError(
          code: SyncErrorCode.serialization,
          message: 'GraphQL response parser failed: $error',
          isRetryable: false,
          cause: error,
          stackTrace: stackTrace,
        ),
      );
    }

    return switch (outcome) {
      GraphQLMutationSuccess(
        :final remoteVersion,
        :final serverTimestamp,
        :final syncedData
      ) =>
        SyncSuccess(
          operationId: operation.operationId,
          remoteVersion: remoteVersion,
          serverTimestamp: serverTimestamp,
          syncedData: syncedData,
        ),
      GraphQLMutationConflict(
        :final remoteSnapshot,
        :final remoteVersion,
        :final serverTimestamp
      ) =>
        SyncConflict(
          operationId: operation.operationId,
          remoteSnapshot: remoteSnapshot,
          remoteVersion: remoteVersion,
          serverTimestamp: serverTimestamp,
        ),
      GraphQLMutationFailure(:final error) =>
        SyncFailure(operationId: operation.operationId, error: error),
    };
  }

  /// Recursively verifies that [value] is JSON-serializable: `null`,
  /// [String], [bool], finite numbers, string-keyed [Map]s, and [List]s
  /// of recursively valid values. [path] identifies the offending value
  /// in diagnostics (for example `variables.input.data`).
  static void _validateJsonCompatible(Object? value, String path) {
    if (value == null || value is String || value is bool || value is int) {
      return;
    }
    if (value is double) {
      if (!value.isFinite) {
        throw GraphQLValidationException(
          'Non-finite number at "$path".',
        );
      }
      return;
    }
    if (value is num) {
      throw GraphQLValidationException(
        'Unsupported numeric value at "$path".',
      );
    }
    if (value is Map) {
      for (final entry in value.entries) {
        final key = entry.key;
        if (key is! String) {
          throw GraphQLValidationException(
            'Non-string map key ${key.runtimeType} at "$path".',
          );
        }
        _validateJsonCompatible(entry.value, '$path.$key');
      }
      return;
    }
    if (value is List) {
      for (var index = 0; index < value.length; index++) {
        _validateJsonCompatible(value[index], '$path[$index]');
      }
      return;
    }
    throw GraphQLValidationException(
      'Unsupported value of type ${value.runtimeType} at "$path".',
    );
  }
}
