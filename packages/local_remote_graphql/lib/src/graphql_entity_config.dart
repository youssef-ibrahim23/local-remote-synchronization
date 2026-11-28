import 'package:gql/ast.dart';
import 'package:local_remote_synchronization/local_remote_synchronization.dart';

/// Builds the GraphQL variables map for a mutation execution.
///
/// The builder receives the original [SyncOperation] and must return a
/// JSON-compatible variables map matching the configured mutation's
/// variable definitions. Applications are responsible for mapping fields
/// such as `entityId`, `payload`, `metadata`, and `idempotencyKey` into
/// the variable names their backend expects.
///
/// `operation.idempotencyKey` is stable across retries, but the adapter
/// does not inject it automatically — the builder must explicitly include
/// it in the returned variables when backend idempotency is required.
typedef GraphQLVariablesBuilder = Map<String, dynamic> Function(
    SyncOperation operation);

/// Configuration for a single GraphQL mutation (create, update, or
/// delete) executed by the remote sync adapter.
final class GraphQLOperationConfig {
  /// Creates an operation configuration.
  ///
  /// [document] is the parsed GraphQL mutation document. [variables]
  /// builds the variables sent with each execution.
  const GraphQLOperationConfig({
    required this.document,
    required this.variables,
  });

  /// Parsed GraphQL mutation document.
  final DocumentNode document;

  /// Builds mutation variables from the [SyncOperation].
  final GraphQLVariablesBuilder variables;
}

/// Per-entity GraphQL mutation configuration.
///
/// Each field is optional; a missing field means the corresponding
/// [SyncOperationType] is unsupported for this entity and produces a
/// non-retryable validation failure at push time.
final class GraphQLEntityConfig {
  /// Creates an entity configuration.
  const GraphQLEntityConfig({this.create, this.update, this.delete});

  /// Mutation executed for [SyncOperationType.create].
  final GraphQLOperationConfig? create;

  /// Mutation executed for [SyncOperationType.update].
  final GraphQLOperationConfig? update;

  /// Mutation executed for [SyncOperationType.delete].
  final GraphQLOperationConfig? delete;

  /// Returns the operation configuration for [type], or `null` when the
  /// entity does not support that operation.
  GraphQLOperationConfig? operationFor(SyncOperationType type) =>
      switch (type) {
        SyncOperationType.create => create,
        SyncOperationType.update => update,
        SyncOperationType.delete => delete,
      };
}
