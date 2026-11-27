import 'package:collection/collection.dart';
import 'package:meta/meta.dart';

import '../enums/sync_operation_type.dart';
import '../utils/deep_freeze.dart';

/// Represents a local data change intent recorded atomically alongside a
/// local database write.
///
/// A [LocalMutation] is converted into a durable [SyncOperation] by an
/// `OutboxWriter` implementation.
@immutable
final class LocalMutation {
  /// Creates an immutable [LocalMutation] and deeply freezes [payload] and
  /// [metadata].
  LocalMutation({
    required this.entityType,
    required this.entityId,
    required this.type,
    Map<String, dynamic>? payload,
    this.idempotencyKey,
    DateTime? occurredAt,
    Map<String, dynamic> metadata = const <String, dynamic>{},
  }) : payload = payload == null
           ? null
           : deepFreezeMap(payload, fieldName: 'payload'),
       occurredAt = occurredAt?.toUtc(),
       metadata = deepFreezeMap(metadata, fieldName: 'metadata') {
    if (entityType.trim().isEmpty) {
      throw ArgumentError.value(entityType, 'entityType', 'Must not be empty.');
    }
    if (entityId.trim().isEmpty) {
      throw ArgumentError.value(entityId, 'entityId', 'Must not be empty.');
    }
    if (type != SyncOperationType.delete && this.payload == null) {
      throw ArgumentError.value(
        payload,
        'payload',
        'Payload is required for ${type.name} mutations.',
      );
    }
    if (idempotencyKey != null && idempotencyKey!.trim().isEmpty) {
      throw ArgumentError.value(
        idempotencyKey,
        'idempotencyKey',
        'If provided, must not be empty.',
      );
    }
  }

  static const DeepCollectionEquality _equality = DeepCollectionEquality();

  /// Logical collection or table name (for example, `'orders'`).
  final String entityType;

  /// Stable identifier of the mutated entity.
  final String entityId;

  /// Kind of mutation performed.
  final SyncOperationType type;

  /// JSON-compatible snapshot or patch. Required for `create` and `update`;
  /// optional for `delete`.
  final Map<String, dynamic>? payload;

  /// Optional caller-provided idempotency key. When `null`, the generated
  /// `operationId` is used as the idempotency key.
  final String? idempotencyKey;

  /// Optional UTC timestamp when the mutation occurred.
  final DateTime? occurredAt;

  /// Optional JSON-compatible metadata (for example, `baseVersion` or `etag`).
  final Map<String, dynamic> metadata;

  @override
  bool operator ==(Object other) =>
      identical(this, other) ||
      other is LocalMutation &&
          runtimeType == other.runtimeType &&
          entityType == other.entityType &&
          entityId == other.entityId &&
          type == other.type &&
          _equality.equals(payload, other.payload) &&
          idempotencyKey == other.idempotencyKey &&
          occurredAt == other.occurredAt &&
          _equality.equals(metadata, other.metadata);

  @override
  int get hashCode => Object.hash(
    entityType,
    entityId,
    type,
    _equality.hash(payload),
    idempotencyKey,
    occurredAt,
    _equality.hash(metadata),
  );
}
