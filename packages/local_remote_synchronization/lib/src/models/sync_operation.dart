import 'package:collection/collection.dart';
import 'package:meta/meta.dart';

import '../enums/sync_operation_status.dart';
import '../enums/sync_operation_type.dart';
import '../utils/deep_freeze.dart';
import 'local_mutation.dart';
import 'sync_error.dart';

/// Durable Outbox record representing a local mutation queued for remote
/// synchronization.
@immutable
final class SyncOperation {
  /// Creates an immutable [SyncOperation] and deeply freezes [payload] and
  /// [metadata].
  SyncOperation({
    required this.operationId,
    required this.idempotencyKey,
    required this.entityType,
    required this.entityId,
    required this.type,
    required DateTime createdAt,
    DateTime? occurredAt,
    Map<String, dynamic>? payload,
    Map<String, dynamic> metadata = const <String, dynamic>{},
    this.status = SyncOperationStatus.pending,
    this.attemptCount = 0,
    this.sequenceNumber,
    DateTime? nextRetryAt,
    this.lastError,
  }) : createdAt = createdAt.toUtc(),
       occurredAt = occurredAt?.toUtc(),
       payload = payload == null
           ? null
           : deepFreezeMap(payload, fieldName: 'payload'),
       metadata = deepFreezeMap(metadata, fieldName: 'metadata'),
       nextRetryAt = nextRetryAt?.toUtc() {
    if (operationId.trim().isEmpty) {
      throw ArgumentError.value(
        operationId,
        'operationId',
        'Must not be empty.',
      );
    }
    if (idempotencyKey.trim().isEmpty) {
      throw ArgumentError.value(
        idempotencyKey,
        'idempotencyKey',
        'Must not be empty.',
      );
    }
    if (entityType.trim().isEmpty) {
      throw ArgumentError.value(entityType, 'entityType', 'Must not be empty.');
    }
    if (entityId.trim().isEmpty) {
      throw ArgumentError.value(entityId, 'entityId', 'Must not be empty.');
    }
    if (attemptCount < 0) {
      throw ArgumentError.value(attemptCount, 'attemptCount', 'Must be >= 0.');
    }
    if (sequenceNumber != null && sequenceNumber! < 0) {
      throw ArgumentError.value(
        sequenceNumber,
        'sequenceNumber',
        'Must be >= 0.',
      );
    }
    if (type != SyncOperationType.delete && this.payload == null) {
      throw ArgumentError.value(
        payload,
        'payload',
        'Payload is required for ${type.name} operations.',
      );
    }
  }

  /// Creates a [SyncOperation] from a [LocalMutation] using an externally
  /// generated [operationId].
  ///
  /// Uses [now] (or `DateTime.now()` when omitted) for [createdAt], while
  /// preserving [LocalMutation.occurredAt] separately as [occurredAt].
  /// The resulting [idempotencyKey] is `mutation.idempotencyKey ?? operationId`
  /// and remains stable across all retries and restarts.
  factory SyncOperation.fromMutation(
    LocalMutation mutation, {
    required String operationId,
    DateTime? now,
    int? sequenceNumber,
  }) {
    final createdAt = (now ?? DateTime.now()).toUtc();
    return SyncOperation(
      operationId: operationId,
      idempotencyKey: mutation.idempotencyKey ?? operationId,
      entityType: mutation.entityType,
      entityId: mutation.entityId,
      type: mutation.type,
      createdAt: createdAt,
      occurredAt: mutation.occurredAt,
      payload: mutation.payload,
      metadata: mutation.metadata,
      sequenceNumber: sequenceNumber,
    );
  }

  /// Deserializes a [SyncOperation] from a JSON map.
  factory SyncOperation.fromJson(Map<String, dynamic> json) {
    final rawOperationId = json['operationId'];
    final rawIdempotencyKey = json['idempotencyKey'];
    final rawEntityType = json['entityType'];
    final rawEntityId = json['entityId'];
    final rawType = json['type'];
    final rawStatus = json['status'];
    final rawAttemptCount = json['attemptCount'];
    final rawCreatedAt = json['createdAt'];
    final rawOccurredAt = json['occurredAt'];
    final rawPayload = json['payload'];
    final rawMetadata = json['metadata'];
    final rawSequenceNumber = json['sequenceNumber'];
    final rawNextRetryAt = json['nextRetryAt'];
    final rawLastError = json['lastError'];

    if (rawOperationId is! String) {
      throw FormatException(
        'Invalid or missing SyncOperation.operationId: $rawOperationId',
      );
    }
    if (rawIdempotencyKey is! String) {
      throw FormatException(
        'Invalid or missing SyncOperation.idempotencyKey: $rawIdempotencyKey',
      );
    }
    if (rawEntityType is! String) {
      throw FormatException(
        'Invalid or missing SyncOperation.entityType: $rawEntityType',
      );
    }
    if (rawEntityId is! String) {
      throw FormatException(
        'Invalid or missing SyncOperation.entityId: $rawEntityId',
      );
    }
    if (rawType is! String) {
      throw FormatException('Invalid or missing SyncOperation.type: $rawType');
    }
    if (rawStatus is! String) {
      throw FormatException(
        'Invalid or missing SyncOperation.status: $rawStatus',
      );
    }
    if (rawAttemptCount is! int) {
      throw FormatException(
        'Invalid or missing SyncOperation.attemptCount: $rawAttemptCount',
      );
    }
    if (rawCreatedAt is! String) {
      throw FormatException(
        'Invalid or missing SyncOperation.createdAt: $rawCreatedAt',
      );
    }
    if (rawOccurredAt != null && rawOccurredAt is! String) {
      throw FormatException('Invalid SyncOperation.occurredAt: $rawOccurredAt');
    }
    if (rawPayload != null && rawPayload is! Map) {
      throw FormatException(
        'Invalid SyncOperation.payload: expected Map or null.',
      );
    }
    if (rawMetadata != null && rawMetadata is! Map) {
      throw FormatException(
        'Invalid SyncOperation.metadata: expected Map or null.',
      );
    }
    if (rawSequenceNumber != null && rawSequenceNumber is! int) {
      throw FormatException(
        'Invalid SyncOperation.sequenceNumber: $rawSequenceNumber',
      );
    }
    if (rawNextRetryAt != null && rawNextRetryAt is! String) {
      throw FormatException(
        'Invalid SyncOperation.nextRetryAt: $rawNextRetryAt',
      );
    }
    if (rawLastError != null && rawLastError is! Map) {
      throw FormatException(
        'Invalid SyncOperation.lastError: expected Map or null.',
      );
    }

    final SyncOperationType type;
    try {
      type = SyncOperationType.values.byName(rawType);
    } on ArgumentError {
      throw FormatException('Unknown SyncOperationType: $rawType');
    }

    final SyncOperationStatus status;
    try {
      status = SyncOperationStatus.values.byName(rawStatus);
    } on ArgumentError {
      throw FormatException('Unknown SyncOperationStatus: $rawStatus');
    }

    return SyncOperation(
      operationId: rawOperationId,
      idempotencyKey: rawIdempotencyKey,
      entityType: rawEntityType,
      entityId: rawEntityId,
      type: type,
      createdAt: DateTime.parse(rawCreatedAt).toUtc(),
      occurredAt: rawOccurredAt == null
          ? null
          : DateTime.parse(rawOccurredAt).toUtc(),
      payload: rawPayload == null ? null : _castStringKeyMap(rawPayload),
      metadata: rawMetadata == null
          ? const <String, dynamic>{}
          : _castStringKeyMap(rawMetadata),
      status: status,
      attemptCount: rawAttemptCount,
      sequenceNumber: rawSequenceNumber,
      nextRetryAt: rawNextRetryAt == null
          ? null
          : DateTime.parse(rawNextRetryAt).toUtc(),
      lastError: rawLastError == null
          ? null
          : SyncError.fromJson(_castStringKeyMap(rawLastError)),
    );
  }

  static Map<String, dynamic> _castStringKeyMap(Map<Object?, Object?> raw) {
    final result = <String, dynamic>{};
    for (final entry in raw.entries) {
      final key = entry.key;
      if (key is! String) {
        throw FormatException(
          'Expected String map key, got ${key.runtimeType}.',
        );
      }
      result[key] = entry.value;
    }
    return result;
  }

  static const DeepCollectionEquality _equality = DeepCollectionEquality();

  /// Unique, stable identifier for this outbox operation.
  final String operationId;

  /// Stable idempotency key sent to remote backends across retries.
  final String idempotencyKey;

  /// Logical collection or table name.
  final String entityType;

  /// Stable identifier of the target entity.
  final String entityId;

  /// Kind of mutation.
  final SyncOperationType type;

  /// Deeply immutable JSON-compatible payload snapshot or patch.
  final Map<String, dynamic>? payload;

  /// Deeply immutable JSON-compatible metadata.
  final Map<String, dynamic> metadata;

  /// Current outbox lifecycle status.
  final SyncOperationStatus status;

  /// Number of push attempts completed so far.
  final int attemptCount;

  /// UTC timestamp when the outbox operation was created/persisted.
  final DateTime createdAt;

  /// Optional UTC timestamp when the original local mutation occurred.
  final DateTime? occurredAt;

  /// Optional monotonic sequence number assigned by local storage for
  /// deterministic ordering within the same timestamp.
  final int? sequenceNumber;

  /// Optional UTC timestamp when the operation becomes eligible for retry.
  final DateTime? nextRetryAt;

  /// Last recorded synchronization error, if any.
  final SyncError? lastError;

  /// Returns a copy with updated lifecycle fields.
  ///
  /// Use [clearSequenceNumber], [clearNextRetryAt], or [clearLastError] to
  /// explicitly reset nullable fields to `null`.
  SyncOperation copyWith({
    SyncOperationStatus? status,
    int? attemptCount,
    int? sequenceNumber,
    bool clearSequenceNumber = false,
    DateTime? nextRetryAt,
    bool clearNextRetryAt = false,
    SyncError? lastError,
    bool clearLastError = false,
  }) => SyncOperation(
    operationId: operationId,
    idempotencyKey: idempotencyKey,
    entityType: entityType,
    entityId: entityId,
    type: type,
    createdAt: createdAt,
    occurredAt: occurredAt,
    payload: payload,
    metadata: metadata,
    status: status ?? this.status,
    attemptCount: attemptCount ?? this.attemptCount,
    sequenceNumber: clearSequenceNumber
        ? null
        : (sequenceNumber ?? this.sequenceNumber),
    nextRetryAt: clearNextRetryAt ? null : (nextRetryAt ?? this.nextRetryAt),
    lastError: clearLastError ? null : (lastError ?? this.lastError),
  );

  /// Serializes this [SyncOperation] to a JSON-compatible map.
  Map<String, dynamic> toJson() => <String, dynamic>{
    'operationId': operationId,
    'idempotencyKey': idempotencyKey,
    'entityType': entityType,
    'entityId': entityId,
    'type': type.name,
    'payload': payload,
    'metadata': metadata,
    'status': status.name,
    'attemptCount': attemptCount,
    'createdAt': createdAt.toIso8601String(),
    if (occurredAt != null) 'occurredAt': occurredAt!.toIso8601String(),
    if (sequenceNumber != null) 'sequenceNumber': sequenceNumber,
    if (nextRetryAt != null) 'nextRetryAt': nextRetryAt!.toIso8601String(),
    if (lastError != null) 'lastError': lastError!.toJson(),
  };

  @override
  bool operator ==(Object other) =>
      identical(this, other) ||
      other is SyncOperation &&
          runtimeType == other.runtimeType &&
          operationId == other.operationId &&
          idempotencyKey == other.idempotencyKey &&
          entityType == other.entityType &&
          entityId == other.entityId &&
          type == other.type &&
          _equality.equals(payload, other.payload) &&
          _equality.equals(metadata, other.metadata) &&
          status == other.status &&
          attemptCount == other.attemptCount &&
          createdAt == other.createdAt &&
          occurredAt == other.occurredAt &&
          sequenceNumber == other.sequenceNumber &&
          nextRetryAt == other.nextRetryAt &&
          lastError == other.lastError;

  @override
  int get hashCode => Object.hash(
    operationId,
    idempotencyKey,
    entityType,
    entityId,
    type,
    _equality.hash(payload),
    _equality.hash(metadata),
    status,
    attemptCount,
    createdAt,
    occurredAt,
    sequenceNumber,
    nextRetryAt,
    lastError,
  );
}
