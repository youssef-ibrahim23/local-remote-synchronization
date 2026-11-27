import 'dart:convert';

import 'package:local_remote_synchronization/local_remote_synchronization.dart';

/// Maps between Core [SyncOperation] models and SQLite row maps.
final class SqfliteOutboxMapper {
  SqfliteOutboxMapper._();

  /// Converts [operation] into a parameterized SQLite insert map.
  static Map<String, Object?> toInsertRow(
    SyncOperation operation, {
    required DateTime? occurredAt,
    required DateTime updatedAt,
  }) =>
      <String, Object?>{
        'operation_id': operation.operationId,
        'idempotency_key': operation.idempotencyKey,
        'entity_type': operation.entityType,
        'entity_id': operation.entityId,
        'operation_type': operation.type.name,
        'payload_json':
            operation.payload == null ? null : jsonEncode(operation.payload),
        'metadata_json': jsonEncode(operation.metadata),
        'status': operation.status.name,
        'attempt_count': operation.attemptCount,
        'created_at_ms': operation.createdAt.toUtc().millisecondsSinceEpoch,
        'occurred_at_ms': occurredAt?.toUtc().millisecondsSinceEpoch,
        'updated_at_ms': updatedAt.toUtc().millisecondsSinceEpoch,
        'next_retry_at_ms':
            operation.nextRetryAt?.toUtc().millisecondsSinceEpoch,
        'last_error_json': operation.lastError == null
            ? null
            : jsonEncode(operation.lastError!.toJson()),
      };

  /// Reconstructs a [SyncOperation] from a SQLite query [row].
  ///
  /// Throws a [FormatException] if the persisted row contains corrupted or
  /// invalid data.
  static SyncOperation fromRow(Map<String, Object?> row) {
    final operationId = row['operation_id'];
    if (operationId is! String || operationId.trim().isEmpty) {
      throw FormatException(
        'Corrupted outbox row: invalid operation_id ($operationId).',
      );
    }

    try {
      final idempotencyKey = _requireString(row, 'idempotency_key');
      final entityType = _requireString(row, 'entity_type');
      final entityId = _requireString(row, 'entity_id');
      final typeName = _requireString(row, 'operation_type');
      final statusName = _requireString(row, 'status');
      final metadataJson = _requireString(row, 'metadata_json');
      final attemptCount = _requireInt(row, 'attempt_count');
      final createdAtMs = _requireInt(row, 'created_at_ms');
      final sequenceNumber = _optionalInt(row, 'sequence_number');
      final nextRetryAtMs = _optionalInt(row, 'next_retry_at_ms');
      final payloadJson = _optionalString(row, 'payload_json');
      final lastErrorJson = _optionalString(row, 'last_error_json');

      final type = SyncOperationType.values.asNameMap()[typeName];
      if (type == null) {
        throw FormatException('Unknown SyncOperationType: $typeName');
      }

      final status = SyncOperationStatus.values.asNameMap()[statusName];
      if (status == null) {
        throw FormatException('Unknown SyncOperationStatus: $statusName');
      }

      final payload =
          payloadJson == null ? null : _decodeJsonMap(payloadJson, 'payload');
      final metadata = _decodeJsonMap(metadataJson, 'metadata');
      final lastError = lastErrorJson == null
          ? null
          : SyncError.fromJson(_decodeJsonMap(lastErrorJson, 'lastError'));

      return SyncOperation(
        operationId: operationId,
        idempotencyKey: idempotencyKey,
        entityType: entityType,
        entityId: entityId,
        type: type,
        createdAt: DateTime.fromMillisecondsSinceEpoch(
          createdAtMs,
          isUtc: true,
        ),
        payload: payload,
        metadata: metadata,
        status: status,
        attemptCount: attemptCount,
        sequenceNumber: sequenceNumber,
        nextRetryAt: nextRetryAtMs == null
            ? null
            : DateTime.fromMillisecondsSinceEpoch(
                nextRetryAtMs,
                isUtc: true,
              ),
        lastError: lastError,
      );
    } on FormatException catch (e) {
      throw FormatException(
        'Corrupted outbox row for operation "$operationId": ${e.message}',
      );
    }
  }

  static String _requireString(Map<String, Object?> row, String key) {
    final value = row[key];
    if (value is! String) {
      throw FormatException('Missing or invalid string column "$key".');
    }
    return value;
  }

  static String? _optionalString(Map<String, Object?> row, String key) {
    final value = row[key];
    if (value is! String?) {
      throw FormatException('Invalid string column "$key".');
    }
    return value;
  }

  static int _requireInt(Map<String, Object?> row, String key) {
    final value = row[key];
    if (value is! int) {
      throw FormatException('Missing or invalid integer column "$key".');
    }
    return value;
  }

  static int? _optionalInt(Map<String, Object?> row, String key) {
    final value = row[key];
    if (value is! int?) {
      throw FormatException('Invalid integer column "$key".');
    }
    return value;
  }

  static Map<String, dynamic> _decodeJsonMap(String raw, String field) {
    final decoded = jsonDecode(raw);
    if (decoded is! Map<String, dynamic>) {
      throw FormatException('Expected JSON object for "$field".');
    }
    return decoded;
  }
}
