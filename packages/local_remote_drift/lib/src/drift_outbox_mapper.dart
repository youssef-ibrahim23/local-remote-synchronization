import 'dart:convert';

import 'package:drift/drift.dart';
import 'package:local_remote_synchronization/local_remote_synchronization.dart';

/// Maps between Core [SyncOperation] models and Drift SQLite rows/variables.
final class DriftOutboxMapper {
  DriftOutboxMapper._();

  /// Converts [operation] into parameterized Drift [Variable]s matching the
  /// insert column order in `DriftOutboxWriter`.
  static List<Variable<Object>> toInsertVariables(
    SyncOperation operation, {
    required DateTime updatedAt,
  }) =>
      <Variable<Object>>[
        Variable.withString(operation.operationId),
        Variable.withString(operation.idempotencyKey),
        Variable.withString(operation.entityType),
        Variable.withString(operation.entityId),
        Variable.withString(operation.type.name),
        Variable<String>(
          operation.payload == null ? null : jsonEncode(operation.payload),
        ),
        Variable.withString(jsonEncode(operation.metadata)),
        Variable.withString(operation.status.name),
        Variable.withInt(operation.attemptCount),
        Variable.withInt(operation.createdAt.toUtc().millisecondsSinceEpoch),
        Variable<int>(operation.occurredAt?.toUtc().millisecondsSinceEpoch),
        Variable.withInt(updatedAt.toUtc().millisecondsSinceEpoch),
        Variable<int>(operation.nextRetryAt?.toUtc().millisecondsSinceEpoch),
        Variable<String>(
          operation.lastError == null
              ? null
              : jsonEncode(operation.lastError!.toJson()),
        ),
      ];

  /// Reconstructs a [SyncOperation] from a Drift [QueryRow.data] map.
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
      final occurredAtMs = _optionalInt(row, 'occurred_at_ms');
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
        occurredAt: occurredAtMs == null
            ? null
            : DateTime.fromMillisecondsSinceEpoch(
                occurredAtMs,
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
