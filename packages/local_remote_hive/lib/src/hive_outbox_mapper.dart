import 'dart:convert';

import 'package:local_remote_synchronization/local_remote_synchronization.dart';

/// Maps between Core [SyncOperation] models and Hive-stored JSON strings.
final class HiveOutboxMapper {
  HiveOutboxMapper._();

  /// Serializes [operation] to a JSON string for Hive storage.
  static String toJson(SyncOperation operation) =>
      jsonEncode(operation.toJson());

  /// Deserializes a [SyncOperation] from a Hive-stored JSON string.
  ///
  /// Throws a [FormatException] if the persisted data contains corrupted or
  /// invalid JSON.
  static SyncOperation fromJson(String json) {
    try {
      final decoded = jsonDecode(json);
      if (decoded is! Map<String, dynamic>) {
        throw const FormatException('Expected JSON object for SyncOperation.');
      }
      return SyncOperation.fromJson(decoded);
    } on FormatException {
      rethrow;
    } on Object catch (e) {
      throw FormatException('Corrupted outbox entry: $e');
    }
  }
}
