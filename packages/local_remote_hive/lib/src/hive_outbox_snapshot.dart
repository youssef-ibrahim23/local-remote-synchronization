import 'dart:convert';

import 'package:local_remote_synchronization/local_remote_synchronization.dart';

import 'hive_outbox_schema.dart';

/// Exception thrown when the persisted outbox snapshot is corrupted or
/// contains invalid data.
///
/// The original persisted data is preserved — this exception is thrown
/// instead of silently skipping or deleting corrupted entries.
final class HiveOutboxCorruptionException implements Exception {
  /// Creates a [HiveOutboxCorruptionException] with [message] and optional
  /// [rawData] for debugging.
  const HiveOutboxCorruptionException(this.message, [this.rawData]);

  /// Human-readable description of the corruption.
  final String message;

  /// The raw persisted data that failed to parse, if available.
  final String? rawData;

  @override
  String toString() => 'HiveOutboxCorruptionException: $message'
      '${rawData != null ? ' (raw: $rawData)' : ''}';
}

/// Exception thrown when the persisted snapshot version is unsupported.
final class HiveOutboxVersionException implements Exception {
  /// Creates a [HiveOutboxVersionException].
  const HiveOutboxVersionException({
    required this.foundVersion,
    required this.minSupportedVersion,
    required this.currentVersion,
  });

  /// The schema version found in the persisted snapshot.
  final int foundVersion;

  /// The minimum schema version supported by this adapter.
  final int minSupportedVersion;

  /// The current schema version written by this adapter.
  final int currentVersion;

  @override
  String toString() =>
      'HiveOutboxVersionException: Found snapshot schema version '
      '$foundVersion, but this adapter supports versions '
      '$minSupportedVersion through $currentVersion. '
      'Migrate or downgrade the adapter to read this snapshot.';
}

/// In-memory representation of the persisted outbox snapshot.
///
/// The snapshot contains all active [SyncOperation] records and the
/// `lastSequenceNumber` counter. It is serialized to a single JSON string
/// and stored under [HiveOutboxSchema.snapshotKey] in the outbox box.
///
/// [lastSequenceNumber] and [operations] are mutable to support transaction
/// staging. [schemaVersion] is immutable.
final class HiveOutboxSnapshot {
  /// Creates a [HiveOutboxSnapshot].
  HiveOutboxSnapshot({
    required this.schemaVersion,
    required this.lastSequenceNumber,
    required this.operations,
  });

  /// Creates an empty snapshot with the current schema version.
  factory HiveOutboxSnapshot.empty() => HiveOutboxSnapshot(
        schemaVersion: HiveOutboxSchema.currentVersion,
        lastSequenceNumber: 0,
        operations: <SyncOperation>[],
      );

  /// Deserializes a [HiveOutboxSnapshot] from a JSON string.
  ///
  /// Throws [HiveOutboxCorruptionException] if the JSON is malformed or
  /// contains invalid operation data.
  /// Throws [HiveOutboxVersionException] if the schema version is
  /// unsupported.
  factory HiveOutboxSnapshot.fromJson(String json) {
    final Object? decoded;
    try {
      decoded = jsonDecode(json);
    } on FormatException catch (e) {
      throw HiveOutboxCorruptionException(
        'Invalid snapshot JSON: ${e.message}',
        json,
      );
    }

    if (decoded is! Map<String, dynamic>) {
      throw HiveOutboxCorruptionException(
        'Snapshot is not a JSON object',
        json,
      );
    }

    return HiveOutboxSnapshot._fromMap(decoded);
  }

  /// Deserializes from an already-decoded JSON map.
  factory HiveOutboxSnapshot._fromMap(Map<String, dynamic> json) {
    final version = json['schemaVersion'];
    if (version is! int) {
      throw HiveOutboxCorruptionException(
        'Missing or invalid schemaVersion field',
        jsonEncode(json),
      );
    }

    if (version > HiveOutboxSchema.currentVersion) {
      throw HiveOutboxVersionException(
        foundVersion: version,
        minSupportedVersion: HiveOutboxSchema.minSupportedVersion,
        currentVersion: HiveOutboxSchema.currentVersion,
      );
    }

    if (version < HiveOutboxSchema.minSupportedVersion) {
      throw HiveOutboxVersionException(
        foundVersion: version,
        minSupportedVersion: HiveOutboxSchema.minSupportedVersion,
        currentVersion: HiveOutboxSchema.currentVersion,
      );
    }

    // Migrate older versions to current.
    var workingJson = json;
    var workingVersion = version;
    while (workingVersion < HiveOutboxSchema.currentVersion) {
      workingJson = _migrate(workingJson, workingVersion);
      workingVersion++;
    }

    // Parse current version.
    return HiveOutboxSnapshot._fromCurrentMap(workingJson);
  }

  /// Deserializes from an already-decoded JSON map at the current version.
  factory HiveOutboxSnapshot._fromCurrentMap(Map<String, dynamic> json) {
    final lastSeq = json['lastSequenceNumber'];
    final opsJson = json['operations'];

    if (lastSeq is! int) {
      throw HiveOutboxCorruptionException(
        'Missing or invalid lastSequenceNumber field',
        jsonEncode(json),
      );
    }

    if (opsJson is! List) {
      throw HiveOutboxCorruptionException(
        'Missing or invalid operations field',
        jsonEncode(json),
      );
    }

    final operations = <SyncOperation>[];
    for (var i = 0; i < opsJson.length; i++) {
      final opJson = opsJson[i];
      if (opJson is! Map<String, dynamic>) {
        throw HiveOutboxCorruptionException(
          'Operation at index $i is not a JSON object',
          jsonEncode(opJson),
        );
      }
      try {
        operations.add(SyncOperation.fromJson(opJson));
      } catch (e) {
        throw HiveOutboxCorruptionException(
          'Invalid operation at index $i: $e',
          jsonEncode(opJson),
        );
      }
    }

    return HiveOutboxSnapshot(
      schemaVersion: HiveOutboxSchema.currentVersion,
      lastSequenceNumber: lastSeq,
      operations: operations,
    );
  }

  /// The schema version of this snapshot.
  final int schemaVersion;

  /// The last assigned sequence number. The next operation will receive
  /// `lastSequenceNumber + 1`.
  int lastSequenceNumber;

  /// All active operations in the outbox.
  final List<SyncOperation> operations;

  /// Creates a mutable copy of this snapshot for transaction staging.
  HiveOutboxSnapshot copy() => HiveOutboxSnapshot(
        schemaVersion: schemaVersion,
        lastSequenceNumber: lastSequenceNumber,
        operations: List<SyncOperation>.of(operations),
      );

  /// Serializes this snapshot to a JSON string for Hive storage.
  String toJson() => jsonEncode(<String, dynamic>{
        'schemaVersion': schemaVersion,
        'lastSequenceNumber': lastSequenceNumber,
        'operations': operations.map((op) => op.toJson()).toList(),
      });

  /// Migrates a snapshot from [fromVersion] to the next version.
  static Map<String, dynamic> _migrate(
    Map<String, dynamic> json,
    int fromVersion,
  ) {
    switch (fromVersion) {
      case 1:
        // v1 -> v2: ensure lastSequenceNumber is present.
        // v1 snapshots may lack this field; compute from operations.
        if (!json.containsKey('lastSequenceNumber')) {
          final opsJson = json['operations'];
          var maxSeq = 0;
          if (opsJson is List) {
            for (final opJson in opsJson) {
              if (opJson is Map<String, dynamic>) {
                final seq = opJson['sequenceNumber'];
                if (seq is int && seq > maxSeq) {
                  maxSeq = seq;
                }
              }
            }
          }
          json['lastSequenceNumber'] = maxSeq;
        }
        json['schemaVersion'] = 2;
        return json;
      default:
        throw HiveOutboxVersionException(
          foundVersion: fromVersion,
          minSupportedVersion: HiveOutboxSchema.minSupportedVersion,
          currentVersion: HiveOutboxSchema.currentVersion,
        );
    }
  }
}
