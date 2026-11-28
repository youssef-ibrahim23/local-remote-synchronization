import 'dart:convert';

import 'package:cloud_firestore/cloud_firestore.dart';
import 'package:crypto/crypto.dart';
import 'package:local_remote_synchronization/local_remote_synchronization.dart';

import 'firestore_entity_config.dart';
import 'firestore_error_mapper.dart';
import 'firestore_validation_exception.dart';

/// [RemoteSyncAdapter] implementation that applies local mutations to a
/// Cloud Firestore backend.
///
/// The adapter is push-only: it never pulls remote changes, never listens
/// to snapshots, and never performs conflict resolution — a detected
/// conflict is reported as [SyncConflict] and Core persists it as a
/// dead-letter.
///
/// Delivery is **at-least-once**. Every operation is applied inside a
/// single Firestore transaction together with a write to a dedicated
/// idempotency ledger (`idempotencyCollectionPath`, default
/// `'_sync_operations'`). The ledger document ID is a SHA-256 hash of
/// `SyncOperation.idempotencyKey`, and the record stores the operation
/// identity and a payload fingerprint. A retry of an already-applied
/// operation replays the recorded result without re-applying the
/// mutation; the same key reused with a different operation identity or
/// payload is rejected as a non-retryable validation failure. This does
/// not make delivery exactly-once and does not cover side effects outside
/// the transaction.
///
/// The application owns Firebase initialization, the
/// [FirebaseFirestore] instance, authentication, and Security Rules. The
/// adapter never initializes Firebase and never disposes the injected
/// Firestore instance.
///
/// [FirestoreEntityConfig.versionField] enables application-managed
/// integer versioning: creates initialize it to `1`, updates increment
/// it inside the transaction, and `metadata[expectedVersionKey]` (default
/// `'baseVersion'`) is compared against the stored value for optimistic
/// concurrency. This is **not** Firestore `updateTime` concurrency.
///
/// Run **one sync engine per outbox**. Core provides no cross-instance
/// atomic claiming, so two engines sharing an outbox can push the same
/// operation concurrently.
final class FirestoreRemoteSyncAdapter implements RemoteSyncAdapter {
  /// Creates an adapter bound to an application-owned [firestore]
  /// instance and an explicit `entityType` → [FirestoreEntityConfig]
  /// mapping.
  ///
  /// [idempotencyCollectionPath] is the collection holding ledger
  /// documents; it must be readable/writable under the application's
  /// Security Rules. [expectedVersionKey] selects the
  /// `SyncOperation.metadata` key carrying the expected version.
  FirestoreRemoteSyncAdapter({
    required FirebaseFirestore firestore,
    required Map<String, FirestoreEntityConfig> entityMapping,
    String idempotencyCollectionPath = '_sync_operations',
    String expectedVersionKey = 'baseVersion',
  })  : _firestore = firestore,
        _entityMapping = _validatedMapping(entityMapping),
        idempotencyCollectionPath =
            FirestoreEntityConfig.validateCollectionPath(
          idempotencyCollectionPath,
        ),
        expectedVersionKey = _validateFieldName(
          expectedVersionKey,
          'expectedVersionKey',
        );

  final FirebaseFirestore _firestore;
  final Map<String, FirestoreEntityConfig> _entityMapping;

  /// Collection path holding idempotency ledger documents.
  final String idempotencyCollectionPath;

  /// `SyncOperation.metadata` key holding the expected version.
  final String expectedVersionKey;

  static Map<String, FirestoreEntityConfig> _validatedMapping(
    Map<String, FirestoreEntityConfig> mapping,
  ) {
    if (mapping.isEmpty) {
      throw ArgumentError.value(
        mapping,
        'entityMapping',
        'Must contain at least one entityType mapping.',
      );
    }
    final result = <String, FirestoreEntityConfig>{};
    for (final entry in mapping.entries) {
      if (entry.key.trim().isEmpty) {
        throw ArgumentError.value(
          entry.key,
          'entityMapping',
          'Entity type keys must not be blank.',
        );
      }
      result[entry.key] = entry.value;
    }
    return Map.unmodifiable(result);
  }

  static String _validateFieldName(String name, String argumentName) {
    final trimmed = name.trim();
    if (trimmed.isEmpty) {
      throw ArgumentError.value(name, argumentName, 'Must not be blank.');
    }
    return trimmed;
  }

  @override
  Future<SyncResult> push(SyncOperation operation) async {
    final config = _entityMapping[operation.entityType];
    if (config == null) {
      return SyncFailure(
        operationId: operation.operationId,
        error: SyncError(
          code: SyncErrorCode.validation,
          message: 'No Firestore collection mapping configured for '
              "entityType '${operation.entityType}'.",
          isRetryable: false,
        ),
      );
    }

    try {
      // Validate all caller input before any Firestore access.
      _validateEntityId(operation.entityId);
      final body = _writePayload(operation, config);
      final expectedVersion = _expectedVersion(operation, config);

      return await _firestore.runTransaction<SyncResult>(
        (transaction) => _applyInTransaction(
          transaction,
          operation,
          config,
          body,
          expectedVersion,
        ),
      );
    } catch (error, stackTrace) {
      final failure = FirestoreErrorMapper.tryMap(operation, error, stackTrace);
      if (failure != null) return failure;
      rethrow;
    }
  }

  // ------------------------------------------------------------------
  // Transaction
  // ------------------------------------------------------------------

  /// Applies [operation] atomically with its idempotency-ledger write.
  ///
  /// Firestore may execute this callback more than once, so it performs
  /// no external side effects, completes all reads before any write, and
  /// mutates no shared state.
  Future<SyncResult> _applyInTransaction(
    Transaction transaction,
    SyncOperation operation,
    FirestoreEntityConfig config,
    Map<String, dynamic> body,
    int? expectedVersion,
  ) async {
    final ledgerRef = _firestore.doc(
        '$idempotencyCollectionPath/${_ledgerId(operation.idempotencyKey)}');
    final entityRef =
        _firestore.collection(config.collectionPath).doc(operation.entityId);

    final ledgerSnapshot = await transaction.get(ledgerRef);
    final ledger = ledgerSnapshot.data();
    if (ledger != null) {
      return _ledgerResult(operation, ledger);
    }

    final entitySnapshot = await transaction.get(entityRef);
    final exists = entitySnapshot.exists;
    final remoteData = entitySnapshot.data();

    return switch (operation.type) {
      SyncOperationType.create => _create(
          transaction,
          operation,
          config,
          entityRef,
          ledgerRef,
          exists,
          remoteData,
          body,
        ),
      SyncOperationType.update => _update(
          transaction,
          operation,
          config,
          entityRef,
          ledgerRef,
          exists,
          remoteData,
          body,
          expectedVersion,
        ),
      SyncOperationType.delete => _delete(
          transaction,
          operation,
          config,
          entityRef,
          ledgerRef,
          exists,
          remoteData,
          expectedVersion,
        ),
    };
  }

  SyncResult _create(
    Transaction transaction,
    SyncOperation operation,
    FirestoreEntityConfig config,
    DocumentReference<Map<String, dynamic>> entityRef,
    DocumentReference<Map<String, dynamic>> ledgerRef,
    bool exists,
    Map<String, dynamic>? remoteData,
    Map<String, dynamic> body,
  ) {
    if (exists) {
      return SyncConflict(
        operationId: operation.operationId,
        remoteSnapshot: remoteData ?? const {},
        remoteVersion: _remoteVersion(config, remoteData),
      );
    }

    final data = Map<String, dynamic>.from(body);
    final versionField = config.versionField;
    if (versionField != null) {
      data[versionField] = 1;
    }
    transaction
      ..set(entityRef, data)
      ..set(
        ledgerRef,
        _ledgerData(
          operation,
          remoteVersion: versionField != null ? '1' : null,
          syncedData: data,
        ),
      );
    return SyncSuccess(
      operationId: operation.operationId,
      remoteVersion: versionField != null ? '1' : null,
      syncedData: data,
    );
  }

  SyncResult _update(
    Transaction transaction,
    SyncOperation operation,
    FirestoreEntityConfig config,
    DocumentReference<Map<String, dynamic>> entityRef,
    DocumentReference<Map<String, dynamic>> ledgerRef,
    bool exists,
    Map<String, dynamic>? remoteData,
    Map<String, dynamic> body,
    int? expectedVersion,
  ) {
    if (!exists) {
      return SyncFailure(
        operationId: operation.operationId,
        error: SyncError(
          code: SyncErrorCode.notFound,
          message: 'Cannot update entity ${operation.entityId}: no remote '
              'document exists in ${config.collectionPath}.',
          isRetryable: false,
        ),
      );
    }

    final versionField = config.versionField;
    int? newVersion;
    if (versionField != null) {
      final current = remoteData?[versionField];
      if (expectedVersion != null &&
          (current is! int || current != expectedVersion)) {
        return SyncConflict(
          operationId: operation.operationId,
          remoteSnapshot: remoteData ?? const {},
          remoteVersion: _remoteVersion(config, remoteData),
        );
      }
      if (current is! int) {
        // Versioning is configured but the stored value is not an
        // integer — the document violates the versioning contract, so
        // no safe increment is possible.
        return SyncConflict(
          operationId: operation.operationId,
          remoteSnapshot: remoteData ?? const {},
          remoteVersion: _remoteVersion(config, remoteData),
        );
      }
      newVersion = current + 1;
    }

    final updateData = Map<String, dynamic>.from(body);
    if (versionField != null) {
      updateData[versionField] = newVersion;
    }
    transaction.update(entityRef, updateData);

    // update() merges top-level fields, so the acknowledged state is the
    // remote document with the payload fields (and version) applied.
    final merged = Map<String, dynamic>.from(remoteData ?? {})
      ..addAll(updateData);
    transaction.set(
      ledgerRef,
      _ledgerData(
        operation,
        remoteVersion: newVersion?.toString(),
        syncedData: merged,
      ),
    );
    return SyncSuccess(
      operationId: operation.operationId,
      remoteVersion: newVersion?.toString(),
      syncedData: merged,
    );
  }

  SyncResult _delete(
    Transaction transaction,
    SyncOperation operation,
    FirestoreEntityConfig config,
    DocumentReference<Map<String, dynamic>> entityRef,
    DocumentReference<Map<String, dynamic>> ledgerRef,
    bool exists,
    Map<String, dynamic>? remoteData,
    int? expectedVersion,
  ) {
    final versionField = config.versionField;
    if (exists && versionField != null && expectedVersion != null) {
      final current = remoteData?[versionField];
      if (current is! int || current != expectedVersion) {
        return SyncConflict(
          operationId: operation.operationId,
          remoteSnapshot: remoteData ?? const {},
          remoteVersion: _remoteVersion(config, remoteData),
        );
      }
    }

    if (exists) {
      transaction.delete(entityRef);
    }
    // The ledger entry is written even when the document was already
    // absent, so a retry replays this operation's success instead of
    // deleting a document recreated under a reused entity ID.
    transaction.set(ledgerRef, _ledgerData(operation));
    return SyncSuccess(operationId: operation.operationId);
  }

  // ------------------------------------------------------------------
  // Idempotency ledger
  // ------------------------------------------------------------------

  /// Deterministic, collision-safe ledger document ID: a SHA-256 hash of
  /// the stable `SyncOperation.idempotencyKey`.
  static String _ledgerId(String idempotencyKey) =>
      'op_${sha256.convert(utf8.encode(idempotencyKey))}';

  /// SHA-256 fingerprint of a canonical JSON encoding of [payload] —
  /// detects the same idempotency key reused with a different payload.
  static String _payloadFingerprint(Map<String, dynamic>? payload) =>
      sha256.convert(utf8.encode(_canonicalJson(payload))).toString();

  static String _canonicalJson(Object? value) {
    if (value is Map) {
      final keys = value.keys.map((key) => key.toString()).toList()..sort();
      final entries = keys.map(
        (key) => '${jsonEncode(key)}:${_canonicalJson(_valueFor(value, key))}',
      );
      return '{${entries.join(',')}}';
    }
    if (value is List) {
      return '[${value.map(_canonicalJson).join(',')}]';
    }
    return jsonEncode(value);
  }

  static Object? _valueFor(Map<dynamic, dynamic> map, String key) {
    for (final entry in map.entries) {
      if (entry.key.toString() == key) return entry.value;
    }
    return null;
  }

  Map<String, dynamic> _ledgerData(
    SyncOperation operation, {
    String? remoteVersion,
    Map<String, dynamic>? syncedData,
  }) =>
      {
        'idempotencyKey': operation.idempotencyKey,
        'operationId': operation.operationId,
        'entityType': operation.entityType,
        'entityId': operation.entityId,
        'operationType': operation.type.name,
        'payloadFingerprint': _payloadFingerprint(operation.payload),
        'status': 'applied',
        'remoteVersion': remoteVersion,
        'syncedData': syncedData,
        'appliedAt': FieldValue.serverTimestamp(),
      };

  /// Replays a committed ledger entry. The entry only counts as the same
  /// operation when every identity field and the payload fingerprint
  /// match; anything else is a key-reuse violation.
  SyncResult _ledgerResult(
    SyncOperation operation,
    Map<String, dynamic> ledger,
  ) {
    final identical = ledger['idempotencyKey'] == operation.idempotencyKey &&
        ledger['operationId'] == operation.operationId &&
        ledger['entityType'] == operation.entityType &&
        ledger['entityId'] == operation.entityId &&
        ledger['operationType'] == operation.type.name &&
        ledger['payloadFingerprint'] == _payloadFingerprint(operation.payload);
    if (!identical) {
      return SyncFailure(
        operationId: operation.operationId,
        error: SyncError(
          code: SyncErrorCode.validation,
          message: 'Idempotency key ${operation.idempotencyKey} was '
              'already used by a different operation.',
          isRetryable: false,
        ),
      );
    }
    final synced = ledger['syncedData'];
    return SyncSuccess(
      operationId: operation.operationId,
      remoteVersion: ledger['remoteVersion'] as String?,
      syncedData: synced is Map ? Map<String, dynamic>.from(synced) : null,
    );
  }

  // ------------------------------------------------------------------
  // Input validation
  // ------------------------------------------------------------------

  /// Firestore document IDs cannot contain `/`, be `.`/`..`, exceed
  /// 1500 bytes, or use the reserved `__.*__` pattern.
  static void _validateEntityId(String entityId) {
    if (entityId.isEmpty ||
        entityId == '.' ||
        entityId == '..' ||
        entityId.contains('/') ||
        utf8.encode(entityId).length > 1500 ||
        (entityId.startsWith('__') && entityId.endsWith('__'))) {
      throw FirestoreValidationException(
        'Invalid Firestore document ID: "$entityId".',
      );
    }
  }

  /// Returns a mutable copy of the payload after validation.
  ///
  /// - Create/update require a non-null payload.
  /// - The configured version field is adapter-managed and may not be
  ///   supplied by the caller.
  Map<String, dynamic> _writePayload(
    SyncOperation operation,
    FirestoreEntityConfig config,
  ) {
    final payload = operation.payload;
    if (operation.type != SyncOperationType.delete && payload == null) {
      throw FirestoreValidationException(
        'Payload is required for ${operation.type.name} operations.',
      );
    }
    final body = Map<String, dynamic>.from(payload ?? const {});

    final versionField = config.versionField;
    if (versionField != null && body.containsKey(versionField)) {
      throw FirestoreValidationException(
        'Payload may not contain the adapter-managed version field '
        '"$versionField".',
      );
    }
    return body;
  }

  /// Returns the expected version from operation metadata, strictly
  /// validated: only `int` values are accepted — strings, doubles, and
  /// null are rejected rather than coerced.
  int? _expectedVersion(
    SyncOperation operation,
    FirestoreEntityConfig config,
  ) {
    if (config.versionField == null) return null;
    final value = operation.metadata[expectedVersionKey];
    if (value == null) return null;
    if (value is! int) {
      throw FirestoreValidationException(
        'Expected version metadata "$expectedVersionKey" must be an int, '
        'got ${value.runtimeType}.',
      );
    }
    return value;
  }

  String? _remoteVersion(
    FirestoreEntityConfig config,
    Map<String, dynamic>? data,
  ) {
    final field = config.versionField;
    if (field == null || data == null) return null;
    final value = data[field];
    return value is int ? value.toString() : null;
  }
}
