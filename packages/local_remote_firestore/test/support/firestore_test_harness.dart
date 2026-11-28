import 'package:fake_cloud_firestore/fake_cloud_firestore.dart';
import 'package:local_remote_firestore/local_remote_firestore.dart';
import 'package:local_remote_synchronization/local_remote_synchronization.dart';

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

/// Test harness bundling a [FakeFirebaseFirestore] instance and the
/// adapter under test.
final class FirestoreTestHarness {
  FirestoreTestHarness({
    required this.firestore,
    required this.adapter,
  });

  factory FirestoreTestHarness.create({
    Map<String, FirestoreEntityConfig>? entityMapping,
    String idempotencyCollectionPath = '_sync_test_operations',
    String expectedVersionKey = 'baseVersion',
  }) {
    final firestore = FakeFirebaseFirestore();
    final adapter = FirestoreRemoteSyncAdapter(
      firestore: firestore,
      entityMapping: entityMapping ??
          {
            'note': FirestoreEntityConfig(
              collectionPath: 'notes',
              versionField: 'sync_version',
            ),
            'todo': FirestoreEntityConfig(collectionPath: 'todos'),
          },
      idempotencyCollectionPath: idempotencyCollectionPath,
      expectedVersionKey: expectedVersionKey,
    );
    return FirestoreTestHarness(firestore: firestore, adapter: adapter);
  }

  final FakeFirebaseFirestore firestore;
  final FirestoreRemoteSyncAdapter adapter;
}
