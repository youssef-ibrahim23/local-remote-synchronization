import 'package:drift/drift.dart' hide isNotNull, isNull;
import 'package:drift/native.dart';
import 'package:local_remote_drift/local_remote_drift.dart';
import 'package:local_remote_drift/src/drift_outbox_writer.dart';
import 'package:local_remote_synchronization/local_remote_synchronization.dart';
import 'package:test/test.dart';
import 'package:uuid/uuid.dart';

final class _WriterTestDb extends GeneratedDatabase {
  _WriterTestDb(super.e);

  @override
  int get schemaVersion => 1;

  @override
  Iterable<TableInfo<Table, Object?>> get allTables =>
      const <TableInfo<Table, Object?>>[];

  @override
  MigrationStrategy get migration => MigrationStrategy(
        onCreate: (_) => DriftOutboxSchema.createSchema(this),
      );
}

void main() {
  group('DriftOutboxWriter', () {
    test('generates UUID v4 once and assigns monotonic sequenceNumber',
        () async {
      final db = _WriterTestDb(NativeDatabase.memory());
      addTearDown(db.close);

      const uuid = Uuid();
      final fixedNow = DateTime.utc(2026, 6, 1, 10);
      final occurredLocal = DateTime(2026, 5, 15, 8);
      final writer = DriftOutboxWriter(
        database: db,
        tableName: DriftOutboxSchema.defaultTableName,
        operationIdGenerator: uuid.v4,
        clock: () => fixedNow,
      );

      final op1 = await writer.enqueue(
        LocalMutation(
          entityType: 'tasks',
          entityId: 't-1',
          type: SyncOperationType.create,
          payload: const <String, dynamic>{'title': 'First'},
          occurredAt: occurredLocal,
        ),
      );
      final op2 = await writer.enqueue(
        LocalMutation(
          entityType: 'tasks',
          entityId: 't-2',
          type: SyncOperationType.delete,
          idempotencyKey: 'custom-key-2',
        ),
      );

      expect(Uuid.isValidUUID(fromString: op1.operationId), isTrue);
      expect(Uuid.isValidUUID(fromString: op2.operationId), isTrue);
      expect(op1.operationId, isNot(equals(op2.operationId)));
      expect(op1.idempotencyKey, op1.operationId);
      expect(op2.idempotencyKey, 'custom-key-2');
      expect(op1.sequenceNumber, 1);
      expect(op2.sequenceNumber, 2);
      expect(op1.createdAt, fixedNow);
      expect(op1.occurredAt, occurredLocal.toUtc());
      expect(op2.payload, isNull);
    });
  });
}
