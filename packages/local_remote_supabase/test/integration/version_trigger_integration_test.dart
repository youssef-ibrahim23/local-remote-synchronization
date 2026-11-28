import 'dart:io';

import 'package:flutter_test/flutter_test.dart';
import 'package:postgres/postgres.dart';

/// PostgreSQL integration tests for the version-increment trigger
/// documented in `local_remote_supabase`'s README.
///
/// These tests require a real PostgreSQL server and are **opt-in**: they
/// are skipped unless `SUPABASE_ADAPTER_TEST_DATABASE_URL` contains a
/// `postgresql://` connection string, e.g.:
///
/// ```bash
/// export SUPABASE_ADAPTER_TEST_DATABASE_URL="postgresql://user:pass@localhost:5432/postgres"
/// flutter test packages/local_remote_supabase
/// ```
///
/// The tests create a uniquely named table and trigger function, verify
/// the documented trigger semantics, and always drop them afterwards.
/// No Supabase project or production credentials are required.
void main() {
  final dsn = Platform.environment['SUPABASE_ADAPTER_TEST_DATABASE_URL'];
  final skipReason = dsn == null || dsn.isEmpty
      ? 'SUPABASE_ADAPTER_TEST_DATABASE_URL is not set — skipping '
          'PostgreSQL integration tests.'
      : false;

  group('PostgreSQL version trigger', skip: skipReason, () {
    Connection? connection;
    var table = '';
    var function = '';

    Future<int> versionOf(String id) async {
      final result = await connection!.execute(
        "select version from $table where id = '$id'",
      );
      expect(result, hasLength(1));
      return (result.single.toColumnMap()['version'] as num).toInt();
    }

    Future<Map<String, dynamic>> rowOf(String id) async {
      final result = await connection!.execute(
        "select * from $table where id = '$id'",
      );
      expect(result, hasLength(1));
      return result.single.toColumnMap();
    }

    setUpAll(() async {
      final suffix = DateTime.now().microsecondsSinceEpoch;
      table = 'notes_vtest_$suffix';
      function = 'bump_row_version_$suffix';
      connection = await Connection.openFromUrl(dsn!);

      // Mirrors the README trigger exactly: version increments only when
      // the row actually changes (OLD IS DISTINCT FROM NEW). Each
      // statement is executed separately because the postgres package
      // uses the Extended Query Protocol, which rejects multi-statement
      // strings.
      await connection!.execute('''
        create table $table (
          id text primary key,
          title text not null,
          _sync_operation_id text,
          version bigint not null default 1
        )
      ''');

      await connection!.execute('''
        create or replace function $function()
        returns trigger
        language plpgsql
        as \$\$
        begin
          if old is distinct from new then
            new.version := old.version + 1;
          end if;
          return new;
        end;
        \$\$
      ''');

      await connection!.execute('''
        create trigger ${table}_version_bump
          before update on $table
          for each row
          execute function $function()
      ''');
    });

    tearDownAll(() async {
      final conn = connection;
      if (conn == null) return;
      try {
        // Cleanup must not mask test results or throw
        // LateInitializationError when setup partially failed.
        await conn.execute('drop table if exists $table cascade');
      } catch (_) {
        // Table may not have been created; still try the function drop.
      }
      try {
        await conn.execute('drop function if exists $function()');
      } catch (_) {
        // Function may not have been created.
      }
      await conn.close();
    });

    test('A. actual update increments the version', () async {
      await connection!.execute(
        'insert into $table (id, title, _sync_operation_id) '
        "values ('a1', 'Hello', 'idem-a1')",
      );
      expect(await versionOf('a1'), 1);

      await connection!.execute(
        "update $table set title = 'Updated' where id = 'a1'",
      );

      expect(await versionOf('a1'), 2);
      expect((await rowOf('a1'))['title'], 'Updated');
    });

    test('B. no-op update does not increment the version', () async {
      await connection!.execute(
        'insert into $table (id, title, _sync_operation_id, version) '
        "values ('b1', 'Same', 'idem-b1', 2)",
      );
      expect(await versionOf('b1'), 2);

      // UPDATE that changes nothing — the trigger's IS DISTINCT FROM
      // guard must keep the version at 2.
      await connection!.execute(
        "update $table set title = title where id = 'b1'",
      );

      expect(await versionOf('b1'), 2);
    });

    test('C. conditional update rejects a stale version', () async {
      await connection!.execute(
        'insert into $table (id, title, _sync_operation_id, version) '
        "values ('c1', 'Original', 'idem-c1', 2)",
      );

      // Conditional PATCH equivalent: id matches but version = 1 is stale.
      final updated = await connection!.execute(
        "update $table set title = 'Stale write' "
        "where id = 'c1' and version = 1",
      );
      expect(updated.affectedRows, 0);

      final row = await rowOf('c1');
      expect(row['title'], 'Original');
      expect((row['version'] as num).toInt(), 2);
    });
  });
}
