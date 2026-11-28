import 'package:local_remote_supabase/local_remote_supabase.dart';
import 'package:local_remote_synchronization/local_remote_synchronization.dart';
import 'package:supabase_flutter/supabase_flutter.dart';

import 'fake_supabase_http.dart';

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

/// Test harness bundling the fake HTTP client, the real [SupabaseClient]
/// wired to it, and the adapter under test.
final class SupabaseTestHarness {
  SupabaseTestHarness({
    required this.http,
    required this.client,
    required this.adapter,
  });

  factory SupabaseTestHarness.create({
    Map<String, SupabaseTableConfig>? tableMapping,
  }) {
    final http = FakeSupabaseHttpClient();
    final client = SupabaseClient(
      'https://test.supabase.co',
      'anon-key',
      httpClient: http,
      accessToken: () async => 'test-access-token',
      postgrestOptions: const PostgrestClientOptions(retryEnabled: false),
    );
    final adapter = SupabaseRemoteSyncAdapter(
      client: client,
      tableMapping: tableMapping ??
          {
            'note': SupabaseTableConfig(tableName: 'notes'),
          },
    );
    return SupabaseTestHarness(http: http, client: client, adapter: adapter);
  }

  final FakeSupabaseHttpClient http;
  final SupabaseClient client;
  final SupabaseRemoteSyncAdapter adapter;

  void dispose() {
    client.dispose();
  }
}
