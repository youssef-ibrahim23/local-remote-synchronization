import 'package:local_remote_rest/local_remote_rest.dart';
import 'package:local_remote_synchronization/local_remote_synchronization.dart';

import 'fake_http_client.dart';

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

/// Test harness bundling the fake HTTP client and the adapter under test.
final class RestTestHarness {
  RestTestHarness({
    required this.http,
    required this.adapter,
  });

  factory RestTestHarness.create({
    Map<String, RestEntityConfig>? entityMapping,
    Map<String, String> defaultHeaders = const {},
    String baseUrl = 'https://api.example.com/',
    Duration? requestTimeout,
    String idempotencyHeader = 'Idempotency-Key',
    String versionRequestHeader = 'If-Match',
    String versionResponseHeader = 'ETag',
    String expectedVersionKey = 'baseVersion',
  }) {
    final fake = FakeHttpClient();
    final adapter = RestRemoteSyncAdapter(
      baseUrl: baseUrl,
      entityMapping: entityMapping ??
          {
            'note': RestEntityConfig(collectionPath: 'notes'),
          },
      httpClient: fake,
      defaultHeaders: defaultHeaders,
      requestTimeout: requestTimeout ?? const Duration(seconds: 30),
      idempotencyHeader: idempotencyHeader,
      versionRequestHeader: versionRequestHeader,
      versionResponseHeader: versionResponseHeader,
      expectedVersionKey: expectedVersionKey,
    );
    return RestTestHarness(http: fake, adapter: adapter);
  }

  final FakeHttpClient http;
  final RestRemoteSyncAdapter adapter;

  void dispose() => adapter.close();
}
