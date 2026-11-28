import 'package:http/http.dart' as http;
import 'package:local_remote_rest/local_remote_rest.dart';

/// REST endpoint configuration for the Todo entity type.
///
/// The [RestRemoteSyncAdapter] uses this to map each `SyncOperation` with
/// `entityType == 'todos'` to the correct HTTP method and URL path.
///
/// Set [todoRestBaseUrl] via an environment variable or compile-time
/// constant before shipping to a real environment. The example defaults to
/// `http://localhost:8080` so the UI and local persistence work without a
/// real backend.
///
/// ## Backend requirements
///
/// A compatible backend must expose:
///
/// | Operation | Method | Path              |
/// |-----------|--------|-------------------|
/// | create    | POST   | `<baseUrl>/todos` |
/// | update    | PATCH  | `<baseUrl>/todos/{id}` |
/// | delete    | DELETE | `<baseUrl>/todos/{id}` |
///
/// Every mutating request includes an `Idempotency-Key` header containing
/// the stable `SyncOperation.idempotencyKey`. The backend **must** persist
/// this key and deduplicate repeat requests with the same key to achieve
/// effective exactly-once delivery. The adapter alone does **not** guarantee
/// exactly-once — it is at-least-once by design.
///
/// Optional optimistic concurrency: include a `baseVersion` field in
/// `SyncOperation.metadata`, and the adapter will send `If-Match: <version>`
/// with each request. The backend should return `412` or `409` on conflicts.
///
/// No real credentials or production URLs are stored here. Supply auth tokens
/// through `RestRemoteSyncAdapter.defaultHeaders` in [buildRestAdapter].

/// Base URL for the REST backend. Override at app startup or via a build
/// environment variable (e.g., `--dart-define=TODO_API_URL=https://api.example.com`).
// ignore: do_not_use_environment
const String todoRestBaseUrl = String.fromEnvironment(
  'TODO_API_URL',
  defaultValue: 'http://localhost:8080',
);

/// Builds the [RestRemoteSyncAdapter] wired to the Todo entity.
///
/// [baseUrl] defaults to [todoRestBaseUrl] and can be overridden in tests.
/// [defaultHeaders] is for auth tokens — never store real secrets here.
/// When [httpClient] is omitted, the adapter creates and owns its own client
/// (which is closed when [RestRemoteSyncAdapter.close] is called). When
/// [httpClient] is provided, the caller retains ownership of it.
RestRemoteSyncAdapter buildRestAdapter({
  String baseUrl = todoRestBaseUrl,
  Map<String, String> defaultHeaders = const {},
  http.Client? httpClient,
}) =>
    RestRemoteSyncAdapter(
      baseUrl: baseUrl,
      entityMapping: {
        'todos': RestEntityConfig(
          collectionPath: 'todos',
          // itemPathTemplate defaults to 'todos/{id}'
          idField: 'id', // adapter injects entityId into the payload body
        ),
      },
      httpClient: httpClient,
      defaultHeaders: defaultHeaders,
    );
