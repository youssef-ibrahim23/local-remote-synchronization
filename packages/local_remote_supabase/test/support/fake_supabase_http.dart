import 'dart:async';
import 'dart:collection';
import 'dart:convert';

import 'package:http/http.dart' as http;

/// A recorded HTTP request sent through [FakeSupabaseHttpClient].
final class RecordedRequest {
  RecordedRequest({
    required this.method,
    required this.url,
    required this.headers,
    required this.body,
  });

  final String method;
  final Uri url;
  final Map<String, String> headers;
  final String body;

  Map<String, dynamic>? get jsonBody {
    if (body.isEmpty) return null;
    final decoded = jsonDecode(body);
    return decoded is Map<String, dynamic> ? decoded : null;
  }

  List<dynamic>? get jsonListBody {
    if (body.isEmpty) return null;
    final decoded = jsonDecode(body);
    return decoded is List ? decoded : null;
  }
}

typedef FakeRequestHandler = FutureOr<http.Response> Function(
  RecordedRequest request,
);

/// Deterministic `package:http` client that replays scripted responses and
/// records every request. Injected into `SupabaseClient(httpClient:)`.
final class FakeSupabaseHttpClient extends http.BaseClient {
  final List<RecordedRequest> requests = <RecordedRequest>[];
  final Queue<FakeRequestHandler> _handlers = Queue<FakeRequestHandler>();

  /// Queues a response with an optional delay (for ordering tests).
  void enqueue(http.Response response, {Duration? delay}) {
    _handlers.add((request) async {
      if (delay != null) await Future<void>.delayed(delay);
      return response;
    });
  }

  void enqueueJson(int statusCode, Object? body,
      {Map<String, String>? headers}) {
    enqueue(
      http.Response(
        jsonEncode(body),
        statusCode,
        headers: {
          'content-type': 'application/json; charset=utf-8',
          ...?headers,
        },
      ),
    );
  }

  /// Queues a PostgREST-style JSON error body.
  void enqueuePostgrestError({
    required int statusCode,
    required String code,
    required String message,
    Object? details,
    String? hint,
  }) {
    enqueueJson(statusCode, {
      'code': code,
      'message': message,
      'details': details,
      'hint': hint,
    });
  }

  /// Queues a non-JSON error body (e.g. a gateway HTML page).
  void enqueueRawError(int statusCode, String body) {
    enqueue(
      http.Response(
        body,
        statusCode,
        headers: {'content-type': 'text/html'},
      ),
    );
  }

  /// Queues a thrown transport-level error.
  void enqueueThrow(Exception error) {
    _handlers.add((request) => throw error);
  }

  /// Queues a custom handler for the next request.
  void enqueueHandler(FakeRequestHandler handler) {
    _handlers.add(handler);
  }

  @override
  Future<http.StreamedResponse> send(http.BaseRequest request) async {
    final bytes = await request.finalize().toBytes();
    final recorded = RecordedRequest(
      method: request.method,
      url: request.url,
      headers: Map<String, String>.of(request.headers),
      body: utf8.decode(bytes, allowMalformed: true),
    );
    requests.add(recorded);

    if (_handlers.isEmpty) {
      return _stream(
        http.Response(
          '{"message":"FakeSupabaseHttpClient: no scripted response"}',
          500,
          headers: {'content-type': 'application/json'},
          request: request,
        ),
        request,
      );
    }

    final handler = _handlers.removeFirst();
    final response = await handler(recorded);
    return _stream(response, request);
  }

  http.StreamedResponse _stream(
    http.Response response,
    http.BaseRequest request,
  ) =>
      http.StreamedResponse(
        Stream<List<int>>.value(utf8.encode(response.body)),
        response.statusCode,
        contentLength: utf8.encode(response.body).length,
        request: request,
        headers: response.headers,
        reasonPhrase: response.reasonPhrase,
        isRedirect: response.isRedirect,
        persistentConnection: response.persistentConnection,
      );
}
