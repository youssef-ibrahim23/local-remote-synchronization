import 'dart:async';
import 'dart:collection';
import 'dart:convert';

import 'package:http/http.dart' as http;

/// A recorded HTTP request captured by [FakeHttpClient].
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

  Map<String, dynamic>? get jsonBody =>
      body.isEmpty ? null : jsonDecode(body) as Map<String, dynamic>?;
}

/// Deterministic [http.Client] test double.
///
/// Responses (or thrown errors) are queued with [enqueue] / [enqueueError]
/// and consumed in FIFO order. Every request is recorded in [requests].
final class FakeHttpClient extends http.BaseClient {
  final List<RecordedRequest> requests = [];
  final Queue<_QueuedResponse> _queue = Queue<_QueuedResponse>();

  /// When set, [send] awaits this future before responding — used to
  /// exercise adapter request timeouts deterministically.
  Future<void>? pending;

  /// Whether [close] was called on this client.
  bool get isClosed => _isClosed;
  bool _isClosed = false;

  @override
  void close() {
    _isClosed = true;
    super.close();
  }

  void enqueue(
    int statusCode, {
    String body = '',
    Map<String, String> headers = const {},
  }) {
    _queue.add(_QueuedResponse.status(statusCode, body, headers));
  }

  void enqueueJson(
    int statusCode,
    Object? body, {
    Map<String, String> headers = const {},
  }) {
    enqueue(
      statusCode,
      body: jsonEncode(body),
      headers: {'content-type': 'application/json', ...headers},
    );
  }

  void enqueueError(Object error) {
    _queue.add(_QueuedResponse.error(error));
  }

  /// Queues a response whose headers arrive immediately but whose body
  /// stream never emits or completes — used to verify the adapter's
  /// request timeout covers body consumption.
  void enqueuePendingBody(
    int statusCode, {
    Map<String, String> headers = const {},
  }) {
    _queue.add(
      _QueuedResponse.streamed(
        statusCode,
        StreamController<List<int>>().stream,
        headers,
      ),
    );
  }

  /// Queues a response with a custom body stream.
  void enqueueStreamed(
    int statusCode,
    Stream<List<int>> body, {
    Map<String, String> headers = const {},
  }) {
    _queue.add(_QueuedResponse.streamed(statusCode, body, headers));
  }

  @override
  Future<http.StreamedResponse> send(http.BaseRequest request) async {
    final bodyBytes = await request.finalize().toBytes();
    requests.add(
      RecordedRequest(
        method: request.method,
        url: request.url,
        // Header names are case-insensitive on the wire; normalize so
        // tests can look them up in lowercase.
        headers: {
          for (final entry in request.headers.entries)
            entry.key.toLowerCase(): entry.value,
        },
        body: utf8.decode(bodyBytes),
      ),
    );

    final pending = this.pending;
    if (pending != null) await pending;

    if (_queue.isEmpty) {
      throw StateError(
        'FakeHttpClient: no queued response for ${request.method} '
        '${request.url}.',
      );
    }
    final queued = _queue.removeFirst();
    final queuedError = queued.error;
    if (queuedError != null) {
      if (queuedError is Exception) throw queuedError;
      if (queuedError is Error) throw queuedError;
      throw ArgumentError.value(
        queuedError,
        'error',
        'Must be an Exception or Error.',
      );
    }

    return http.StreamedResponse(
      queued.stream ?? Stream.value(utf8.encode(queued.body)),
      queued.statusCode,
      headers: queued.headers,
      reasonPhrase: queued.statusCode == 204 ? 'No Content' : null,
    );
  }
}

final class _QueuedResponse {
  _QueuedResponse.status(this.statusCode, this.body, this.headers)
      : error = null,
        stream = null;
  _QueuedResponse.error(this.error)
      : statusCode = 0,
        body = '',
        headers = const {},
        stream = null;
  _QueuedResponse.streamed(this.statusCode, this.stream, this.headers)
      : error = null,
        body = '';

  final int statusCode;
  final String body;
  final Map<String, String> headers;
  final Object? error;
  final Stream<List<int>>? stream;
}
