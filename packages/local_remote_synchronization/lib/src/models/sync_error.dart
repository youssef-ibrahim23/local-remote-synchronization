import 'package:meta/meta.dart';

import '../enums/sync_error_code.dart';

/// Structured, backend-agnostic error value for failed sync operations.
///
/// Persistable fields ([code], [message], [isRetryable], [retryAfter],
/// [statusCode]) are included in JSON serialization and value equality.
/// Diagnostic fields ([cause], [stackTrace]) are strictly in-memory and
/// excluded from both serialization and equality.
@immutable
final class SyncError {
  /// Creates an immutable [SyncError].
  SyncError({
    required this.code,
    required this.message,
    required this.isRetryable,
    this.retryAfter,
    this.statusCode,
    this.cause,
    this.stackTrace,
  }) {
    if (message.trim().isEmpty) {
      throw ArgumentError.value(message, 'message', 'Must not be empty.');
    }
    if (retryAfter != null && retryAfter!.isNegative) {
      throw ArgumentError.value(
        retryAfter,
        'retryAfter',
        'Must not be negative.',
      );
    }
  }

  /// Deserializes a [SyncError] from a JSON map.
  factory SyncError.fromJson(Map<String, dynamic> json) {
    final rawCode = json['code'];
    final rawMessage = json['message'];
    final rawIsRetryable = json['isRetryable'];
    final rawRetryAfterMs = json['retryAfterMs'];
    final rawStatusCode = json['statusCode'];

    if (rawCode is! String) {
      throw FormatException('Invalid or missing SyncError.code: $rawCode');
    }
    if (rawMessage is! String) {
      throw FormatException(
        'Invalid or missing SyncError.message: $rawMessage',
      );
    }
    if (rawIsRetryable is! bool) {
      throw FormatException(
        'Invalid or missing SyncError.isRetryable: $rawIsRetryable',
      );
    }
    if (rawRetryAfterMs != null && rawRetryAfterMs is! int) {
      throw FormatException(
        'Invalid SyncError.retryAfterMs: $rawRetryAfterMs',
      );
    }
    if (rawStatusCode != null && rawStatusCode is! int) {
      throw FormatException(
        'Invalid SyncError.statusCode: $rawStatusCode',
      );
    }

    final SyncErrorCode code;
    try {
      code = SyncErrorCode.values.byName(rawCode);
    } on ArgumentError {
      throw FormatException('Unknown SyncErrorCode: $rawCode');
    }

    return SyncError(
      code: code,
      message: rawMessage,
      isRetryable: rawIsRetryable,
      retryAfter: rawRetryAfterMs == null
          ? null
          : Duration(milliseconds: rawRetryAfterMs),
      statusCode: rawStatusCode,
    );
  }

  /// Classification of the error.
  final SyncErrorCode code;

  /// Human-readable error description.
  final String message;

  /// Whether the operation may be retried later.
  final bool isRetryable;

  /// Optional server-suggested delay before retrying (for example, Retry-After).
  final Duration? retryAfter;

  /// Optional transport status code for diagnostics (for example, HTTP status).
  final int? statusCode;

  /// In-memory original error or exception. Never serialized.
  final Object? cause;

  /// In-memory stack trace for debugging. Never serialized.
  final StackTrace? stackTrace;

  /// Serializes persistable error fields to a JSON map.
  Map<String, dynamic> toJson() => <String, dynamic>{
        'code': code.name,
        'message': message,
        'isRetryable': isRetryable,
        if (retryAfter != null) 'retryAfterMs': retryAfter!.inMilliseconds,
        if (statusCode != null) 'statusCode': statusCode,
      };

  @override
  bool operator ==(Object other) =>
      identical(this, other) ||
      other is SyncError &&
          runtimeType == other.runtimeType &&
          code == other.code &&
          message == other.message &&
          isRetryable == other.isRetryable &&
          retryAfter == other.retryAfter &&
          statusCode == other.statusCode;

  @override
  int get hashCode =>
      Object.hash(code, message, isRetryable, retryAfter, statusCode);
}
