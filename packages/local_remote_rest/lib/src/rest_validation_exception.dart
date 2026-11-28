/// Internal signal for caller-payload validation failures detected by
/// `RestRemoteSyncAdapter`. Mapped to a non-retryable
/// `SyncErrorCode.validation` `SyncFailure` by `RestErrorMapper`.
final class RestValidationException implements Exception {
  RestValidationException(this.message);

  final String message;

  @override
  String toString() => 'RestValidationException: $message';
}
