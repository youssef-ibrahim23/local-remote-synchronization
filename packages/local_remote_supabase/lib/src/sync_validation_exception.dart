/// Internal signal for caller-payload validation failures detected by
/// `SupabaseRemoteSyncAdapter`. Mapped to a non-retryable
/// `SyncErrorCode.validation` `SyncFailure` by `SupabaseErrorMapper`.
final class SyncValidationException implements Exception {
  SyncValidationException(this.message);

  final String message;

  @override
  String toString() => 'SyncValidationException: $message';
}
