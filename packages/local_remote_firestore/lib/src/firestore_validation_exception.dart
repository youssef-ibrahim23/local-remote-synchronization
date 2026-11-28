/// Internal signal for caller-input validation failures detected by
/// `FirestoreRemoteSyncAdapter` before any Firestore access. Mapped to a
/// non-retryable `SyncErrorCode.validation` `SyncFailure` by
/// `FirestoreErrorMapper`.
final class FirestoreValidationException implements Exception {
  FirestoreValidationException(this.message);

  final String message;

  @override
  String toString() => 'FirestoreValidationException: $message';
}
