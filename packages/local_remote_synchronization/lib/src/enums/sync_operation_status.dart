/// Lifecycle status of a durable [SyncOperation] in the outbox.
enum SyncOperationStatus {
  /// Queued and waiting to be picked up for synchronization.
  pending,

  /// Currently claimed by a sync worker; reset to [pending] on recovery.
  inFlight,

  /// Failed at least once and scheduled for retry at `nextRetryAt`.
  failed,

  /// Exhausted retries or encountered a non-retryable error; excluded from
  /// automatic synchronization.
  deadLetter,
}
