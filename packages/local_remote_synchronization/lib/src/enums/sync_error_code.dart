/// Backend-agnostic classification of synchronization errors.
enum SyncErrorCode {
  /// Network connectivity or DNS/socket failure.
  network,

  /// Operation exceeded the configured timeout.
  timeout,

  /// Authentication credentials are missing, expired, or invalid.
  unauthorized,

  /// Authenticated caller lacks permission for the target operation.
  forbidden,

  /// Target remote resource was not found.
  notFound,

  /// Remote rejected the operation due to a version or state conflict.
  conflict,

  /// Remote rejected the payload due to schema or business validation rules.
  validation,

  /// Remote throttled the request (e.g., HTTP 429).
  rateLimited,

  /// Remote experienced an internal or transient server failure (e.g., 5xx).
  serverError,

  /// Failed to serialize or deserialize operation data.
  serialization,

  /// Local storage or outbox persistence failure.
  storage,

  /// Operation was cancelled before completion.
  cancelled,

  /// Unclassified error.
  unknown,
}
