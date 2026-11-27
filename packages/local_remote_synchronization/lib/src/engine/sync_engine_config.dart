/// Immutable configuration for `SyncEngine`.
final class SyncEngineConfig {
  /// Creates a [SyncEngineConfig] with the given values.
  ///
  /// Throws [ArgumentError] if [batchSize] or [maxConcurrency] is not
  /// positive.
  SyncEngineConfig({
    this.batchSize = 50,
    this.maxConcurrency = 5,
    this.syncOnStart = true,
    this.syncOnConnectivityRestore = true,
    this.syncWhenOffline = false,
  }) {
    if (batchSize <= 0) {
      throw ArgumentError.value(batchSize, 'batchSize', 'Must be positive.');
    }
    if (maxConcurrency <= 0) {
      throw ArgumentError.value(
        maxConcurrency,
        'maxConcurrency',
        'Must be positive.',
      );
    }
  }

  /// Maximum number of operations fetched per batch.
  /// Default: 50. Must be > 0.
  final int batchSize;

  /// Maximum number of concurrent pushes within a batch.
  /// Default: 5. Must be > 0.
  final int maxConcurrency;

  /// Whether to trigger a sync cycle automatically on
  /// `SyncEngine.start` when the device is online.
  /// Default: true.
  final bool syncOnStart;

  /// Whether to trigger a sync cycle when connectivity transitions from
  /// offline to online.
  /// Default: true.
  final bool syncOnConnectivityRestore;

  /// Whether `SyncEngine.syncNow` should attempt a sync even when the
  /// device is offline. When `false` (default), offline manual syncs
  /// return a skipped report.
  /// Default: false.
  final bool syncWhenOffline;
}
