/// Hive box name validation for the local_remote_hive outbox.
final class HiveOutboxSchema {
  HiveOutboxSchema._();

  /// Default Hive box name used by `HiveLocalStorageAdapter`.
  static const String defaultBoxName = 'sync_outbox';

  /// Metadata box name for storing adapter-internal counters.
  static const String metadataBoxName = 'sync_outbox_meta';

  /// Key within [metadataBoxName] that stores the last assigned
  /// `sequenceNumber`.
  static const String sequenceCounterKey = 'sequence_counter';

  static final RegExp _boxNamePattern = RegExp(r'^[a-z][a-z0-9_]*$');

  /// Validates that [boxName] is a safe Hive box name.
  ///
  /// Hive box names must be lowercase alphanumeric with underscores and must
  /// start with a letter. This prevents injection and ensures compatibility
  /// across all Hive backends (including web/IndexedDB).
  static String validateBoxName(String boxName) {
    if (!_boxNamePattern.hasMatch(boxName)) {
      throw ArgumentError.value(
        boxName,
        'boxName',
        r'Invalid Hive box name. Must match ^[a-z][a-z0-9_]*$.',
      );
    }
    return boxName;
  }
}
