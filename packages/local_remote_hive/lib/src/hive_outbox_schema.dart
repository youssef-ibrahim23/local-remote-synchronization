/// Hive box name validation and outbox snapshot constants for the
/// local_remote_hive adapter.
final class HiveOutboxSchema {
  HiveOutboxSchema._();

  /// Default Hive box name used by `HiveLocalStorageAdapter`.
  static const String defaultBoxName = 'sync_outbox';

  /// Key within the outbox box that stores the serialized snapshot.
  static const String snapshotKey = 'outbox_snapshot';

  /// Current snapshot schema version.
  static const int currentVersion = 2;

  /// Minimum snapshot schema version supported by this adapter.
  static const int minSupportedVersion = 1;

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
