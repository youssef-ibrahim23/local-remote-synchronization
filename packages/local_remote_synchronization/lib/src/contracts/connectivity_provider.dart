/// Reports network reachability for synchronization orchestration.
abstract interface class ConnectivityProvider {
  /// Returns `true` when the device currently has network connectivity.
  Future<bool> get isConnected;

  /// Emits connectivity transitions (`true` when online, `false` when offline).
  Stream<bool> get onConnectivityChanged;
}
