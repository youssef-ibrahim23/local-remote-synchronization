import 'package:connectivity_plus/connectivity_plus.dart';
import 'package:local_remote_synchronization/local_remote_synchronization.dart';

/// [ConnectivityProvider] implementation backed by the `connectivity_plus`
/// package.
///
/// This adapter lives in the application layer. The Core and sync adapter
/// packages never depend on `connectivity_plus` or on Flutter.
final class ConnectivityPlusProvider implements ConnectivityProvider {
  ConnectivityPlusProvider([Connectivity? connectivity])
      : _connectivity = connectivity ?? Connectivity();

  final Connectivity _connectivity;

  @override
  Future<bool> get isConnected async {
    final results = await _connectivity.checkConnectivity();
    return _isOnline(results);
  }

  @override
  Stream<bool> get onConnectivityChanged =>
      _connectivity.onConnectivityChanged.map(_isOnline);

  static bool _isOnline(List<ConnectivityResult> results) =>
      results.any((r) => r != ConnectivityResult.none);
}
