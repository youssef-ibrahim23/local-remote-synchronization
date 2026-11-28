import 'package:flutter/material.dart';

import 'app/app_initializer.dart';
import 'state/todo_app_state.dart';
import 'ui/todo_list_screen.dart';

Future<void> main() async {
  WidgetsFlutterBinding.ensureInitialized();

  final initializer = AppInitializer();

  runApp(
    AppInitializerScope(
      initializer: initializer,
      child: _OfflineFirstApp(initializer: initializer),
    ),
  );
}

class _OfflineFirstApp extends StatefulWidget {
  const _OfflineFirstApp({required this.initializer});

  final AppInitializer initializer;

  @override
  State<_OfflineFirstApp> createState() => _OfflineFirstAppState();
}

class _OfflineFirstAppState extends State<_OfflineFirstApp> {
  _AppStartState _startState = _AppStartState.starting;
  String? _startError;
  TodoAppState? _appState;

  @override
  void initState() {
    super.initState();
    _start();
  }

  Future<void> _start() async {
    try {
      await widget.initializer.start();
      if (mounted) {
        setState(() {
          _appState = widget.initializer.appState;
          _startState = _AppStartState.ready;
        });
      }
    } catch (e) {
      if (mounted) {
        setState(() {
          _startError = e.toString();
          _startState = _AppStartState.error;
        });
      }
    }
  }

  @override
  void dispose() {
    // Dispose the initializer when the root widget is removed.
    // In a full app this would be called from app lifecycle callbacks.
    widget.initializer.dispose();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) => MaterialApp(
        title: 'Offline-First Todos',
        theme: ThemeData(
          colorSchemeSeed: Colors.indigo,
          useMaterial3: true,
        ),
        home: switch (_startState) {
          _AppStartState.starting => const Scaffold(
              body: Center(child: CircularProgressIndicator()),
            ),
          _AppStartState.error => Scaffold(
              body: Center(
                child: Padding(
                  padding: const EdgeInsets.all(24),
                  child: Column(
                    mainAxisSize: MainAxisSize.min,
                    children: [
                      const Icon(
                        Icons.error_outline,
                        size: 48,
                        color: Colors.red,
                      ),
                      const SizedBox(height: 16),
                      const Text(
                        'Failed to initialise the application.',
                        style: TextStyle(fontWeight: FontWeight.bold),
                      ),
                      const SizedBox(height: 8),
                      Text(
                        _startError ?? 'Unknown error',
                        textAlign: TextAlign.center,
                      ),
                    ],
                  ),
                ),
              ),
            ),
          _AppStartState.ready => TodoListScreen(appState: _appState!),
        },
      );
}

enum _AppStartState { starting, ready, error }
