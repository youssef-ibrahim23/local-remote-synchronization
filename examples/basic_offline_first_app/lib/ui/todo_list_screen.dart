import 'package:flutter/material.dart';
import 'package:uuid/uuid.dart';

import '../domain/todo.dart';
import '../state/todo_app_state.dart';

/// Minimal UI for the Offline-First Todo reference application.
///
/// The UI reads exclusively from [TodoAppState], which is backed by local
/// SQLite storage. No remote data is fetched directly by any widget.
class TodoListScreen extends StatefulWidget {
  const TodoListScreen({required this.appState, super.key});

  final TodoAppState appState;

  @override
  State<TodoListScreen> createState() => _TodoListScreenState();
}

class _TodoListScreenState extends State<TodoListScreen> {
  static const _uuid = Uuid();

  @override
  void initState() {
    super.initState();
    widget.appState.addListener(_onStateChanged);
  }

  @override
  void dispose() {
    widget.appState.removeListener(_onStateChanged);
    super.dispose();
  }

  void _onStateChanged() {
    if (mounted) setState(() {});
  }

  Future<void> _addTodo() async {
    final title = await showDialog<String>(
      context: context,
      builder: (ctx) => _AddTodoDialog(),
    );
    if (title == null || title.trim().isEmpty) return;
    try {
      await widget.appState.createTodo(title, _uuid.v4());
    } catch (e) {
      _showError('Failed to create todo: $e');
    }
  }

  Future<void> _deleteTodo(Todo todo) async {
    try {
      await widget.appState.deleteTodo(todo);
    } catch (e) {
      _showError('Failed to delete todo: $e');
    }
  }

  Future<void> _toggleTodo(Todo todo) async {
    try {
      await widget.appState.toggleTodo(todo);
    } catch (e) {
      _showError('Failed to update todo: $e');
    }
  }

  Future<void> _syncNow() async {
    try {
      await widget.appState.syncNow();
    } catch (e) {
      _showError('Sync failed: $e');
    }
  }

  void _showError(String message) {
    if (!mounted) return;
    ScaffoldMessenger.of(context).showSnackBar(
      SnackBar(
        content: Text(message),
        backgroundColor: Colors.red.shade700,
      ),
    );
  }

  @override
  Widget build(BuildContext context) {
    final state = widget.appState;

    return Scaffold(
      appBar: AppBar(
        title: const Text('Offline-First Todos'),
        actions: [
          IconButton(
            tooltip: 'Sync now',
            onPressed: state.isSyncing ? null : _syncNow,
            icon: state.isSyncing
                ? const SizedBox(
                    width: 20,
                    height: 20,
                    child: CircularProgressIndicator(strokeWidth: 2),
                  )
                : const Icon(Icons.sync),
          ),
        ],
      ),
      body: Column(
        children: [
          _SyncStatusBar(state: state),
          Expanded(
            child: state.todos.isEmpty
                ? const Center(
                    child: Text(
                      'No todos yet.\nTap + to create one.',
                      textAlign: TextAlign.center,
                    ),
                  )
                : ListView.builder(
                    itemCount: state.todos.length,
                    itemBuilder: (ctx, i) {
                      final todo = state.todos[i];
                      return _TodoTile(
                        todo: todo,
                        onToggle: () => _toggleTodo(todo),
                        onDelete: () => _deleteTodo(todo),
                      );
                    },
                  ),
          ),
        ],
      ),
      floatingActionButton: FloatingActionButton(
        onPressed: _addTodo,
        tooltip: 'Add Todo',
        child: const Icon(Icons.add),
      ),
    );
  }
}

/// Compact sync status bar displayed below the AppBar.
class _SyncStatusBar extends StatelessWidget {
  const _SyncStatusBar({required this.state});

  final TodoAppState state;

  @override
  Widget build(BuildContext context) {
    final theme = Theme.of(context);
    final error = state.lastSyncError;
    final lastAt = state.lastSyncAt;

    return Container(
      color: theme.colorScheme.surfaceContainerHighest,
      padding: const EdgeInsets.symmetric(horizontal: 16, vertical: 6),
      child: Row(
        children: [
          Expanded(
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                Row(
                  children: [
                    Icon(
                      state.pendingCount > 0
                          ? Icons.cloud_upload_outlined
                          : Icons.cloud_done_outlined,
                      size: 16,
                      color: theme.colorScheme.onSurfaceVariant,
                    ),
                    const SizedBox(width: 6),
                    Text(
                      state.pendingCount > 0
                          ? '${state.pendingCount} operation(s) pending sync'
                          : 'Outbox empty',
                      style: theme.textTheme.bodySmall,
                    ),
                  ],
                ),
                if (error != null)
                  Padding(
                    padding: const EdgeInsets.only(top: 2),
                    child: Text(
                      error,
                      style: theme.textTheme.bodySmall?.copyWith(
                        color: theme.colorScheme.error,
                      ),
                      maxLines: 2,
                      overflow: TextOverflow.ellipsis,
                    ),
                  ),
                if (lastAt != null && error == null)
                  Padding(
                    padding: const EdgeInsets.only(top: 2),
                    child: Text(
                      'Last sync: ${_formatTime(lastAt)} '
                      '(✓ ${state.lastSyncSucceeded}  '
                      '✗ ${state.lastSyncFailed}  '
                      '☠ ${state.lastSyncDeadLettered})',
                      style: theme.textTheme.bodySmall?.copyWith(
                        color: theme.colorScheme.onSurfaceVariant,
                      ),
                    ),
                  ),
              ],
            ),
          ),
        ],
      ),
    );
  }

  String _formatTime(DateTime dt) {
    final local = dt.toLocal();
    final h = local.hour.toString().padLeft(2, '0');
    final m = local.minute.toString().padLeft(2, '0');
    final s = local.second.toString().padLeft(2, '0');
    return '$h:$m:$s';
  }
}

/// A single todo row.
class _TodoTile extends StatelessWidget {
  const _TodoTile({
    required this.todo,
    required this.onToggle,
    required this.onDelete,
  });

  final Todo todo;
  final VoidCallback onToggle;
  final VoidCallback onDelete;

  @override
  Widget build(BuildContext context) => ListTile(
        leading: Checkbox(
          value: todo.completed,
          onChanged: (_) => onToggle(),
        ),
        title: Text(
          todo.title,
          style: todo.completed
              ? const TextStyle(decoration: TextDecoration.lineThrough)
              : null,
        ),
        trailing: IconButton(
          icon: const Icon(Icons.delete_outline),
          onPressed: onDelete,
          tooltip: 'Delete',
        ),
      );
}

/// Simple dialog for entering a new todo title.
class _AddTodoDialog extends StatefulWidget {
  @override
  State<_AddTodoDialog> createState() => _AddTodoDialogState();
}

class _AddTodoDialogState extends State<_AddTodoDialog> {
  final _controller = TextEditingController();

  @override
  void dispose() {
    _controller.dispose();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) => AlertDialog(
        title: const Text('New Todo'),
        content: TextField(
          controller: _controller,
          autofocus: true,
          decoration: const InputDecoration(labelText: 'Title'),
          textCapitalization: TextCapitalization.sentences,
          onSubmitted: (_) => _submit(context),
        ),
        actions: [
          TextButton(
            onPressed: () => Navigator.of(context).pop(),
            child: const Text('Cancel'),
          ),
          FilledButton(
            onPressed: () => _submit(context),
            child: const Text('Add'),
          ),
        ],
      );

  void _submit(BuildContext context) {
    final text = _controller.text.trim();
    Navigator.of(context).pop(text.isEmpty ? null : text);
  }
}
