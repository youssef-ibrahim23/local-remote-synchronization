/// Domain model for a Todo item — owned by the application, not by the sync
/// packages.
///
/// The sync packages know nothing about [Todo]. They only see the
/// JSON-compatible payload supplied through `LocalMutation`.
final class Todo {
  const Todo({
    required this.id,
    required this.title,
    this.completed = false,
  });

  final String id;
  final String title;
  final bool completed;

  /// Serialises the [Todo] to a JSON-compatible map suitable as a
  /// `LocalMutation` payload.
  Map<String, dynamic> toPayload() => <String, dynamic>{
        'id': id,
        'title': title,
        'completed': completed,
      };

  Todo copyWith({String? title, bool? completed}) => Todo(
        id: id,
        title: title ?? this.title,
        completed: completed ?? this.completed,
      );

  @override
  bool operator ==(Object other) =>
      identical(this, other) ||
      other is Todo &&
          runtimeType == other.runtimeType &&
          id == other.id &&
          title == other.title &&
          completed == other.completed;

  @override
  int get hashCode => Object.hash(id, title, completed);
}
