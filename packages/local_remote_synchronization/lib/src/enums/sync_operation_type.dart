/// The kind of mutation performed on a local entity.
enum SyncOperationType {
  /// A new entity was created locally.
  create,

  /// An existing entity was updated locally.
  update,

  /// An entity was deleted locally.
  delete,
}
