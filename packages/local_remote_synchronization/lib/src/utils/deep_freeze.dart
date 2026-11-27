/// Recursively clones and freezes a JSON-compatible map so neither the caller
/// nor any consumer can mutate the map or any nested maps/lists.
Map<String, dynamic> deepFreezeMap(
  Map<String, dynamic> source, {
  String fieldName = 'payload',
}) {
  final copy = <String, dynamic>{};
  for (final entry in source.entries) {
    copy[entry.key] = deepFreezeJsonValue(
      entry.value,
      path: '$fieldName.${entry.key}',
    );
  }
  return Map<String, dynamic>.unmodifiable(copy);
}

/// Recursively validates and freezes a JSON-compatible value.
///
/// Supported types: `Map<String, dynamic>` (or `Map` with `String` keys),
/// `List`, `String`, `num`, `bool`, and `null`.
Object? deepFreezeJsonValue(Object? value, {String path = 'value'}) {
  if (value == null || value is String || value is int || value is bool) {
    return value;
  }

  if (value is double) {
    if (!value.isFinite) {
      throw ArgumentError.value(
        value,
        path,
        'Non-finite double values (NaN, Infinity, -Infinity) are not valid '
        'JSON numbers.',
      );
    }
    return value;
  }

  if (value is Map) {
    final copy = <String, dynamic>{};
    for (final entry in value.entries) {
      final key = entry.key;
      if (key is! String) {
        throw ArgumentError.value(
          key,
          path,
          'Map keys must be String, got ${key.runtimeType}.',
        );
      }
      copy[key] = deepFreezeJsonValue(entry.value, path: '$path.$key');
    }
    return Map<String, dynamic>.unmodifiable(copy);
  }

  if (value is List) {
    final copy = <Object?>[
      for (var i = 0; i < value.length; i++)
        deepFreezeJsonValue(value[i], path: '$path[$i]'),
    ];
    return List<Object?>.unmodifiable(copy);
  }

  throw ArgumentError.value(
    value,
    path,
    'Unsupported JSON value type ${value.runtimeType}. '
    'Allowed: Map, List, String, num, bool, null.',
  );
}
