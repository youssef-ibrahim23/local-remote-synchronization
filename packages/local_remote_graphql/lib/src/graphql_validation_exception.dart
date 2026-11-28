/// Internal signal for invalid adapter input discovered before or during
/// mutation execution.
final class GraphQLValidationException implements Exception {
  /// Creates a validation exception with a descriptive [message].
  const GraphQLValidationException(this.message);

  /// Human-readable description of the invalid input.
  final String message;

  @override
  String toString() => 'GraphQLValidationException: $message';
}
