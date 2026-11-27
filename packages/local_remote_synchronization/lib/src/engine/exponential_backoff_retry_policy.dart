import 'dart:math' as math;

import '../models/sync_error.dart';
import 'retry_policy.dart';

/// Default [RetryPolicy] using exponential backoff with jitter.
///
/// Delay formula:
///   1. `base = min(initialDelay * 2^(attemptCount - 1), maxDelay)`
///      — the local backoff cap is [maxDelay]. This cap applies to local
///      backoff only, NOT to server-provided [SyncError.retryAfter].
///   2. `jittered = base * (1 - jitterFactor + random() * 2 * jitterFactor)`
///      — with `jitterFactor = 1.0` the lower bound is 0, which could
///      produce a zero delay. To prevent tight retry loops, the result
///      is clamped to a strictly positive minimum that never exceeds
///      [maxDelay]: `min(max(jittered, 1ms), maxDelay)`.
///   3. `finalDelay = max(jittered, error.retryAfter ?? zero)` —
///      `retryAfter` is a minimum and is never shortened by local
///      backoff or jitter. A `retryAfter` larger than [maxDelay] is
///      preserved because [maxDelay] caps local backoff only.
///
/// Example with `initialDelay = 1s`, `maxDelay = 5m`,
/// `jitterFactor = 0.2`:
///   attempt 1 → ~1s   (0.8s – 1.2s)
///   attempt 2 → ~2s   (1.6s – 2.4s)
///   attempt 3 → ~4s   (3.2s – 4.8s)
///   attempt 4 → ~8s   (6.4s – 9.6s)
///   ...
///   attempt 10 → ~5m  (4m – 5m, jittered then capped at maxDelay=5m)
///
/// If `error.retryAfter = 10m` (> maxDelay=5m): returns 10m — the
/// server suggestion is preserved, not capped by maxDelay.
final class ExponentialBackoffRetryPolicy implements RetryPolicy {
  /// Creates an [ExponentialBackoffRetryPolicy].
  ///
  /// [initialDelay] — delay before the first retry. Default: 1 second.
  ///   Must be > 0.
  /// [maxDelay] — maximum delay for **local backoff** between retries.
  ///   Default: 5 minutes. Must be >= [initialDelay]. This does NOT cap
  ///   server-provided [SyncError.retryAfter].
  /// [maxAttempts] — maximum push attempts before dead-lettering.
  ///   Default: 10. Must be > 0.
  /// [jitterFactor] — fractional jitter applied to each delay.
  ///   `0.0` = no jitter, `0.2` = ±20%. Default: 0.2.
  ///   Must be in `[0.0, 1.0]`.
  /// [random] — injectable RNG for jitter. Defaults to
  ///   [math.Random.secure].
  ///   Only this class owns a [Random]; `SyncEngine` does not accept
  ///   one.
  ///
  /// Throws [ArgumentError] if any argument violates the constraints
  /// above.
  ExponentialBackoffRetryPolicy({
    Duration initialDelay = const Duration(seconds: 1),
    Duration maxDelay = const Duration(minutes: 5),
    this.maxAttempts = 10,
    this.jitterFactor = 0.2,
    math.Random? random,
  })  : _initialDelay = initialDelay,
        _maxDelay = maxDelay,
        _random = random ?? math.Random.secure() {
    if (initialDelay.compareTo(Duration.zero) <= 0) {
      throw ArgumentError.value(
        initialDelay,
        'initialDelay',
        'Must be positive.',
      );
    }
    if (maxDelay.compareTo(initialDelay) < 0) {
      throw ArgumentError.value(
        maxDelay,
        'maxDelay',
        'Must be >= initialDelay.',
      );
    }
    if (maxAttempts <= 0) {
      throw ArgumentError.value(
        maxAttempts,
        'maxAttempts',
        'Must be positive.',
      );
    }
    if (jitterFactor < 0.0 || jitterFactor > 1.0) {
      throw ArgumentError.value(
        jitterFactor,
        'jitterFactor',
        'Must be in [0.0, 1.0].',
      );
    }
  }

  final Duration _initialDelay;
  final Duration _maxDelay;
  final math.Random _random;

  /// Maximum number of push attempts before dead-lettering. Must be > 0.
  final int maxAttempts;

  /// Fractional jitter applied to computed delays. Must be in
  /// `[0.0, 1.0]`.
  final double jitterFactor;

  static const Duration _minPositiveDelay = Duration(milliseconds: 1);

  void _validateAttemptCount(int attemptCount) {
    if (attemptCount < 1) {
      throw ArgumentError.value(
        attemptCount,
        'attemptCount',
        'Must be >= 1 (completed push attempts).',
      );
    }
  }

  @override
  Duration computeDelay(int attemptCount, SyncError error) {
    _validateAttemptCount(attemptCount);

    // Local backoff saturates at maxDelay. Computed in microseconds as
    // doubles so 2^(attemptCount - 1) can never overflow — the double
    // base keeps math.pow in floating point (an int base can wrap to
    // 0 on overflow), and values beyond the cap simply clamp to
    // maxDelay.
    final baseMicros = math.min(
      _initialDelay.inMicroseconds * math.pow(2.0, attemptCount - 1),
      _maxDelay.inMicroseconds.toDouble(),
    );
    final base = Duration(microseconds: baseMicros.round());

    // jittered = base * (1 - jitterFactor + random() * 2 * jitterFactor)
    final multiplier =
        1 - jitterFactor + _random.nextDouble() * 2 * jitterFactor;
    var jittered = Duration(
      microseconds: (base.inMicroseconds * multiplier).round(),
    );

    // Clamp to a strictly positive delay that never exceeds maxDelay.
    if (jittered.compareTo(_minPositiveDelay) < 0) {
      jittered = _minPositiveDelay;
    }
    if (jittered.compareTo(_maxDelay) > 0) {
      jittered = _maxDelay;
    }

    // retryAfter is a minimum — never shortened, never capped by
    // maxDelay.
    final retryAfter = error.retryAfter;
    if (retryAfter != null && retryAfter.compareTo(jittered) > 0) {
      return retryAfter;
    }
    return jittered;
  }

  @override
  bool shouldDeadLetter(int attemptCount, SyncError error) {
    _validateAttemptCount(attemptCount);
    if (!error.isRetryable) {
      return true;
    }
    return attemptCount >= maxAttempts;
  }
}
