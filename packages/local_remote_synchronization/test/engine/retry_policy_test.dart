import 'dart:math' as math;

import 'package:local_remote_synchronization/local_remote_synchronization.dart';
import 'package:test/test.dart';

/// Deterministic [math.Random] returning a fixed sequence of doubles.
final class _SequenceRandom implements math.Random {
  _SequenceRandom(this.values);

  final List<double> values;
  var _index = 0;

  @override
  double nextDouble() => values[_index++ % values.length];

  @override
  bool nextBool() => nextDouble() > 0.5;

  @override
  int nextInt(int max) => (nextDouble() * max).floor();
}

SyncError retryableError({Duration? retryAfter}) => SyncError(
      code: SyncErrorCode.network,
      message: 'transient failure',
      isRetryable: true,
      retryAfter: retryAfter,
    );

SyncError nonRetryableError() => SyncError(
      code: SyncErrorCode.validation,
      message: 'permanent failure',
      isRetryable: false,
    );

void main() {
  group('ExponentialBackoffRetryPolicy constructor validation', () {
    test('accepts defaults', () {
      expect(() => ExponentialBackoffRetryPolicy(), returnsNormally);
    });

    test('rejects non-positive initialDelay', () {
      expect(
        () => ExponentialBackoffRetryPolicy(
          initialDelay: Duration.zero,
        ),
        throwsArgumentError,
      );
      expect(
        () => ExponentialBackoffRetryPolicy(
          initialDelay: const Duration(milliseconds: -1),
        ),
        throwsArgumentError,
      );
    });

    test('rejects maxDelay below initialDelay', () {
      expect(
        () => ExponentialBackoffRetryPolicy(
          initialDelay: const Duration(seconds: 10),
          maxDelay: const Duration(seconds: 5),
        ),
        throwsArgumentError,
      );
    });

    test('accepts maxDelay equal to initialDelay', () {
      expect(
        () => ExponentialBackoffRetryPolicy(
          initialDelay: const Duration(seconds: 5),
          maxDelay: const Duration(seconds: 5),
        ),
        returnsNormally,
      );
    });

    test('rejects non-positive maxAttempts', () {
      expect(
        () => ExponentialBackoffRetryPolicy(maxAttempts: 0),
        throwsArgumentError,
      );
      expect(
        () => ExponentialBackoffRetryPolicy(maxAttempts: -3),
        throwsArgumentError,
      );
    });

    test('rejects jitterFactor outside [0.0, 1.0]', () {
      expect(
        () => ExponentialBackoffRetryPolicy(jitterFactor: -0.1),
        throwsArgumentError,
      );
      expect(
        () => ExponentialBackoffRetryPolicy(jitterFactor: 1.1),
        throwsArgumentError,
      );
    });

    test('accepts jitterFactor boundaries 0.0 and 1.0', () {
      expect(
        () => ExponentialBackoffRetryPolicy(jitterFactor: 0.0),
        returnsNormally,
      );
      expect(
        () => ExponentialBackoffRetryPolicy(jitterFactor: 1.0),
        returnsNormally,
      );
    });
  });

  group('computeDelay', () {
    test('doubles per attempt with jitterFactor = 0.0', () {
      final policy = ExponentialBackoffRetryPolicy(
        initialDelay: const Duration(seconds: 1),
        maxDelay: const Duration(minutes: 5),
        jitterFactor: 0.0,
        random: _SequenceRandom(const [0.0]),
      );

      expect(
          policy.computeDelay(1, retryableError()), const Duration(seconds: 1));
      expect(
          policy.computeDelay(2, retryableError()), const Duration(seconds: 2));
      expect(
          policy.computeDelay(3, retryableError()), const Duration(seconds: 4));
      expect(
          policy.computeDelay(4, retryableError()), const Duration(seconds: 8));
    });

    test('saturates local backoff at maxDelay', () {
      final policy = ExponentialBackoffRetryPolicy(
        initialDelay: const Duration(seconds: 1),
        maxDelay: const Duration(seconds: 10),
        jitterFactor: 0.0,
        random: _SequenceRandom(const [0.0]),
      );

      // 1s * 2^9 = 512s >> 10s cap.
      expect(policy.computeDelay(10, retryableError()),
          const Duration(seconds: 10));
      expect(policy.computeDelay(100, retryableError()),
          const Duration(seconds: 10));
    });

    test('is overflow-safe for very large attempt counts', () {
      final policy = ExponentialBackoffRetryPolicy(
        initialDelay: const Duration(microseconds: 1),
        maxDelay: const Duration(seconds: 1),
        jitterFactor: 0.0,
        random: _SequenceRandom(const [0.0]),
      );

      // 2^2000 would overflow any integer representation; the result
      // must still be the capped maxDelay.
      expect(policy.computeDelay(2000, retryableError()),
          const Duration(seconds: 1));
    });

    test('applies symmetric jitter around the base', () {
      // jitterFactor 0.2 -> multiplier range [0.8, 1.2].
      final low = ExponentialBackoffRetryPolicy(
        initialDelay: const Duration(seconds: 10),
        jitterFactor: 0.2,
        random: _SequenceRandom(const [0.0]), // multiplier = 0.8
      );
      final high = ExponentialBackoffRetryPolicy(
        initialDelay: const Duration(seconds: 10),
        jitterFactor: 0.2,
        random: _SequenceRandom(const [1.0]), // multiplier = 1.2
      );
      final mid = ExponentialBackoffRetryPolicy(
        initialDelay: const Duration(seconds: 10),
        jitterFactor: 0.2,
        random: _SequenceRandom(const [0.5]), // multiplier = 1.0
      );

      expect(low.computeDelay(1, retryableError()), const Duration(seconds: 8));
      expect(
          high.computeDelay(1, retryableError()), const Duration(seconds: 12));
      expect(
          mid.computeDelay(1, retryableError()), const Duration(seconds: 10));
    });

    test('jitterFactor = 1.0 can reach near zero but stays positive', () {
      final policy = ExponentialBackoffRetryPolicy(
        initialDelay: const Duration(seconds: 10),
        jitterFactor: 1.0,
        random: _SequenceRandom(const [0.0]), // multiplier = 0.0
      );

      final delay = policy.computeDelay(1, retryableError());
      expect(delay, greaterThan(Duration.zero));
      expect(delay, const Duration(milliseconds: 1));
    });

    test('delay never exceeds maxDelay after jitter clamping', () {
      final policy = ExponentialBackoffRetryPolicy(
        initialDelay: const Duration(seconds: 1),
        maxDelay: const Duration(seconds: 2),
        jitterFactor: 1.0,
        random: _SequenceRandom(const [1.0]), // max jitter
      );

      // base already capped at 2s for attempt 2; jitter could push
      // higher but the clamp restores maxDelay.
      expect(
          policy.computeDelay(5, retryableError()), const Duration(seconds: 2));
    });

    test('retryAfter smaller than local delay does not shorten it', () {
      final policy = ExponentialBackoffRetryPolicy(
        initialDelay: const Duration(seconds: 10),
        jitterFactor: 0.0,
        random: _SequenceRandom(const [0.0]),
      );

      expect(
        policy.computeDelay(
          1,
          retryableError(retryAfter: const Duration(seconds: 2)),
        ),
        const Duration(seconds: 10),
      );
    });

    test('retryAfter larger than local delay is honored', () {
      final policy = ExponentialBackoffRetryPolicy(
        initialDelay: const Duration(seconds: 1),
        jitterFactor: 0.0,
        random: _SequenceRandom(const [0.0]),
      );

      expect(
        policy.computeDelay(
          1,
          retryableError(retryAfter: const Duration(seconds: 30)),
        ),
        const Duration(seconds: 30),
      );
    });

    test('retryAfter larger than maxDelay is NOT capped', () {
      final policy = ExponentialBackoffRetryPolicy(
        initialDelay: const Duration(seconds: 1),
        maxDelay: const Duration(seconds: 10),
        jitterFactor: 0.0,
        random: _SequenceRandom(const [0.0]),
      );

      expect(
        policy.computeDelay(
          1,
          retryableError(retryAfter: const Duration(minutes: 10)),
        ),
        const Duration(minutes: 10),
      );
    });

    test('rejects attemptCount < 1', () {
      final policy = ExponentialBackoffRetryPolicy(
        random: _SequenceRandom(const [0.0]),
      );

      expect(
          () => policy.computeDelay(0, retryableError()), throwsArgumentError);
      expect(
          () => policy.computeDelay(-1, retryableError()), throwsArgumentError);
    });
  });

  group('shouldDeadLetter', () {
    test('non-retryable error dead-letters on first failure', () {
      final policy = ExponentialBackoffRetryPolicy(maxAttempts: 10);
      expect(policy.shouldDeadLetter(1, nonRetryableError()), isTrue);
    });

    test('retryable error retries below maxAttempts', () {
      final policy = ExponentialBackoffRetryPolicy(maxAttempts: 3);
      expect(policy.shouldDeadLetter(1, retryableError()), isFalse);
      expect(policy.shouldDeadLetter(2, retryableError()), isFalse);
    });

    test('retryable error dead-letters at maxAttempts', () {
      final policy = ExponentialBackoffRetryPolicy(maxAttempts: 3);
      expect(policy.shouldDeadLetter(3, retryableError()), isTrue);
      expect(policy.shouldDeadLetter(4, retryableError()), isTrue);
    });

    test('non-retryable error dead-letters regardless of attempts', () {
      final policy = ExponentialBackoffRetryPolicy(maxAttempts: 100);
      expect(policy.shouldDeadLetter(99, nonRetryableError()), isTrue);
    });

    test('rejects attemptCount < 1', () {
      final policy = ExponentialBackoffRetryPolicy();
      expect(() => policy.shouldDeadLetter(0, retryableError()),
          throwsArgumentError);
    });
  });
}
