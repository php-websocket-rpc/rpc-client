<?php

declare(strict_types=1);

namespace PhpWebsocketRpc\RpcClient\Client;

/**
 * Configuration for exponential backoff retry strategy.
 *
 * Used by RpcClient::callWithRetry() to automatically retry
 * rate-limited requests with increasing delays.
 *
 * Defaults:
 *   maxRetries:    3
 *   initialDelay: 500ms
 *   multiplier:   2.0  (500ms → 1s → 2s → 4s → ...)
 *   maxDelay:     30s
 *   jitter:       100ms random +/- to avoid thundering herd
 *
 * Usage:
 *   new RetryStrategy(maxRetries: 5, initialDelayMs: 200);
 */
final class RetryStrategy
{
    /**
     * @param positive-int $maxRetries    Max number of retry attempts (default: 3)
     * @param positive-int $initialDelayMs Initial backoff delay in milliseconds (default: 500)
     * @param float        $multiplier    Exponential factor per attempt (default: 2.0)
     * @param positive-int $maxDelayMs    Cap on delay in milliseconds (default: 30_000)
     * @param positive-int $jitterMs      Random +/- jitter in milliseconds (default: 100, 0 to disable)
     */
    public function __construct(
        public readonly int $maxRetries = 3,
        public readonly int $initialDelayMs = 500,
        public readonly float $multiplier = 2.0,
        public readonly int $maxDelayMs = 30_000,
        public readonly int $jitterMs = 100,
    ) {
        if ($maxRetries < 1) {
            throw new \InvalidArgumentException('maxRetries must be at least 1');
        }
        if ($initialDelayMs < 1) {
            throw new \InvalidArgumentException('initialDelayMs must be at least 1');
        }
        if ($multiplier < 1.0) {
            throw new \InvalidArgumentException('multiplier must be >= 1.0');
        }
        if ($maxDelayMs < 1) {
            throw new \InvalidArgumentException('maxDelayMs must be at least 1');
        }
    }

    /**
     * Calculate the delay in seconds for the Nth retry attempt.
     *
     * @param positive-int $attempt The retry attempt number (1-based)
     *
     * @return float Delay in seconds
     */
    public function getDelay(int $attempt): float
    {
        $delay = $this->initialDelayMs * ($this->multiplier ** ($attempt - 1));
        $delay = \min($delay, $this->maxDelayMs);

        // Add random jitter to avoid thundering herd
        if ($this->jitterMs > 0) {
            $jitter = \random_int(0, $this->jitterMs * 2) - $this->jitterMs;
            $delay += $jitter;
        }

        return \max($delay, 0.0) / 1000.0;
    }
}
