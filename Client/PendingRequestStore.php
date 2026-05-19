<?php

declare(strict_types=1);

namespace PhpWebsocketRpc\RpcClient\Client;

use Amp\DeferredFuture;
use Amp\Future;
use PhpWebsocketRpc\Rpc\Payload\Payload;

/**
 * @internal
 */
final class PendingRequestStore
{
    /**
     * @var array<string, array{DeferredFuture, class-string<Payload>}>
     */
    private array $pending = [];

    /**
     * @param class-string<Payload> $responseClass The expected response class
     *
     * @return Future<Payload> A Future that resolves with the typed response
     */
    public function register(string $id, string $responseClass): Future
    {
        $deferred = new DeferredFuture();
        $this->pending[$id] = [$deferred, $responseClass];

        return $deferred->getFuture();
    }

    public function resolve(string $id, Payload $payload): void
    {
        $entry = $this->pending[$id] ?? null;

        if ($entry === null) {
            return; // Unknown ID — might be a late response after timeout
        }

        [$deferred, $expectedClass] = $entry;
        unset($this->pending[$id]);

        if ($payload instanceof $expectedClass) {
            $deferred->complete($payload);
        } else {
            $deferred->error(new \RuntimeException(\sprintf(
                'Expected response of type %s, got %s',
                $expectedClass,
                $payload::class,
            )));
        }
    }

    public function reject(string $id, \Throwable $error): void
    {
        $entry = $this->pending[$id] ?? null;

        if ($entry === null) {
            return;
        }

        [$deferred] = $entry;
        unset($this->pending[$id]);

        $deferred->error($error);
    }

    public function rejectAll(\Throwable $error): void
    {
        foreach ($this->pending as $id => [$deferred]) {
            $deferred->error($error);
        }

        $this->pending = [];
    }

    public function hasPending(): bool
    {
        return $this->pending !== [];
    }

    public function count(): int
    {
        return \count($this->pending);
    }
}
