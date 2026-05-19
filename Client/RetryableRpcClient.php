<?php

declare(strict_types=1);

namespace PhpWebsocketRpc\RpcClient\Client;

use Amp\Future;
use PhpWebsocketRpc\Rpc\Exception\RateLimitException;
use PhpWebsocketRpc\Rpc\Payload\Kind;
use PhpWebsocketRpc\Rpc\Payload\Payload;
use PhpWebsocketRpc\Rpc\Stream\StreamSubscribable;
use PhpWebsocketRpc\RpcClient\Middleware\ClientMiddlewareInterface;
use PhpWebsocketRpc\RpcClient\Stream\Subscription;
use Amp\Websocket\Client\WebsocketConnector;

/**
 * RPC client wrapper that automatically retries on rate limit errors.
 *
 * Delegates all operations to an inner RpcClient instance. The call()
 * method catches RateLimitException and retries with exponential backoff.
 * All other methods delegate directly.
 *
 * Usage:
 *   $client = RetryableRpcClient::connect(
 *       uri: 'ws://127.0.0.1:9501/rpc',
 *       strategy: new RetryStrategy(maxRetries: 3, initialDelayMs: 500),
 *   );
 *
 *   // Auto-retries up to 3 times with exponential backoff
 *   $result = $client->call($request)->await();
 */
final class RetryableRpcClient
{
    private readonly RpcClient $client;
    private readonly RetryStrategy $strategy;

    private function __construct(RpcClient $client, RetryStrategy $strategy)
    {
        $this->client = $client;
        $this->strategy = $strategy;
    }

    /**
     * Connect to an RPC WebSocket server with retry support.
     *
     * @param string                $uri       WebSocket URI (ws:// or wss://)
     * @param RetryStrategy|null    $strategy  Backoff config (default: 3 retries, 500ms base, 2x multiplier)
     * @param WebsocketConnector|null $connector Custom WebSocket connector (e.g. for mTLS)
     */
    public static function connect(
        string $uri,
        ?RetryStrategy $strategy = null,
        ?WebsocketConnector $connector = null,
    ): self {
        return new self(
            RpcClient::connect($uri, $connector),
            $strategy ?? new RetryStrategy(),
        );
    }

    /**
     * Call an RPC method with automatic retry on rate limit errors.
     *
     * Uses exponential backoff when the server returns a RateLimitException.
     * Other exceptions propagate immediately.
     *
     * Returns a Future immediately — the caller must await() it.
     * The retry loop runs inside the future, so the calling fiber
     * is suspended during backoff delays.
     *
     * @param Kind\RpcRequest&Payload $payload
     * @param float|null              $timeout  Optional timeout in seconds per attempt.
     *
     * @return Future<Payload>
     */
    public function call(Kind\RpcRequest&Payload $payload, ?float $timeout = null): Future
    {
        return \Amp\async(function () use ($payload, $timeout): Payload {
            $attempt = 0;

            while (true) {
                try {
                    return $this->client->call($payload, $timeout)->await();
                } catch (RateLimitException $e) {
                    $attempt++;
                    if ($attempt > $this->strategy->maxRetries) {
                        throw $e;
                    }

                    $delaySeconds = $this->strategy->getDelay($attempt);
                    \Amp\delay($delaySeconds);
                }
            }
        });
    }

    /**
     * @param Kind\Notification&Payload $payload
     */
    public function notify(Kind\Notification&Payload $payload): void
    {
        $this->client->notify($payload);
    }

    /**
     * @template T of Payload
     * @param Kind\StreamOpen&StreamSubscribable&T $payload
     *
     * @return Subscription
     */
    public function subscribe(Kind\StreamOpen&StreamSubscribable&Payload $payload): Subscription
    {
        return $this->client->subscribe($payload);
    }

    /**
     * @param Kind\StreamData&Payload $payload
     */
    public function publish(Kind\StreamData&Payload $payload): void
    {
        $this->client->publish($payload);
    }

    public function use(ClientMiddlewareInterface $middleware): void
    {
        $this->client->use($middleware);
    }

    public function close(): void
    {
        $this->client->close();
    }

    public function isClosed(): bool
    {
        return $this->client->isClosed();
    }
}
