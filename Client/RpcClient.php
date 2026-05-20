<?php

declare(strict_types=1);

namespace PhpWebsocketRpc\RpcClient\Client;

use Amp\DeferredFuture;
use Amp\Future;
use Amp\TimeoutCancellation;
use Amp\Websocket\Client\WebsocketConnector;
use Amp\Websocket\Client\WebsocketHandshake;
use PhpWebsocketRpc\Rpc\Middleware\MiddlewarePipeline;
use PhpWebsocketRpc\Rpc\Payload\Error;
use PhpWebsocketRpc\Rpc\Payload\Kind;
use PhpWebsocketRpc\Rpc\Payload\Payload;
use PhpWebsocketRpc\Rpc\Payload\RpcResponse;
use PhpWebsocketRpc\Rpc\Payload\StreamClose;
use PhpWebsocketRpc\Rpc\Stream\StreamSubscribable;

use PhpWebsocketRpc\RpcClient\Middleware\ClientMiddlewareInterface;
use PhpWebsocketRpc\RpcClient\Stream\Subscription;
use PhpWebsocketRpc\Rpc\Transport\FramedConnection;

use function Amp\Websocket\Client\connect as wsConnect;

final class RpcClient
{
    private readonly FramedConnection $connection;
    private readonly PendingRequestStore $pendingRequests;
    private readonly SubscriptionManager $subscriptions;
    /** @var MiddlewarePipeline<Payload, Future> */
    private readonly MiddlewarePipeline $middlewarePipeline;
    private bool $closed = false;

    private function __construct(FramedConnection $connection)
    {
        $this->connection = $connection;
        $this->pendingRequests = new PendingRequestStore();
        $this->subscriptions = new SubscriptionManager();
        $this->middlewarePipeline = new MiddlewarePipeline();
    }

    public static function connect(string $uri, ?WebsocketConnector $connector = null): self
    {
        $ws = $connector ? $connector->connect(new WebsocketHandshake($uri)) : wsConnect($uri);
        $connection = new FramedConnection($ws);

        $client = new self($connection);
        $client->startReceiveLoop();

        return $client;
    }

    /**
     * @param Kind\RpcRequest&Payload $payload
     * @param float|null              $timeout  Optional timeout in seconds.
     *                                          When exceeded, throws ClientException(Error::TIMEOUT).
     *
     * @return Future<Payload>
     */
    public function call(Kind\RpcRequest&Payload $payload, ?float $timeout = null): Future
    {
        if ($this->closed) {
            throw new ClientException('Client is closed');
        }

        $responseClass = $payload::responseClass();
        if ($responseClass === null) {
            throw new ClientException(\sprintf(
                '%s must implement responseClass() to be used with call()',
                $payload::class,
            ));
        }

        $future = $this->pendingRequests->register($payload->id, $responseClass);

        $this->sendWithMiddleware($payload);

        if ($timeout !== null && $timeout > 0.0) {
            $requestId = $payload->id;
            $cancellation = new TimeoutCancellation($timeout);

            return \Amp\async(function () use ($future, $timeout, $cancellation, $requestId): Payload {
                try {
                    return $future->await($cancellation);
                } catch (\Amp\TimeoutException) {
                    $this->pendingRequests->reject($requestId, new \Amp\TimeoutException('Request timed out'));

                    throw new ClientException(
                        \sprintf('Request timed out after %.1f seconds', $timeout),
                        Error::TIMEOUT,
                    );
                }
            });
        }

        return $future;
    }

    public function notify(Kind\Notification&Payload $payload): void
    {
        $this->sendPayload($payload);
    }

    /**
     * @template T of Payload
     * @param Kind\StreamOpen&StreamSubscribable&T $payload
     *
     * @return Subscription
     */
    public function subscribe(Kind\StreamOpen&StreamSubscribable&Payload $payload): Subscription
    {
        $channel = $payload->channel();
        $subscription = new Subscription($channel);

        // When the user closes the subscription, notify the server
        $subscription->onClose(function () use ($channel): void {
            if (!$this->closed && !$this->connection->isClosed()) {
                $this->connection->send(new StreamClose($channel));
            }
        });

        $this->subscriptions->add($channel, $subscription);

        // Send the subscription request
        $this->sendPayload($payload);

        return $subscription;
    }

    /**
     * @param Kind\StreamData&Payload $payload
     */
    public function publish(Kind\StreamData&Payload $payload): void
    {
        $this->sendPayload($payload);
    }

    public function use(ClientMiddlewareInterface $middleware): void
    {
        $this->middlewarePipeline->use(static function (Payload $payload, callable $next) use ($middleware): \Amp\Future {
            return $middleware->handle($payload, $next);
        });
    }

    public function close(): void
    {
        if ($this->closed) {
            return;
        }

        $this->closed = true;

        $this->pendingRequests->rejectAll(new ClientException('Connection closed by client'));

        $this->subscriptions->closeAll();

        $this->connection->close();
    }

    public function isClosed(): bool
    {
        return $this->closed || $this->connection->isClosed();
    }

    /**
     * Create a dynamic RPC proxy from a service interface.
     *
     * The proxy intercepts all method calls and transparently performs
     * RPC operations via the contract system.
     *
     * Supported patterns:
     *   - call/response  (return type != void/Iterator)
     *   - notification   (return type = void, no callable param)
     *   - streaming      (return type = Iterator/Generator/Traversable/iterable)
     *   - subscription   (single callable param, void return)
     *
     * @template T of object
     * @param class-string<T> $interface
     *
     * @return T
     */
    public function createProxy(string $interface): object
    {
        $factory = new ContractProxyFactory($this);

        return $factory->createProxy($interface);
    }

    // ─── Internal ──────────────────────────────────────────────

    private function startReceiveLoop(): void
    {
        \Amp\async(function (): void {
            try {
                foreach ($this->connection->receiveStream() as $payload) {
                    $this->dispatchIncoming($payload);
                }
            } catch (\Throwable $e) {
                // Connection closed or error — clean up pending
                if (!$this->closed) {
                    $this->closed = true;
                    $this->pendingRequests->rejectAll($e);
                    $this->subscriptions->closeAll();
                }
            }
        });
    }

    private function dispatchIncoming(Payload $payload): void
    {
        match (true) {
            $payload instanceof Kind\RpcResponse => $this->handleRpcResponse($payload),
            $payload instanceof Kind\StreamData => $this->subscriptions->feed($payload),
            $payload instanceof Kind\StreamClose => $this->handleStreamClose($payload),
            default => null, // Ignore unknown types
        };
    }

    private function handleRpcResponse(Payload $payload): void
    {
        if (!$payload instanceof RpcResponse) {
            return;
        }

        if ($payload->isSuccess()) {
            $result = $payload->getPayload();
            if ($result !== null) {
                $this->pendingRequests->resolve($payload->id, $result);
            } else {
                $this->pendingRequests->reject($payload->id, new \RuntimeException('Empty success response payload'));
            }
        } else {
            $error = $payload->getError();
            $exception = $this->reconstructException($error);
            $this->pendingRequests->reject($payload->id, $exception);
        }
    }

    private function reconstructException(?Error $error): \Throwable
    {
        $message = $error->message ?? 'Unknown RPC error';
        $code = $error->code ?? Error::INTERNAL_ERROR;
        $data = $error?->data;
        $class = $error?->exceptionClass;

        if ($class !== null && \class_exists($class) && \is_subclass_of($class, \Throwable::class)) {
            try {
                return new $class($message, $code, $data);
            } catch (\Throwable) {
            }
        }

        return new ClientException($message, $code, $data);
    }

    private function handleStreamClose(Payload $payload): void
    {
        if ($payload instanceof Kind\StreamClose) {
            $this->subscriptions->close($payload->channel());
        }
    }

    private function sendWithMiddleware(Payload $payload): void
    {
        if ($this->middlewarePipeline->count() === 0) {
            $this->sendPayload($payload);
            return;
        }

        $this->middlewarePipeline->execute($payload, function (Payload $payload): \Amp\Future {
            $this->sendPayload($payload);
            return new DeferredFuture()->getFuture();
        });
    }

    private function sendPayload(Payload $payload): void
    {
        if ($this->closed || $this->connection->isClosed()) {
            throw new ClientException('Cannot send — connection is closed');
        }

        $this->connection->send($payload);
    }
}
