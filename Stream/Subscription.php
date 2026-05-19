<?php

declare(strict_types=1);

namespace PhpWebsocketRpc\RpcClient\Stream;

use Amp\Pipeline\ConcurrentIterator;
use Amp\Pipeline\Queue;
use PhpWebsocketRpc\Rpc\Payload\Payload;

/**
 * Client-side handle for a subscribed stream channel.
 *
 * Wraps an internal Queue that the background receive fiber
 * feeds data into. The user iterates over the Subscription
 * with foreach.
 *
 * Usage:
 *   $sub = $client->subscribe(new SubscribeOrders(filter: 'active'));
 *   foreach ($sub as $data) {
 *       // $data is the typed payload (e.g. OrderEvent)
 *   }
 */
final class Subscription implements \IteratorAggregate
{
    /** @var Queue<Payload> */
    private readonly Queue $queue;

    /** @var ConcurrentIterator<Payload>|null */
    private ?ConcurrentIterator $iterator = null;

    private bool $closed = false;

    /** @var \Closure(): void|null */
    private ?\Closure $onCloseCallback = null;

    public function __construct(
        private readonly string $channel,
    ) {
        $this->queue = new Queue();
    }

    /**
     * Register a callback to invoke when this subscription is closed.
     *
     * The callback is called once, when close() is first invoked.
     *
     * @param \Closure(): void $callback
     */
    public function onClose(\Closure $callback): void
    {
        $this->onCloseCallback = $callback;
    }

    /**
     * Get the channel name this subscription belongs to.
     */
    public function getChannel(): string
    {
        return $this->channel;
    }

    /**
     * Push incoming stream data into the subscription queue.
     *
     * Called internally by the client's receive fiber.
     */
    public function push(Payload $data): void
    {
        if ($this->closed) {
            return;
        }

        $this->queue->pushAsync($data);
    }

    /**
     * Close the subscription and signal the stream has ended.
     *
     * Invokes the registered onClose callback once.
     */
    public function complete(): void
    {
        if ($this->closed) {
            return;
        }

        $this->closed = true;
        $this->queue->complete();

        if ($this->onCloseCallback !== null) {
            $cb = $this->onCloseCallback;
            $this->onCloseCallback = null;
            $cb();
        }
    }

    /**
     * Signal that the stream ended with an error.
     */
    public function error(\Throwable $error): void
    {
        if ($this->closed) {
            return;
        }

        $this->closed = true;
        $this->queue->error($error);

        if ($this->onCloseCallback !== null) {
            $cb = $this->onCloseCallback;
            $this->onCloseCallback = null;
            $cb();
        }
    }

    /**
     * Iterate over incoming stream data.
     *
     * Yields each typed Payload as it arrives from the server.
     *
     * @return ConcurrentIterator<Payload>
     */
    public function getIterator(): ConcurrentIterator
    {
        if ($this->iterator === null) {
            $this->iterator = $this->queue->iterate();
        }

        return $this->iterator;
    }
}
