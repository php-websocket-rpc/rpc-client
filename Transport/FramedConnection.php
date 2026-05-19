<?php

declare(strict_types=1);

namespace PhpWebsocketRpc\RpcClient\Transport;

use Amp\Websocket\WebsocketClient;
use PhpWebsocketRpc\Rpc\Payload\Payload;
use PhpWebsocketRpc\Rpc\Serialization\Serializer;

/**
 * Wraps an amphp WebsocketClient connection with the shared Serializer.
 *
 * Provides typed send/receive for Payload objects over binary WebSocket frames.
 */
final class FramedConnection
{
    private readonly Serializer $serializer;

    public function __construct(
        private readonly WebsocketClient $websocket,
        ?Serializer $serializer = null,
    ) {
        $this->serializer = $serializer ?? new Serializer();
    }

    /**
     * Serialize and send a Payload as a binary WebSocket message.
     */
    public function send(Payload $payload): void
    {
        $data = $this->serializer->encode($payload);
        $this->websocket->sendBinary($data);
    }

    /**
     * Receive and deserialize the next binary WebSocket message.
     *
     * Suspends until a message is available. Must be called from
     * within an amphp fiber context.
     *
     * @throws \Amp\Websocket\ClosedException
     */
    public function receive(): Payload
    {
        $message = $this->websocket->receive();
        $buffer = $message->buffer();

        return $this->serializer->decode($buffer);
    }

    /**
     * Iterate incoming messages as Payload objects.
     *
     * Yields each deserialized payload as it arrives.
     * Compatible with foreach.
     *
     * @return \Traversable<Payload>
     */
    public function receiveStream(): \Traversable
    {
        foreach ($this->websocket as $message) {
            $buffer = $message->buffer();
            yield $this->serializer->decode($buffer);
        }
    }

    /**
     * Close the underlying WebSocket connection.
     */
    public function close(): void
    {
        $this->websocket->close();
    }

    /**
     * Check if the connection is closed.
     */
    public function isClosed(): bool
    {
        return $this->websocket->isClosed();
    }

    /**
     * Get the underlying WebsocketClient instance.
     */
    public function getWebsocket(): WebsocketClient
    {
        return $this->websocket;
    }
}
