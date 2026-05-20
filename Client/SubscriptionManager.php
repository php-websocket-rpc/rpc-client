<?php

declare(strict_types=1);

namespace PhpWebsocketRpc\RpcClient\Client;

use PhpWebsocketRpc\Rpc\Payload\Payload;
use PhpWebsocketRpc\Rpc\Stream\StreamChannelAware;
use PhpWebsocketRpc\RpcClient\Stream\Subscription;

/**
 * @internal
 */
final class SubscriptionManager
{
    /**
     * @var array<string, Subscription>
     */
    private array $subscriptions = [];

    public function add(string $channel, Subscription $subscription): void
    {
        $this->subscriptions[$channel] = $subscription;
    }

    public function remove(string $channel): void
    {
        if (isset($this->subscriptions[$channel])) {
            $this->subscriptions[$channel]->complete();
            unset($this->subscriptions[$channel]);
        }
    }

    public function feed(Payload $payload): void
    {
        if (!$payload instanceof StreamChannelAware) {
            return; // Not a stream-aware payload, can't route
        }

        $channel = $payload->channel();
        $sub = $this->subscriptions[$channel] ?? null;

        if ($sub !== null) {
            $sub->push($payload);
        }
    }

    public function close(string $channel): void
    {
        $this->remove($channel);
    }

    public function closeAll(): void
    {
        foreach (\array_keys($this->subscriptions) as $channel) {
            $this->remove($channel);
        }
    }

    public function has(string $channel): bool
    {
        return isset($this->subscriptions[$channel]);
    }

    public function count(): int
    {
        return \count($this->subscriptions);
    }
}
