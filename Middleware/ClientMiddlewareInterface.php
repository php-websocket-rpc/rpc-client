<?php

declare(strict_types=1);

namespace PhpWebsocketRpc\RpcClient\Middleware;

use PhpWebsocketRpc\Rpc\Payload\Payload;

interface ClientMiddlewareInterface
{
    /**
     * @param callable(Payload): \Amp\Future $next Next in chain
     */
    public function handle(Payload $payload, callable $next): \Amp\Future;
}
