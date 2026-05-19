<?php

declare(strict_types=1);

namespace PhpWebsocketRpc\RpcClient\Tests;

use PHPUnit\Framework\TestCase;
use PhpWebsocketRpc\RpcClient\Client\ClientException;
use PhpWebsocketRpc\Rpc\Payload\Payload;
use PhpWebsocketRpc\Rpc\Payload\Kind;

class RpcClientTest extends TestCase
{
    public function testClientExceptionConstruction(): void
    {
        $e = new ClientException('test error', -32000, ['key' => 'val']);
        $this->assertSame('test error', $e->getMessage());
        $this->assertSame(-32000, $e->getRpcCode());
        $this->assertSame(['key' => 'val'], $e->getErrorData());
    }

    public function testClientExceptionDefaults(): void
    {
        $e = new ClientException();
        $this->assertSame('', $e->getMessage());
        $this->assertSame(0, $e->getRpcCode());
        $this->assertNull($e->getErrorData());
    }

    public function testCallReturnsFuture(): void
    {
        // Verify the call() method signature returns Future by
        // checking it's an instance of Amp\Future
        $ref = new \ReflectionMethod(
            \PhpWebsocketRpc\RpcClient\Client\RpcClient::class,
            'call'
        );

        $returnType = $ref->getReturnType();
        $this->assertNotNull($returnType);
        $this->assertSame(\Amp\Future::class, $returnType->getName());
    }
}
