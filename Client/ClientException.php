<?php

declare(strict_types=1);

namespace PhpWebsocketRpc\RpcClient\Client;

final class ClientException extends \RuntimeException
{
    public function __construct(
        string $message = '',
        private readonly int $rpcCode = 0,
        private readonly ?array $data = null,
        ?\Throwable $previous = null,
    ) {
        parent::__construct($message, 0, $previous);
    }

    public function getRpcCode(): int
    {
        return $this->rpcCode;
    }

    public function getErrorData(): ?array
    {
        return $this->data;
    }
}
