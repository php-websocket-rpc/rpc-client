<?php

declare(strict_types=1);

namespace PhpWebsocketRpc\RpcClient\Client;

use PhpWebsocketRpc\Rpc\Contract\Attribute\RpcPublish;
use PhpWebsocketRpc\Rpc\Contract\Attribute\RpcStream;
use PhpWebsocketRpc\Rpc\Contract\Attribute\RpcSubscribe;
use PhpWebsocketRpc\Rpc\Contract\ContractInvocation;
use PhpWebsocketRpc\Rpc\Contract\ContractPublish;
use PhpWebsocketRpc\Rpc\Contract\ContractSerializer;
use PhpWebsocketRpc\Rpc\Contract\ContractStreamInvocation;
use PhpWebsocketRpc\Rpc\Contract\ContractStreamValue;
use ProxyManager\Factory\AccessInterceptorValueHolderFactory;
use ProxyManager\Factory\NullObjectFactory;

/**
 * Generates RPC proxy objects from service interfaces.
 *
 * Uses proxy-manager-lts to:
 *   1. Create a null-object stub implementing the interface (NullObjectFactory).
 *   2. Wrap it with method interceptors (AccessInterceptorValueHolderFactory).
 *
 * Each intercepted method converts the call into a contract-based RPC operation.
 * Method behavior is determined by return type, parameters, and attributes:
 *
 *   Attribute                  | Pattern        | Behavior
 *   ──────────────────────────|────────────────|──────────────────────────────
 *   #[RpcPublish('chan')]     | Publish        | $client->publish(ContractPublish)
 *   #[RpcSubscribe('chan')]   | Subscribe      | $client->subscribe() + callback
 *   #[RpcStream]              | Stream         | $client->subscribe() + Generator
 *   (callable param, void)    | Subscribe      | $client->subscribe() + callback (auto channel)
 *   (Iterator return)         | Stream         | $client->subscribe() + Generator (auto channel)
 *   (void return)             | Notification   | $client->notify(ContractInvocation)
 *   (everything else)         | Call/Response  | $client->call(ContractInvocation)
 */
final class ContractProxyFactory
{
    private readonly NullObjectFactory $nullFactory;
    private readonly AccessInterceptorValueHolderFactory $interceptorFactory;

    public function __construct(
        private readonly RpcClient $rpcClient,
    ) {
        $this->nullFactory = new NullObjectFactory();
        $this->interceptorFactory = new AccessInterceptorValueHolderFactory();
    }

    /**
     * Create a proxy for the given service interface.
     *
     * @template T of object
     * @param class-string<T> $interface
     *
     * @return T
     */
    public function createProxy(string $interface): object
    {
        $stub = $this->nullFactory->createProxy($interface);
        $interceptors = $this->buildInterceptors($interface);

        /** @var T */
        return $this->interceptorFactory->createProxy($stub, $interceptors);
    }

    /**
     * Build prefix interceptors for every method on the interface.
     *
     * @return array<string, \Closure> method name → interceptor closure
     */
    private function buildInterceptors(string $interface): array
    {
        $refClass = new \ReflectionClass($interface);
        $interceptors = [];

        foreach ($refClass->getMethods() as $method) {
            $interceptors[$method->getName()] = $this->buildInterceptor($method, $interface);
        }

        return $interceptors;
    }

    /**
     * Build an interceptor closure for a single method.
     */
    private function buildInterceptor(\ReflectionMethod $method, string $interface): \Closure
    {
        $returnType = $method->getReturnType();
        $returnTypeName = $returnType instanceof \ReflectionNamedType ? $returnType->getName() : null;
        $params = $method->getParameters();

        // ─── Check attributes ──────────────────────────────────

        $publishAttr = $method->getAttributes(RpcPublish::class);
        $subscribeAttr = $method->getAttributes(RpcSubscribe::class);
        $streamAttr = $method->getAttributes(RpcStream::class);

        // ─── 1. Publish pattern ───────────────────────────────

        if (!empty($publishAttr)) {
            $channel = $publishAttr[0]->newInstance()->channel;

            return function (
                object $proxy,
                object $instance,
                string $methodName,
                array $args,
                bool &$returnEarly,
            ) use ($interface, $channel): void {
                $publish = new ContractPublish(
                    service: $interface,
                    method: $methodName,
                    data: ContractSerializer::encode(\array_values($args)),
                    channelName: $channel,
                );

                $this->rpcClient->publish($publish);
                $returnEarly = true;
            };
        }

        // ─── 2. Subscribe pattern (attribute or callable param) ─

        $hasCallableParam = $this->hasCallableParameter($params);

        if (!empty($subscribeAttr) || ($hasCallableParam && $returnTypeName === 'void')) {
            $attr = !empty($subscribeAttr) ? $subscribeAttr[0]->newInstance() : null;
            $channel = $attr?->channel ?: '';
            $callableArgType = $attr?->type;

            return function (
                object $proxy,
                object $instance,
                string $methodName,
                array $args,
                bool &$returnEarly,
            ) use ($interface, $channel, $callableArgType): void {
                $userCallback = $args[\array_key_last($args)] ?? null;

                if (!\is_callable($userCallback)) {
                    throw new \InvalidArgumentException(\sprintf(
                        'Last argument to %s::%s must be a callable',
                        $interface,
                        $methodName,
                    ));
                }

                $invocation = new ContractStreamInvocation(
                    service: $interface,
                    method: $methodName,
                    params: [],
                );

                if ($channel !== '') {
                    $invocation->setChannel($channel);
                }

                $subscription = $this->rpcClient->subscribe($invocation);
                $returnEarly = true;

                \Amp\async(function () use ($subscription, $userCallback, $callableArgType): void {
                    foreach ($subscription as $streamValue) {
                        if ($streamValue instanceof ContractStreamValue) {
                            $decoded = $streamValue->value;

                            if ($callableArgType !== null && \is_array($decoded)) {
                                $decoded = ContractSerializer::decode($decoded, $callableArgType);
                            }

                            $userCallback($decoded);
                        }
                    }
                });
            };
        }

        // ─── 3. Stream pattern (attribute or Iterator return) ──

        $isIteratorReturn = \in_array($returnTypeName, [
            'Iterator', 'Generator', 'Traversable', 'iterable',
        ], true);

        if (!empty($streamAttr) || $isIteratorReturn) {
            $attr = !empty($streamAttr) ? $streamAttr[0]->newInstance() : null;
            $innerType = $attr?->type;

            return function (
                object $proxy,
                object $instance,
                string $methodName,
                array $args,
                bool &$returnEarly,
            ) use ($interface, $innerType): \Generator {
                $invocation = new ContractStreamInvocation(
                    service: $interface,
                    method: $methodName,
                    params: $args,
                );

                $subscription = $this->rpcClient->subscribe($invocation);
                $returnEarly = true;

                return (function () use ($subscription, $innerType): \Generator {
                    foreach ($subscription as $streamValue) {
                        if ($streamValue instanceof ContractStreamValue) {
                            $decoded = $streamValue->value;

                            if ($innerType !== null && \is_array($decoded)) {
                                $decoded = ContractSerializer::decode($decoded, $innerType);
                            }

                            yield $decoded;
                        }
                    }
                })();
            };
        }

        // ─── 4. Notification pattern ─────────────────────────

        if ($returnTypeName === 'void') {
            return function (
                object $proxy,
                object $instance,
                string $methodName,
                array $args,
                bool &$returnEarly,
            ) use ($interface): void {
                $invocation = new ContractInvocation(
                    service: $interface,
                    method: $methodName,
                    params: \array_map([ContractSerializer::class, 'encode'], $args),
                );

                $this->rpcClient->notify($invocation);
                $returnEarly = true;
            };
        }

        // ─── 5. Call/Response pattern ─────────────────────────

        return function (
            object $proxy,
            object $instance,
            string $methodName,
            array $args,
            bool &$returnEarly,
        ) use ($interface): mixed {
            $invocation = new ContractInvocation(
                service: $interface,
                method: $methodName,
                params: \array_map([ContractSerializer::class, 'encode'], $args),
            );

            /** @var \Amp\Future<\PhpWebsocketRpc\Rpc\Contract\ContractResponse> $future */
            $future = $this->rpcClient->call($invocation);
            $response = $future->await();
            $returnEarly = true;

            if ($response instanceof \PhpWebsocketRpc\Rpc\Contract\ContractResponse) {
                return ContractSerializer::decode($response->result);
            }

            return $response;
        };
    }

    /**
     * Check if a method has a callable parameter.
     *
     * @param array<\ReflectionParameter> $params
     */
    private function hasCallableParameter(array $params): bool
    {
        foreach ($params as $param) {
            $type = $param->getType();
            if ($type instanceof \ReflectionNamedType && $type->getName() === 'callable') {
                return true;
            }
        }

        return false;
    }
}
