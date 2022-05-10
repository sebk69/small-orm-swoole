<?php

namespace Sebk\SmallOrmSwoole\Pool;

use Predis\Client;

class PRedisPool extends \Swoole\ConnectionPool
{

    public function __construct(protected PRedisConfig $config, int $maxConnections, ?string $proxy = null)
    {
        parent::__construct([$this, 'createConnection'], $maxConnections, $proxy);
    }

    public function createConnection(): Client
    {
        $hosts = explode(",", $this->config->hosts);
        return new \Predis\Client(count($hosts) == 1 ? $hosts[0] : $hosts, count($hosts) > 1
            ? array_merge(["cluster" => "redis-cluster", "exceptions" => true, "async_connect" => false], $this->config->options)
            : $this->config->options);
    }

}