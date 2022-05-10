<?php

namespace Sebk\SmallOrmSwoole\Pool;

class PRedisConfig
{
    public function __construct(public string $hosts, public array $options)
    {
    }
}