<?php
/**
 * This file is a part of sebk/small-orm-swoole
 * Copyright 2021 - Sébastien Kus
 * Under GNU GPL V3 licence
 */

namespace Sebk\SmallOrmSwoole\Database;

use Sebk\SmallOrmCore\Database\AbstractConnection;
use Sebk\SmallOrmCore\Database\ConnectionException;
use Sebk\SmallOrmSwoole\Pool\PRedisConfig;
use Sebk\SmallSwoolePatterns\Manager\Connection\PRedisClientManager;
use Sebk\SmallSwoolePatterns\Pool\Pool;
use Swoole\Coroutine;

/**
 * Connection to redis database
 */
class ConnectionSwooleRedis extends AbstractConnection
{
    const MAX_CONNECTIONS = 100;

    public $pool;

    /**
     * Create redis object, use existing if exists and connect
     * @throws ConnectionException
     */
    public function connect($forceReconnect = false)
    {
        if (Coroutine::getuid() === -1) {
            $result = null;
            \Co\run(function() use($forceReconnect, &$result) {
                $result = $this->connect($forceReconnect);
            });
            return $result;
        }

        if ($this->pool == null) {
            $this->pool = new Pool(new PRedisClientManager($this->host),
                static::MAX_CONNECTIONS
            );
        }

        return $this->pool;
    }

    /**
     * Execute redis instruction
     * @param $sql
     * @param $params
     * @param $retry
     * @param $forceConnection
     * @return array
     * @throws ConnectionException
     */
    public function execute($sql, $params = [], $retry = false, $forceConnection = null)
    {
        if (Coroutine::getuid() === -1) {
            $result = null;
            \Co\run(function() use($sql, $params, $retry, $forceConnection, &$result) {
                $result = $this->execute($sql, $params, $retry, $forceConnection);
            });
            return $result;
        }

        $this->connect();

        if (!array_key_exists("key", $params) && !in_array($sql,  ["keys"])) {
            throw new \Exception("Fail to query redis : \$param['key'] must be defined");
        }

        $con = $this->pool->get();
        switch ($sql) {
            case "get":
                $result = $this->get($params["key"], $con);
                break;
            case "set":
                $this->set($params["key"], isset($params["value"]) ? $params["value"] : "", $con);
                $result = null;
                break;
            case "del":
                $result = $this->del($params["key"], $con);
                break;
            case "keys":
                $result = $this->keys($params, $con);
                break;

            default:
                throw new \Exception("Redis instruction not found ! ($sql)");
        }
        $this->pool->put($con);

        return $result;
    }

    /**
     * Get value of a key
     * @param mixed $fullkey
     * @return array
     */
    protected function get(array $fullkey, $con): array
    {
        if (count($fullkey) == 0) {
            throw new \Exception("Redis instruction get with empty key bag");
        }

        if (count($fullkey) == 1) {
            return [json_decode($con->get($fullkey[0]), true)];
        }

        $result = [];
        foreach ($con->mget($fullkey) as $item) {
            $result[] = json_decode($item, true);
        }

        return $result;
    }

    /**
     * Set a value of a key
     * @param string $fullkey
     * @param $value
     * @return $this
     */
    protected function set(array $fullkey, array $value, $con)
    {
        $result = true;
        foreach ($fullkey as $i => $key) {
            if (!$con->set($key, json_encode($value[$i]))) {
                $result = false;
            }
        }

        if (!$result) {
            throw new \Exception("Redis set failed !");
        }

        return $this;
    }

    /**
     * Delete a value of a key
     * @param string $fullkey
     * @return $this
     */
    protected function del(array $fullkey, $con)
    {
        foreach ($fullkey as $i => $key) {
            if (!$con->del($key)) {
                $result = false;
            }
        }

        return $this;
    }

    protected function keys(array $params, $con)
    {
        $result = [];
        foreach ($params["key"] as $pattern) {
            $result = array_merge($result, $con->keys($pattern));
        }

        if (!is_array($result)) {
            return [];
        }

        return $result;
    }

    /**
     * Start transaction
     * @return $this
     * @throws ConnectionException
     * @throws TransactionException
     */
    public function startTransaction()
    {
        throw new \Exception("Redis connection does not support transactions");
    }

    /**
     * Commit transaction
     * @return $this
     * @throws TransactionException
     */
    public function commit()
    {
        throw new \Exception("Redis connection does not support transactions");
    }

    /**
     * Rollback transaction
     * @return $this
     * @throws ConnectionException
     * @throws TransactionException
     */
    public function rollback()
    {
        throw new \Exception("Redis connection does not support transactions");
    }

    /**
     * Get last insert id
     * @return int
     */
    public function lastInsertId()
    {
        throw new \Exception("Redis connection does not support last insert id");
    }

}
