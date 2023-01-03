<?php
/**
 * This file is a part of sebk/small-orm-swoole
 * Copyright 2021 - Sébastien Kus
 * Under GNU GPL V3 licence
 */

namespace Sebk\SmallOrmSwoole\Database;

use mysql_xdevapi\Exception;
use Sebk\SmallOrmCore\Database\AbstractConnection;
use Sebk\SmallOrmCore\Database\ConnectionException;

use Sebk\SmallOrmCore\Database\ConnectionMysql;
use Sebk\SmallOrmSwoole\Pool\Connection;
use Sebk\SmallOrmSwoole\Pool\PdoMysqlPool;
use Sebk\SmallSwoolePatterns\Manager\Connection\MysqlClientManager;
use Sebk\SmallSwoolePatterns\Pool\Pool;
use Swoole\Coroutine;
use Swoole\Database\PDOConfig;
use Swoole\Database\PDOPool;

/**
 * Connection to mysql database
 */
class ConnectionSwooleMysql extends AbstractConnection
{
    const MAX_CONNECTIONS = 100;

    /** @var PdoMysqlPool */
    public $pool;

    /**
     * Get fallback connection type if executed from symfony console
     * @return string
     */
    public static function getFallbackForCli()
    {
        return "mysql";
    }

    /**
     * Create mysql object, use existing if exists and connect
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
            $this->pool = new Pool(new MysqlClientManager(
                $this->database,
                $this->host,
                $this->encoding,
                $this->user,
                $this->password
            ), self::MAX_CONNECTIONS);
        }

        return $this->pool;
    }

    /**
     * Get next pdo in pool
     * @return \PDO
     */
    public function getPdo()
    {
        if (Coroutine::getuid() === -1) {
            $result = null;
            \Co\run(function() use($forceReconnect, &$result) {
                $result = $this->pool->get();
            });
            return $result;
        }

        return $this->pool->get();
    }

    /**
     * Execute sql instruction
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

        // Get connection
        if ($forceConnection != null && $forceConnection instanceof \PDO) {
            $pdo = $forceConnection;
        } else {
            /** @var \PDO $pdo */
            $pdo = $this->pool->get();
        }

        // Execute
        if (!in_array(strtolower(explode(" ", trim($sql))[0]), ["insert", "update"])) {
            $statement = $pdo->prepare($sql);

            foreach ($params as $param => $value) {
                $statement->bindValue(":" . $param, $value);
            }
            if ($statement->execute()) {
                $result = $statement->fetchAll(\PDO::FETCH_ASSOC);
            } else {
                throw new ConnectionException("Fail to execute request : SQLSTATE[" . $errInfo[0] . "][" . $errInfo[1] . "] " . $errInfo[2]);
            }
        } elseif (strtolower(explode(" ", trim($sql))[0]) == "insert") {
            $statement = $pdo->prepare($sql);

            foreach ($params as $param => $value) {
                $statement->bindValue(":" . $param, $value);
            }
            if ($statement->execute()) {
                $result = $statement->fetchAll(\PDO::FETCH_ASSOC);
            } else {
                throw new ConnectionException("Fail to execute request : SQLSTATE[" . $errInfo[0] . "][" . $errInfo[1] . "] " . $errInfo[2]);
            }
            $result = (int)$pdo->lastInsertId();
        } else {
            $statement = $pdo->prepare($sql);

            foreach ($params as $param => $value) {
                $statement->bindValue(":" . $param, $value);
            }
            if ($statement->execute()) {
                $result = $statement->fetchAll(\PDO::FETCH_ASSOC);
            } else {
                throw new ConnectionException("Fail to execute request : SQLSTATE[" . $errInfo[0] . "][" . $errInfo[1] . "] " . $errInfo[2]);
            }
            $result = null;
        }

        // Release connection
        $this->pool->put($pdo);

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
        throw new Exception("Not yet supported");
    }

    /**
     * Commit transaction
     * @return $this
     * @throws ConnectionException
     * @throws TransactionException
     */
    public function commit()
    {
        throw new Exception("Not yet supported");
    }

    /**
     * Rollback transaction
     * @return $this
     * @throws ConnectionException
     * @throws TransactionException
     */
    public function rollback()
    {
        throw new Exception("Not yet supported");
    }

    /**
     * Get last insert id
     * @return int
     */
    public function lastInsertId()
    {
        throw new Exception("Not yet supported");
    }
}
