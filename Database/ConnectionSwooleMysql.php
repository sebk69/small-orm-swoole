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

use Sebk\SmallOrmSwoole\Pool\Connection;
use Sebk\SmallOrmSwoole\Pool\PdoMysqlPool;
use Swoole\Database\PDOConfig;
use Swoole\Database\PDOPool;

/**
 * Connection to mysql database
 */
class ConnectionSwooleMysql extends AbstractConnection
{
    const MAX_CONNECTIONS = 1000;

    /** @var PdoMysqlPool */
    private $pool;

    /**
     * Create mysql object, use existing if exists and connect
     * @throws ConnectionException
     */
    public function connect($forceReconnect = false)
    {
        if ($this->pool == null) {
            $this->pool = new PDOPool(
                (new PDOConfig())
                    ->withHost($this->host)
                    ->withPort(3306)
                    // ->withUnixSocket('/tmp/mysql.sock')
                    ->withDbName($this->database)
                    ->withCharset($this->encoding)
                    ->withUsername($this->user)
                    ->withPassword($this->password)
                    ->withOptions([\PDO::ATTR_PERSISTENT => true]),
                static::MAX_CONNECTIONS
            );
        }

        return $this->pool;
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
        $this->connect();

        // Get connection
        /** @var \PDO $pdo */
        $pdo = $this->pool->get();

        // Execute
        if (!in_array(strtolower(explode(" ", trim($sql))[0]), "insert", "update")) {
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
        $pdo->

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
