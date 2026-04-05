<?php
declare(strict_types=1);

namespace e2;

/**
 * =================================================================================================
 * dbAdapterMysqli – Concrete implementation of the adapter for the mysqli extension
 * =================================================================================================
 *
 * This adapter encapsulates all mysqli‑specific calls and implements the waveQlDbInterface.
 * It is automatically created by the factory (waveQl::create()) when a mysqli instance is passed.
 *
 * -------------------------------------------------------------------------------------------------
 * Usage:
 *   $mysqli  = new mysqli('localhost', 'user', 'pass', 'db');
 *   $adapter = new dbAdapterMysqli($mysqli);
 *   $result  = $adapter->query('SELECT * FROM users');
 *   $rows    = $adapter->fetchAll($result);
 * -------------------------------------------------------------------------------------------------
 *
 * =================================================================================================
 */
class dbAdapterMysqli implements waveQlDbInterface
{
    private \mysqli $db;

    ########################### CONSTRUCTOR

    public function __construct(\mysqli $db)
    {
        $this->db = $db;
    }

    ########################### QUERY EXECUTION

    ##### Executes a query directly.
    public function query(string $sql): mixed
    {
        $result = $this->db->query($sql);
        if ($result === false) {
            throw new waveQlQueryException('Query failed: ' . $this->db->error);
        }
        return $result;
    }

    ##### Prepares an SQL statement.
    public function prepare(string $sql): mixed
    {
        $stmt = $this->db->prepare($sql);
        if ($stmt === false) {
            throw new waveQlQueryException('Prepare failed: ' . $this->db->error);
        }
        return $stmt;
    }

    ##### Executes a prepared statement.
    public function execute(mixed $stmt, array $params, string $types): bool
    {
        if (!($stmt instanceof \mysqli_stmt)) {
            throw new waveQlInvalidArgumentException('Statement must be mysqli_stmt');
        }
        //-- Bind parameters if any
        if (!empty($params)) {
            $stmt->bind_param($types, ...$params);
        }
        $success = $stmt->execute();
        if (!$success) {
            throw new waveQlQueryException('Execute failed: ' . $stmt->error);
        }
        return $success;
    }

    ########################### HELPER METHODS

    public function escape(string $value): string
    {
        return $this->db->real_escape_string($value);
    }

    public function lastInsertId(): int|string
    {
        return $this->db->insert_id;
    }

    public function affectedRows(): int
    {
        return $this->db->affected_rows;
    }

    ##### Fetches all rows of a result.
    public function fetchAll(mixed $result): array
    {
        if ($result instanceof \mysqli_result) {
            return $result->fetch_all(MYSQLI_ASSOC);
        }
        if ($result instanceof \mysqli_stmt) {
            $result = $result->get_result();
            if ($result) {
                return $result->fetch_all(MYSQLI_ASSOC);
            }
        }
        return [];
    }

    public function error(): string
    {
        return $this->db->error;
    }

    ##### Checks whether a prepared statement returns a result set.
    public function isResultSet(mixed $stmt): bool
    {
        if (!($stmt instanceof \mysqli_stmt)) {
            return false;
        }
        return (bool) $stmt->result_metadata();
    }

    ##### Quotes an identifier for MySQL (backticks) and splits at dot.
    public function quoteIdentifier(string $name, bool $splitDot = false): string
    {
        $name = trim($name);
        if ($name === '') return '';

        //-- Function calls (contain parentheses) are not quoted
        if (strpos($name, '(') !== false && strpos($name, ')') !== false) {
            return $name;
        }

        //-- Dot notation quote each part
        if ($splitDot || strpos($name, '.') !== false) {
            $parts = explode('.', $name);
            $quoted = [];
            foreach ($parts as $part) {
                $part = trim($part);
                if ($part !== '') {
                    $quoted[] = '`' . $part . '`';
                }
            }
            return implode('.', $quoted);
        }

        //-- Simple identifier
        return '`' . $name . '`';
    }
}