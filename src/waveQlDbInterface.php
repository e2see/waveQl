<?php

declare(strict_types=1);

namespace e2;

/**
 * =================================================================================================
 * waveQlDbInterface – Contract for database adapters
 * =================================================================================================
 *
 * This interface defines all methods that an adapter must provide
 * to work with waveQl. This allows different database libraries
 * (e.g. mysqli, PDO) to be swapped without changing the core logic.
 *
 * Each adapter must:
 *   - Execute queries (prepared or direct)
 *   - Escape strings
 *   - Return the last inserted ID and the number of affected rows
 *   - Return error messages
 *   - Correctly quote identifiers (table/column names)
 *
 * -------------------------------------------------------------------------------------------------
 * Example (with mysqli):
 *   $adapter = new dbAdapterMysqli($mysqli);
 *   $result = $adapter->query('SELECT * FROM users');
 *   $data = $adapter->fetchAll($result);
 * -------------------------------------------------------------------------------------------------
 *
 * =================================================================================================
 */
interface waveQlDbInterface
{
    ##### Executes a query directly (not prepared).
    public function query(string $sql): mixed;

    ##### Prepares an SQL statement.
    public function prepare(string $sql): mixed;

    ##### Executes a prepared statement with parameters.
    public function execute(mixed $stmt, array $params, string $types): bool;

    ##### Escapes a string for direct insertion into SQL.
    public function escape(string $value): string;

    ##### Returns the last inserted ID.
    public function lastInsertId(): int|string;

    ##### Returns the number of affected rows of the last operation.
    public function affectedRows(): int;

    ##### Fetches all rows from a result (as associative array).
    public function fetchAll(mixed $result): array;

    ##### Returns the last error message.
    public function error(): string;

    ##### Checks whether the prepared statement returns a result set (SELECT etc.).
    public function isResultSet(mixed $stmt): bool;

    ##### Quotes an identifier (table, column name) for the concrete database.
    public function quoteIdentifier(string $name, bool $splitDot = false): string;
}
