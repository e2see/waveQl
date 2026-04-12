<?php

declare(strict_types=1);

namespace e2;

/**
 * =================================================================================================
 * waveQlWrite – Logic for INSERT, UPDATE, DELETE (Write)
 * =================================================================================================
 *
 * This class extends waveQlCore and enables inserting, updating and deleting records.
 * It works on the principle: The presence of the uniqueKey field in the values array
 * decides between INSERT (missing) or UPDATE (present). Additional options:
 *
 *   - 'returning'    : Returns the complete record after INSERT/UPDATE.
 *   - 'safe'         : Prevents DELETE without WHERE condition.
 *   - 'updatePrimaryKey' : Allows changing the primary key (disabled by default).
 *
 * -------------------------------------------------------------------------------------------------
 * Usage:
 *   $write = $wave->write();
 *
 *   // INSERT
 *   $newId = $write->setMeta(['uniqueKey' => 'id'])
 *                  ->setValues(['name' => 'Max', 'age' => 30])
 *                  ->execute();
 *
 *   // UPDATE
 *   $affected = $write->setMeta(['uniqueKey' => 'id'])
 *                     ->setValues(['id' => 42, 'name' => 'Max (updated)'])
 *                     ->execute();
 *
 *   // DELETE
 *   $deleted = $write->setMeta(['uniqueKey' => 'id', 'safe' => true])
 *                    ->setValues(['id' => 42])
 *                    ->delete();
 *
 *   $sql = $write->getQuery();
 * -------------------------------------------------------------------------------------------------
 *
 * =================================================================================================
 */
class waveQlWrite extends waveQlCore
{
    protected array $meta             = [];
    protected array $values           = [];
    protected const ALLOWED_META_KEYS = [
        'uniqueKey',
        'returning',
        'safe',
        'updatePrimaryKey',
    ];

    ########################### CONSTRUCTOR

    public function __construct(waveQlDbInterface $db, array $tableManifest, array $keyManifest, array $options = [])
    {
        parent::__construct($db, $tableManifest, $keyManifest, $options);
    }

    ########################### API METHODS

    ##### Sets meta information (uniqueKey, returning, safe, updatePrimaryKey).
    public function setMeta(array $meta): self
    {
        $unknown = array_diff(array_keys($meta), static::ALLOWED_META_KEYS);
        if (!empty($unknown)) {
            $this->handleError('invalidArgument', 'Unknown meta key(s): ' . implode(', ', $unknown));
        }
        $this->meta = $meta;
        return $this;
    }

    ##### Sets the values for INSERT/UPDATE/DELETE.
    public function setValues(array $values): self
    {
        $this->values = $values;
        return $this;
    }

    ##### Executes INSERT or UPDATE (depending on uniqueKey).
    public function execute(): int|array|null
    {
        $uniqueKey = $this->meta['uniqueKey'] ?? null;
        if (!$uniqueKey) {
            $this->handleError('meta', 'For write operations uniqueKey must be set in meta.');
        }

        if (isset($this->values[$uniqueKey])) {
            return $this->update();
        } else {
            return $this->insert();
        }
    }

    ##### Executes an INSERT (always, regardless of uniqueKey).
    public function insert(): int|array
    {
        $uniqueKey = $this->meta['uniqueKey'] ?? null;
        if (!$uniqueKey) {
            $this->handleError('meta', 'For INSERT uniqueKey must be set in meta.');
        }

        $fields       = [];
        $placeholders = [];
        $params       = [];
        $types        = '';

        //-- Build columns and placeholders from field definitions
        foreach ($this->values as $field => $value) {
            $detail = $this->getFieldDetail($field);
            if (!$detail) {
                $this->handleError('invalidArgument', 'Field ' . $field . ' not defined in keyManifest.');
            }
            $fields[]        = $detail['fullQuoted'];
            $placeholders[]  = '?';
            $params[]        = $value;
            $types          .= $this->typeToBindParam($this->keyManifest[$field]['type']);
        }

        $tableDetail = $this->getTableDetail($this->tableManifest['tableName'] ?? '');
        if (!$tableDetail) {
            $this->handleError('meta', 'No table defined.');
        }
        $table = $tableDetail['nameQuoted'];

        $sql = 'INSERT INTO ' . $table . ' (' . implode(', ', $fields) . ') VALUES (' . implode(', ', $placeholders) . ')';

        $stmt = $this->db->prepare($sql);
        if (!$stmt) {
            $this->handleError('query', 'Prepare failed: ' . $this->db->error());
        }

        $this->db->execute($stmt, $params, $types);
        $newId = $this->db->lastInsertId();

        if (!empty($this->meta['returning'])) {
            return $this->fetchReturning($newId);
        }

        return $newId;
    }

    ##### Executes an UPDATE.
    public function update(): int|array
    {
        $uniqueKey = $this->meta['uniqueKey'] ?? null;
        if (!$uniqueKey) {
            $this->handleError('meta', 'For UPDATE uniqueKey must be set in meta.');
        }

        $idValue = $this->values[$uniqueKey] ?? null;
        if ($idValue === null) {
            $this->handleError('invalidArgument', 'uniqueKey ' . $uniqueKey . ' not present in values.');
        }

        $setParts         = [];
        $params           = [];
        $types            = '';
        $updatePrimaryKey = $this->meta['updatePrimaryKey'] ?? false;

        //-- Build SET part, optionally skip uniqueKey
        foreach ($this->values as $logicalName => $value) {
            if ($logicalName === $uniqueKey && !$updatePrimaryKey) {
                continue;
            }
            $detail = $this->getFieldDetail($logicalName);
            if (!$detail) {
                $this->handleError('invalidArgument', 'Field ' . $logicalName . ' not defined in keyManifest.');
            }
            $setParts[] = $detail['fullQuoted'] . ' = ?';
            $params[]   = $value;
            $types     .= $this->typeToBindParam($this->keyManifest[$logicalName]['type']);
        }

        if (empty($setParts)) {
            return 0;
        }

        //-- WHERE part
        $idDetail = $this->getFieldDetail($uniqueKey);
        if (!$idDetail) {
            $this->handleError('invalidArgument', 'uniqueKey ' . $uniqueKey . ' not defined in keyManifest.');
        }

        $tableDetail = $this->getTableDetail($this->tableManifest['tableName'] ?? '');
        if (!$tableDetail) {
            $this->handleError('meta', 'No table defined.');
        }
        $table = $tableDetail['nameQuoted'];

        $sql = 'UPDATE ' . $table . ' SET ' . implode(', ', $setParts)
            . ' WHERE ' . $idDetail['fullQuoted'] . ' = ?';
        $params[] = $idValue;
        $types   .= $this->typeToBindParam($this->keyManifest[$uniqueKey]['type']);

        $stmt = $this->db->prepare($sql);
        if (!$stmt) {
            $this->handleError('query', 'Prepare failed: ' . $this->db->error());
        }

        $this->db->execute($stmt, $params, $types);
        $affected = $this->db->affectedRows();

        if (!empty($this->meta['returning'])) {
            return $this->fetchReturning($idValue);
        }

        return $affected;
    }

    ##### Executes a DELETE.
    public function delete(): int
    {
        $uniqueKey = $this->meta['uniqueKey'] ?? null;
        if (!$uniqueKey) {
            $this->handleError('meta', 'For DELETE uniqueKey must be set in meta.');
        }
        $safe = $this->meta['safe'] ?? false;
        if ($safe && empty($this->values)) {
            $this->handleError('meta', 'Safe mode: DELETE without WHERE condition not allowed.');
        }

        $idValue = $this->values[$uniqueKey] ?? null;
        if ($idValue === null) {
            $this->handleError('invalidArgument', 'uniqueKey ' . $uniqueKey . ' not present in values.');
        }

        $idDetail = $this->getFieldDetail($uniqueKey);
        if (!$idDetail) {
            $this->handleError('invalidArgument', 'uniqueKey ' . $uniqueKey . ' not defined in keyManifest.');
        }

        $tableDetail = $this->getTableDetail($this->tableManifest['tableName'] ?? '');
        if (!$tableDetail) {
            $this->handleError('meta', 'No table defined.');
        }
        $table = $tableDetail['nameQuoted'];

        $sql = 'DELETE FROM ' . $table . ' WHERE ' . $idDetail['fullQuoted'] . ' = ?';
        $params = [$idValue];
        $types = $this->typeToBindParam($this->keyManifest[$uniqueKey]['type']);

        $stmt = $this->db->prepare($sql);
        if (!$stmt) {
            $this->handleError('query', 'Prepare failed: ' . $this->db->error());
        }

        $this->db->execute($stmt, $params, $types);
        return $this->db->affectedRows();
    }

    ##### Returns the SQL query (for debugging) – either INSERT or UPDATE.
    public function getQuery(): string|false
    {
        $uniqueKey = $this->meta['uniqueKey'] ?? null;
        if (!$uniqueKey) return false;

        if (isset($this->values[$uniqueKey])) {
            return $this->buildUpdateQuery();
        } else {
            return $this->buildInsertQuery();
        }
    }

    ########################### INTERNAL HELPERS

    ##### Fetches the complete record after INSERT/UPDATE (returning).
    protected function fetchReturning($id)
    {
        $uniqueKey = $this->meta['uniqueKey'] ?? null;
        if (!$uniqueKey) {
            return $id;
        }

        $read = new waveQlRead($this->db, $this->tableManifest, $this->keyManifest);
        $read->setValues([$uniqueKey => $id]);
        $rows = $read->execute();
        return $rows[0] ?? null;
    }

    ##### Builds an INSERT query as a string (for debugging).
    protected function buildInsertQuery(): string|false
    {
        $fields = [];
        $values = [];
        foreach ($this->values as $logicalName => $value) {
            $detail = $this->getFieldDetail($logicalName);
            if (!$detail) continue;
            $fields[] = $detail['fullQuoted'];
            $values[] = "'" . $this->db->escape((string)$value) . "'";
        }
        if (empty($fields)) return false;

        $tableDetail = $this->getTableDetail($this->tableManifest['tableName'] ?? '');
        if (!$tableDetail) return false;
        $table = $tableDetail['nameQuoted'];

        return 'INSERT INTO ' . $table . ' (' . implode(', ', $fields) . ') VALUES (' . implode(', ', $values) . ')';
    }

    ##### Builds an UPDATE query as a string (for debugging).
    protected function buildUpdateQuery(): string|false
    {
        $uniqueKey = $this->meta['uniqueKey'] ?? null;
        if (!$uniqueKey) return false;
        $idValue = $this->values[$uniqueKey] ?? null;
        if ($idValue === null) return false;

        $setParts = [];
        foreach ($this->values as $logicalName => $value) {
            if ($logicalName === $uniqueKey) continue;
            $detail = $this->getFieldDetail($logicalName);
            if (!$detail) continue;
            $setParts[] = $detail['fullQuoted'] . " = '" . $this->db->escape((string)$value) . "'";
        }
        if (empty($setParts)) return false;

        $idDetail = $this->getFieldDetail($uniqueKey);
        if (!$idDetail) return false;

        $tableDetail = $this->getTableDetail($this->tableManifest['tableName'] ?? '');
        if (!$tableDetail) return false;
        $table = $tableDetail['nameQuoted'];

        return 'UPDATE ' . $table . ' SET ' . implode(', ', $setParts) . ' WHERE ' . $idDetail['fullQuoted'] . " = '" . $this->db->escape((string)$idValue) . "'";
    }
}
