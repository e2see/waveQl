<?php

declare(strict_types=1);

namespace e2;

/**
 * =================================================================================================
 * waveQlWrite – Logik für INSERT, UPDATE, DELETE (Write)
 * =================================================================================================
 *
 * Diese Klasse erweitert waveQlCore und ermöglicht das Einfügen, Aktualisieren und Löschen von
 * Datensätzen. Sie arbeitet nach dem Prinzip: Das Vorhandensein des uniqueKey‑Feldes im Werte‑Array
 * entscheidet über INSERT (fehlt) oder UPDATE (vorhanden). Zusätzliche Optionen:
 *
 *   - 'returning'    : Liefert nach INSERT/UPDATE den kompletten Datensatz zurück.
 *   - 'safe'         : Verhindert DELETE ohne WHERE‑Bedingung.
 *   - 'updatePrimaryKey' : Erlaubt die Änderung des Primärschlüssels (standardmäßig deaktiviert).
 *
 * -------------------------------------------------------------------------------------------------
 * Verwendung:
 *   $write = $wave->write();
 *
 *   // INSERT
 *   $newId = $write->setMeta(['uniqueKey' => 'id'])
 *                  ->setValues(['name' => 'Max', 'age' => 30])
 *                  ->execute();
 *
 *   // UPDATE
 *   $affected = $write->setMeta(['uniqueKey' => 'id'])
 *                     ->setValues(['id' => 42, 'name' => 'Max (aktualisiert)'])
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
    protected array $meta   = [];
    protected array $values = [];


    ########################### KONSTRUKTOR


    public function __construct(waveQlDbInterface $db, array $tableManifest, array $keyManifest, array $options = [])
    {
        parent::__construct($db, $tableManifest, $keyManifest, $options);
    }


    ########################### API-METHODEN


    ##### Setzt Meta‑Informationen (uniqueKey, returning, safe, updatePrimaryKey)
    public function setMeta(array $meta): self
    {
        $this->meta = $meta;
        return $this;
    }


    ##### Setzt die Werte für INSERT/UPDATE/DELETE
    public function setValues(array $values): self
    {
        $this->values = $values;
        return $this;
    }


    ##### Führt INSERT oder UPDATE aus (abhängig vom uniqueKey)
    public function execute(): int|array|null
    {
        $uniqueKey = $this->meta['uniqueKey'] ?? null;
        if (!$uniqueKey) {
            throw new waveQlMetaException('Für Write-Operationen muss uniqueKey in Meta gesetzt sein.');
        }

        if (isset($this->values[$uniqueKey])) {
            return $this->update();
        } else {
            return $this->insert();
        }
    }


    ##### Führt ein INSERT aus (immer, unabhängig vom uniqueKey)
    public function insert(): int|array
    {
        $uniqueKey = $this->meta['uniqueKey'] ?? null;
        if (!$uniqueKey) {
            throw new waveQlMetaException('Für INSERT muss uniqueKey in Meta gesetzt sein.');
        }

        $fields       = [];
        $placeholders = [];
        $params       = [];
        $types        = '';

        //-- Spalten und Platzhalter aus den Felddefinitionen bauen
        foreach ($this->values as $field => $value) {
            $detail = $this->getFieldDetail($field);
            if (!$detail) {
                throw new waveQlInvalidArgumentException("Feld '$field' nicht in keyManifest definiert.");
            }
            $fields[]        = $detail['fullQuoted'];
            $placeholders[]  = '?';
            $params[]        = $value;
            $types          .= $this->typeToBindParam($this->keyManifest[$field]['type']);
        }

        $tableDetail = $this->getTableDetail($this->tableManifest['tableName'] ?? '');
        if (!$tableDetail) {
            throw new waveQlMetaException('Keine Tabelle definiert.');
        }
        $table = $tableDetail['nameQuoted'];

        $sql = 'INSERT INTO ' . $table . ' (' . implode(', ', $fields) . ') VALUES (' . implode(', ', $placeholders) . ')';

        $stmt = $this->db->prepare($sql);
        if (!$stmt) {
            throw new waveQlQueryException('Prepare fehlgeschlagen: ' . $this->db->error());
        }

        $this->db->execute($stmt, $params, $types);
        $newId = $this->db->lastInsertId();

        if (!empty($this->meta['returning'])) {
            return $this->fetchReturning($newId);
        }

        return $newId;
    }


    ##### Führt ein UPDATE aus
    public function update(): int|array
    {
        $uniqueKey = $this->meta['uniqueKey'] ?? null;
        if (!$uniqueKey) {
            throw new waveQlMetaException('Für UPDATE muss uniqueKey in Meta gesetzt sein.');
        }

        $idValue = $this->values[$uniqueKey] ?? null;
        if ($idValue === null) {
            throw new waveQlInvalidArgumentException("uniqueKey '$uniqueKey' nicht in values vorhanden.");
        }

        $setParts         = [];
        $params           = [];
        $types            = '';
        $updatePrimaryKey = $this->meta['updatePrimaryKey'] ?? false;

        //-- SET‑Teil aufbauen, ggf. uniqueKey überspringen
        foreach ($this->values as $logicalName => $value) {
            if ($logicalName === $uniqueKey && !$updatePrimaryKey) {
                continue;
            }
            $detail = $this->getFieldDetail($logicalName);
            if (!$detail) {
                throw new waveQlInvalidArgumentException("Feld '$logicalName' nicht in keyManifest definiert.");
            }
            $setParts[] = $detail['fullQuoted'] . ' = ?';
            $params[]   = $value;
            $types     .= $this->typeToBindParam($this->keyManifest[$logicalName]['type']);
        }

        if (empty($setParts)) {
            return 0;
        }

        //-- WHERE‑Teil
        $idDetail = $this->getFieldDetail($uniqueKey);
        if (!$idDetail) {
            throw new waveQlInvalidArgumentException("uniqueKey '$uniqueKey' nicht in keyManifest definiert.");
        }

        $tableDetail = $this->getTableDetail($this->tableManifest['tableName'] ?? '');
        if (!$tableDetail) {
            throw new waveQlMetaException('Keine Tabelle definiert.');
        }
        $table = $tableDetail['nameQuoted'];

        $sql = 'UPDATE ' . $table . ' SET ' . implode(', ', $setParts)
            . ' WHERE ' . $idDetail['fullQuoted'] . ' = ?';
        $params[] = $idValue;
        $types   .= $this->typeToBindParam($this->keyManifest[$uniqueKey]['type']);

        $stmt = $this->db->prepare($sql);
        if (!$stmt) {
            throw new waveQlQueryException('Prepare fehlgeschlagen: ' . $this->db->error());
        }

        $this->db->execute($stmt, $params, $types);
        $affected = $this->db->affectedRows();

        if (!empty($this->meta['returning'])) {
            return $this->fetchReturning($idValue);
        }

        return $affected;
    }


    ##### Führt ein DELETE aus
    public function delete(): int
    {
        $uniqueKey = $this->meta['uniqueKey'] ?? null;
        if (!$uniqueKey) {
            throw new waveQlMetaException('Für DELETE muss uniqueKey in Meta gesetzt sein.');
        }
        $safe = $this->meta['safe'] ?? false;
        if ($safe && empty($this->values)) {
            throw new waveQlMetaException('Sicherheitsmodus: DELETE ohne WHERE-Bedingung nicht erlaubt.');
        }

        $idValue = $this->values[$uniqueKey] ?? null;
        if ($idValue === null) {
            throw new waveQlInvalidArgumentException("uniqueKey '$uniqueKey' nicht in values vorhanden.");
        }

        $idDetail = $this->getFieldDetail($uniqueKey);
        if (!$idDetail) {
            throw new waveQlInvalidArgumentException("uniqueKey '$uniqueKey' nicht in keyManifest definiert.");
        }

        $tableDetail = $this->getTableDetail($this->tableManifest['tableName'] ?? '');
        if (!$tableDetail) {
            throw new waveQlMetaException('Keine Tabelle definiert.');
        }
        $table = $tableDetail['nameQuoted'];

        $sql = 'DELETE FROM ' . $table . ' WHERE ' . $idDetail['fullQuoted'] . ' = ?';
        $params = [$idValue];
        $types = $this->typeToBindParam($this->keyManifest[$uniqueKey]['type']);

        $stmt = $this->db->prepare($sql);
        if (!$stmt) {
            throw new waveQlQueryException('Prepare fehlgeschlagen: ' . $this->db->error());
        }

        $this->db->execute($stmt, $params, $types);
        return $this->db->affectedRows();
    }


    ##### Liefert die SQL‑Query (für Debug) – entweder INSERT oder UPDATE
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


    ########################### INTERNE HELFER


    ##### Holt nach INSERT/UPDATE den kompletten Datensatz zurück (returning)
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


    ##### Baut eine INSERT‑Query als String (für Debug)
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


    ##### Baut eine UPDATE‑Query als String (für Debug)
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