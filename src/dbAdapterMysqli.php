<?php
declare(strict_types=1);

namespace e2;

/**
 * =================================================================================================
 * dbAdapterMysqli – Konkrete Implementierung des Adapters für die mysqli‑Erweiterung
 * =================================================================================================
 *
 * Dieser Adapter kapselt alle mysqli‑spezifischen Aufrufe und setzt das waveQlDbInterface um.
 * Er wird von der Factory (waveQl::create()) automatisch erzeugt, wenn eine mysqli‑Instanz
 * übergeben wird.
 *
 * -------------------------------------------------------------------------------------------------
 * Verwendung:
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

    ########################### KONSTRUKTOR

    public function __construct(\mysqli $db)
    {
        $this->db = $db;
    }

    ########################### QUERY-AUSFÜHRUNG

    ##### Führt eine Query direkt aus.
    public function query(string $sql): mixed
    {
        $result = $this->db->query($sql);
        if ($result === false) {
            throw new waveQlQueryException('Query fehlgeschlagen: ' . $this->db->error);
        }
        return $result;
    }

    ##### Bereitet eine SQL-Anweisung vor.
    public function prepare(string $sql): mixed
    {
        $stmt = $this->db->prepare($sql);
        if ($stmt === false) {
            throw new waveQlQueryException('Prepare fehlgeschlagen: ' . $this->db->error);
        }
        return $stmt;
    }

    ##### Führt ein vorbereitetes Statement aus.
    public function execute(mixed $stmt, array $params, string $types): bool
    {
        if (!($stmt instanceof \mysqli_stmt)) {
            throw new waveQlInvalidArgumentException('Statement must be mysqli_stmt');
        }
        //-- Parameter binden, falls vorhanden
        if (!empty($params)) {
            $stmt->bind_param($types, ...$params);
        }
        $success = $stmt->execute();
        if (!$success) {
            throw new waveQlQueryException('Execute fehlgeschlagen: ' . $stmt->error);
        }
        return $success;
    }

    ########################### HILFSMETHODEN

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

    ##### Holt alle Zeilen eines Ergebnisses.
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

    ##### Prüft, ob ein vorbereitetes Statement ein Resultset liefert.
    public function isResultSet(mixed $stmt): bool
    {
        if (!($stmt instanceof \mysqli_stmt)) {
            return false;
        }
        return (bool) $stmt->result_metadata();
    }

    ##### Quotiert einen Identifier für MySQL (Backticks) und splittet bei Punkt.
    public function quoteIdentifier(string $name, bool $splitDot = false): string
    {
        $name = trim($name);
        if ($name === '') return '';

        //-- Funktionsaufrufe (enthalten Klammern) nicht quoten
        if (strpos($name, '(') !== false && strpos($name, ')') !== false) {
            return $name;
        }

        //-- Punkt‑Notation einzeln quoten
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

        //-- Einfacher Bezeichner
        return '`' . $name . '`';
    }
}