<?php
declare(strict_types=1);

namespace e2;

/**
 * =================================================================================================
 * waveQlDbInterface – Vertrag für Datenbankadapter
 * =================================================================================================
 *
 * Diese Schnittstelle definiert alle Methoden, die ein Adapter bereitstellen muss,
 * um mit waveQl zusammenzuarbeiten. Dadurch können unterschiedliche Datenbank‑Bibliotheken
 * (z. B. mysqli, PDO) ausgetauscht werden, ohne die Kernlogik zu ändern.
 *
 * Jeder Adapter muss:
 *   - Queries ausführen (vorbereitet oder direkt)
 *   - Zeichenketten escapen
 *   - die letzte eingefügte ID und die Anzahl betroffener Zeilen liefern
 *   - Fehlermeldungen zurückgeben
 *   - Identifier (Tabellen‑/Spaltennamen) korrekt quoten
 *
 * -------------------------------------------------------------------------------------------------
 * Beispiel (mit mysqli):
 *   $adapter = new dbAdapterMysqli($mysqli);
 *   $result = $adapter->query('SELECT * FROM users');
 *   $data = $adapter->fetchAll($result);
 * -------------------------------------------------------------------------------------------------
 *
 * =================================================================================================
 */
interface waveQlDbInterface
{
    ##### Führt eine Query direkt aus (nicht vorbereitet).
    public function query(string $sql): mixed;

    ##### Bereitet eine SQL-Anweisung vor.
    public function prepare(string $sql): mixed;

    ##### Führt ein vorbereitetes Statement mit Parametern aus.
    public function execute(mixed $stmt, array $params, string $types): bool;

    ##### Escaped einen String für den direkten Einbau in SQL.
    public function escape(string $value): string;

    ##### Liefert die letzte eingefügte ID.
    public function lastInsertId(): int|string;

    ##### Liefert die Anzahl betroffener Zeilen der letzten Operation.
    public function affectedRows(): int;

    ##### Holt alle Zeilen aus einem Ergebnis (als assoziatives Array).
    public function fetchAll(mixed $result): array;

    ##### Liefert die letzte Fehlermeldung.
    public function error(): string;

    ##### Prüft, ob das vorbereitete Statement ein Resultset (SELECT o.ä.) zurückgibt.
    public function isResultSet(mixed $stmt): bool;

    ##### Quotiert einen Identifier (Tabellen‑, Spaltenname) für die konkrete Datenbank.
    public function quoteIdentifier(string $name, bool $splitDot = false): string;
}