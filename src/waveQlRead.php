<?php
declare(strict_types=1);

namespace e2;

/**
 * =================================================================================================
 * waveQlRead – Logik für SELECT‑Abfragen (Read)
 * =================================================================================================
 *
 * Diese Klasse erweitert waveQlCore und implementiert alle Funktionen zum Erzeugen und Ausführen
 * von SELECT‑Queries. Sie verarbeitet die über setValues() übergebenen Suchkriterien inklusive
 * der eingebetteten Operatoren (>, <, ~, !NULL, BLANK, EMPTY, Bereichsoperatoren) und setzt sie
 * in WHERE‑Bedingungen um. Paginierung, Sortierung und Volltextsuche werden über setMeta()
 * gesteuert.
 *
 * -------------------------------------------------------------------------------------------------
 * Verwendung:
 *   $read = $wave->read();
 *   $read->setMeta(['sort' => '>name', 'pageSize' => 10])
 *        ->setValues(['age' => '>18', 'name' => '~Müller~'])
 *        ->execute();
 *
 *   $count = $read->count();
 *   $exists = $read->exists();
 *   $sql = $read->getQuery();
 * -------------------------------------------------------------------------------------------------
 *
 * =================================================================================================
 */
class waveQlRead extends waveQlCore
{
    protected bool $usePrepared;

    ########################### KONSTRUKTOR

    public function __construct(waveQlDbInterface $db, array $tableManifest, array $keyManifest, array $options = [])
    {
        parent::__construct($db, $tableManifest, $keyManifest, $options);
        $this->usePrepared = $options['prepared'] ?? false;
    }

    ########################### API-METHODEN

    ##### Setzt Meta‑Informationen (Sortierung, Paginierung, Suche, benutzerdefinierte SQL)
    public function setMeta(array $meta): self
    {
        $this->updateLive(null, $meta, false, true);
        return $this;
    }

    ##### Setzt die Suchwerte (mit optionalen Operatoren)
    public function setValues(array $values): self
    {
        $this->updateLive($values, null, true, false);
        return $this;
    }

    ##### Führt die Query aus und liefert das Ergebnis als Array
    public function execute(): array|int
    {
        if ($this->usePrepared) {
            $prep = $this->getPreparedQuery();
            if (!$prep) {
                throw new waveQlQueryException('Keine gültige Query.');
            }
            $stmt = $this->db->prepare($prep['query']);
            if (!$stmt) {
                throw new waveQlQueryException('Prepare fehlgeschlagen: ' . $this->db->error());
            }
            if (!empty($prep['params'])) {
                $this->db->execute($stmt, $prep['params'], $prep['types']);
            } else {
                $stmt->execute();
            }
            //-- Ergebnis abholen
            if ($this->db->isResultSet($stmt)) {
                $data = $this->db->fetchAll($stmt);
                return $data;
            }
            return $this->db->affectedRows();
        } else {
            $query = $this->getQuery();
            if (!$query) {
                throw new waveQlQueryException('Keine gültige Query.');
            }
            $result = $this->db->query($query);
            if (!$result) {
                throw new waveQlQueryException('Query fehlgeschlagen: ' . $this->db->error());
            }
            return $this->db->fetchAll($result);
        }
    }

    ##### Liefert die Anzahl der Treffer (ohne LIMIT)
    public function count(bool $total = false): int
    {
        $query = $this->getCountQuery($total);
        if (!$query) {
            throw new waveQlQueryException('Keine gültige Query.');
        }
        $result = $this->db->query($query);
        if (!$result) {
            throw new waveQlQueryException('Query fehlgeschlagen: ' . $this->db->error());
        }
        $rows = $this->db->fetchAll($result);
        if (empty($rows)) {
            throw new waveQlQueryException('Count query returned no rows.');
        }
        return (int)($rows[0]['count'] ?? 0);
    }

    ##### Prüft, ob mindestens ein Treffer existiert
    public function exists(): bool
    {
        return $this->count() > 0;
    }

    ##### Liefert die vollständige SQL‑Query (für Debug)
    public function getQuery(): string|false
    {
        $limitless = $this->getLimitlessQuery();
        return $limitless ? $limitless . $this->getOrderQuery() . $this->getLimitQuery() : false;
    }

    ########################### OPERATOR-PARSING

    ##### Parst den Wert eines Feldes und ermittelt die verwendeten Operatoren
    protected function parseOperators(array $def): array
    {
        $result = [];
        $value = $def['value'];
        $type = $def['type'];

        if ($value === '' || $value === self::VAL_UNSET) {
            return $result;
        }

        //-- Spezialfälle NULL / NOT NULL
        if ($value === self::VAL_NULL) {
            $result[self::OP_IS_NULL] = true;
            return $result;
        }
        if ($value === self::VAL_NOT_NULL) {
            $result[self::OP_IS_NOT_NULL] = true;
            return $result;
        }

        //-- Magic-Keys (BLANK, !BLANK, EMPTY, !EMPTY)
        if ($value === self::VAL_BLANK) {
            $result[self::OP_IS_BLANK] = true;
            return $result;
        }
        if ($value === self::VAL_NOT_BLANK) {
            $result[self::OP_IS_NOT_BLANK] = true;
            return $result;
        }
        if ($value === self::VAL_EMPTY) {
            $result[self::OP_IS_EMPTY] = true;
            return $result;
        }
        if ($value === self::VAL_NOT_EMPTY) {
            $result[self::OP_IS_NOT_EMPTY] = true;
            return $result;
        }

        //-- Je nach Typ unterschiedliche Parsing‑Logik
        if (in_array($type, self::NUMERIC_TYPES)) {
            return $this->parseNumericOperators($value);
        } elseif (in_array($type, self::DATETIME_TYPES)) {
            return $this->parseDateTimeOperators($value);
        } else {
            return $this->parseStringOperators($value);
        }
    }

    ##### Parst Operatoren für numerische Typen
    protected function parseNumericOperators(string $value): array
    {
        $result = [];
        $this->parseRange($value, true, $result);

        if (
            isset($result[self::OP_GREATER_THAN]) ||
            isset($result[self::OP_LESS_THAN]) ||
            isset($result[self::OP_GREATER_EQUAL]) ||
            isset($result[self::OP_LESS_EQUAL])
        ) {
            return $result;
        }

        $fl = mb_substr($value, 0, 1);
        $sl = mb_substr($value, 1, 1);

        if ($fl === '>' || $fl === '<') {
            $rest = trim(mb_substr($value, 1));
            if ($fl === '>' && $sl === '=') {
                $val = trim(mb_substr($rest, 1));
                if (is_numeric($val)) $result[self::OP_GREATER_EQUAL] = (float)$val;
            } elseif ($fl === '>') {
                if (is_numeric($rest)) $result[self::OP_GREATER_THAN] = (float)$rest;
            } elseif ($fl === '<' && $sl === '=') {
                $val = trim(mb_substr($rest, 1));
                if (is_numeric($val)) $result[self::OP_LESS_EQUAL] = (float)$val;
            } elseif ($fl === '<') {
                if (is_numeric($rest)) $result[self::OP_LESS_THAN] = (float)$rest;
            }
            return $result;
        }

        if ($fl === '!') {
            $rest = trim(mb_substr($value, 1));
            if ($rest !== '' && is_numeric($rest)) $result[self::OP_NOT_EQUAL] = (float)$rest;
            return $result;
        }

        if (is_numeric($value)) {
            $result[self::OP_EQUAL] = (float)$value;
        }
        return $result;
    }

    ##### Parst Operatoren für Datums‑/Zeit‑Typen
    protected function parseDateTimeOperators(string $value): array
    {
        $result = [];
        $this->parseRange($value, false, $result);

        if (
            isset($result[self::OP_GREATER_THAN]) ||
            isset($result[self::OP_LESS_THAN]) ||
            isset($result[self::OP_GREATER_EQUAL]) ||
            isset($result[self::OP_LESS_EQUAL])
        ) {
            return $result;
        }

        $fl = mb_substr($value, 0, 1);
        $sl = mb_substr($value, 1, 1);

        if ($fl === '>' || $fl === '<') {
            $rest = trim(mb_substr($value, 1));
            if ($fl === '>' && $sl === '=') {
                $val = trim(mb_substr($rest, 1));
                if ($val !== '') $result[self::OP_GREATER_EQUAL] = $val;
            } elseif ($fl === '>') {
                if ($rest !== '') $result[self::OP_GREATER_THAN] = $rest;
            } elseif ($fl === '<' && $sl === '=') {
                $val = trim(mb_substr($rest, 1));
                if ($val !== '') $result[self::OP_LESS_EQUAL] = $val;
            } elseif ($fl === '<') {
                if ($rest !== '') $result[self::OP_LESS_THAN] = $rest;
            }
            return $result;
        }

        if ($fl === '!') {
            $rest = trim(mb_substr($value, 1));
            if ($rest !== '') $result[self::OP_NOT_EQUAL] = $rest;
            return $result;
        }

        if ($value !== '') {
            $result[self::OP_EQUAL] = $value;
        }
        return $result;
    }

    ##### Parst Bereichsoperatoren wie ><, >=<, etc.
    protected function parseRange(string $value, bool $asNumber, array &$result): void
    {
        //-- =><= (inklusiv-inklusiv)
        if (strpos($value, '=><=') !== false) {
            $parts = explode('=><=', $value);
            if (count($parts) === 2) {
                $a = $asNumber ? (is_numeric($parts[0]) ? (float)$parts[0] : null) : $parts[0];
                $b = $asNumber ? (is_numeric($parts[1]) ? (float)$parts[1] : null) : $parts[1];
                if ($a !== null && $b !== null) {
                    if ($asNumber ? ($a <= $b) : (strcmp($a, $b) <= 0)) {
                        $result[self::OP_GREATER_EQUAL] = $a;
                        $result[self::OP_LESS_EQUAL] = $b;
                    } else {
                        $result[self::OP_GREATER_EQUAL] = $b;
                        $result[self::OP_LESS_EQUAL] = $a;
                    }
                }
            }
            return;
        }

        //-- =>< (inklusiv-exklusiv)
        if (strpos($value, '=><') !== false) {
            $parts = explode('=><', $value);
            if (count($parts) === 2) {
                $a = $asNumber ? (is_numeric($parts[0]) ? (float)$parts[0] : null) : $parts[0];
                $b = $asNumber ? (is_numeric($parts[1]) ? (float)$parts[1] : null) : $parts[1];
                if ($a !== null && $b !== null) {
                    if ($asNumber ? ($a <= $b) : (strcmp($a, $b) <= 0)) {
                        $result[self::OP_GREATER_EQUAL] = $a;
                        $result[self::OP_LESS_THAN] = $b;
                    } else {
                        $result[self::OP_LESS_EQUAL] = $a;
                        $result[self::OP_GREATER_THAN] = $b;
                    }
                }
            }
            return;
        }

        //-- ><= (exklusiv-inklusiv)
        if (strpos($value, '><=') !== false) {
            $parts = explode('><=', $value);
            if (count($parts) === 2) {
                $a = $asNumber ? (is_numeric($parts[0]) ? (float)$parts[0] : null) : $parts[0];
                $b = $asNumber ? (is_numeric($parts[1]) ? (float)$parts[1] : null) : $parts[1];
                if ($a !== null && $b !== null) {
                    if ($asNumber ? ($a <= $b) : (strcmp($a, $b) <= 0)) {
                        $result[self::OP_GREATER_THAN] = $a;
                        $result[self::OP_LESS_EQUAL] = $b;
                    } else {
                        $result[self::OP_LESS_THAN] = $a;
                        $result[self::OP_GREATER_EQUAL] = $b;
                    }
                }
            }
            return;
        }

        //-- >< (exklusiv-exklusiv)
        if (strpos($value, '><') !== false) {
            $parts = explode('><', $value);
            if (count($parts) === 2) {
                $a = $asNumber ? (is_numeric($parts[0]) ? (float)$parts[0] : null) : $parts[0];
                $b = $asNumber ? (is_numeric($parts[1]) ? (float)$parts[1] : null) : $parts[1];
                if ($a !== null && $b !== null) {
                    if ($asNumber ? ($a <= $b) : (strcmp($a, $b) <= 0)) {
                        $result[self::OP_GREATER_THAN] = $a;
                        $result[self::OP_LESS_THAN] = $b;
                    } else {
                        $result[self::OP_GREATER_THAN] = $b;
                        $result[self::OP_LESS_THAN] = $a;
                    }
                }
            }
            return;
        }
    }

    ##### Parst Operatoren für String‑Typen (LIKE, !, Literal‑Escaping)
    protected function parseStringOperators(string $value): array
    {
        $result = [];

        if ($value === self::VAL_UNSET) {
            return $result;
        }

        $fl = mb_substr($value, 0, 1);

        //-- Backslash escaped den nachfolgenden Magic-Key
        if ($fl === '\\') {
            $rest = trim(mb_substr($value, 1));
            if ($rest !== '') {
                $result[self::OP_LITERAL] = $rest;
            }
            return $result;
        }

        //-- Ungleich
        if ($fl === '!') {
            $rest = trim(mb_substr($value, 1));
            if ($rest !== '') {
                $result[self::OP_NOT_EQUAL] = $rest;
            }
            return $result;
        }

        //-- LIKE-Operator: mindestens eine Tilde
        if (substr_count($value, '~') >= 1) {
            $parts = explode('~', $value);
            $string = implode('~', $parts);
            if (strpos($string, '~~') === false) {
                $result[self::OP_LIKE] = $string;
            }
            return $result;
        }

        //-- Einfacher Gleichheitswert
        if ($value !== '') {
            $result[self::OP_EQUAL] = $value;
        }
        return $result;
    }

    ########################### WHERE-BEDINGUNGEN BAUEN

    ##### Zentrale Methode zum Bauen von WHERE‑Bedingungen (für String und Prepared)
    protected function buildWhereConditions(array $def, int $pad, string $mode): array
    {
        $lines = [];
        $tab = 4;

        //--- Behandlung von OR‑Gruppen ---
        if (isset($def['type']) && $def['type'] === self::GROUP_OR . 'item') {
            $groupLines = [];
            foreach ($def['conditions'] as $logicalName => $value) {
                $main = $this->keyManifestLive;
                if (!isset($main[$logicalName]) || !isset($main[$logicalName]['rowName'])) continue;
                $fieldDef          = $main[$logicalName];
                $fieldDef['value'] = $value;
                //-- Hole die bereits geparsten Operatoren
                $parsed = $this->keyManifestLiveOp[$logicalName] ?? [];
                $fieldDef = array_merge($fieldDef, $parsed);
                $fieldLines        = $this->buildWhereConditions($fieldDef, $pad - 5, $mode);
                if (!empty($fieldLines)) {
                    $groupLines[] = '(' . implode(' AND ', $fieldLines) . ')';
                }
            }
            if (!empty($groupLines)) {
                $lines[] = '(' .
                    PHP_EOL . str_repeat(' ', $tab * 3) .
                    implode(PHP_EOL . str_repeat(' ', ($tab * 3) - 3) . 'OR ', $groupLines) .
                    PHP_EOL .
                    '    )';
            }
            return $lines;
        }
        //--- Ende Gruppenbehandlung ---

        if (!isset($def['rowName']) || $def['rowName'] === null) return $lines;

        //-- UNSET? (wird bereits vor dem Parsen gefiltert, aber sicherheitshalber)
        if (isset($def['value']) && $def['value'] === self::VAL_UNSET) return $lines;

        //-- IS NULL / IS NOT NULL
        if (isset($def[self::OP_IS_NULL])) {
            $lines[] = str_pad($this->quoteMe($def['rowName']), $pad, ' ') . ' IS NULL';
            return $lines;
        }
        if (isset($def[self::OP_IS_NOT_NULL])) {
            $lines[] = str_pad($this->quoteMe($def['rowName']), $pad, ' ') . ' IS NOT NULL';
            return $lines;
        }

        //-- BLANK / NOT BLANK
        if (isset($def[self::OP_IS_BLANK])) {
            if (in_array($def['type'], self::NUMERIC_TYPES)) {
                $lines[] = str_pad($this->quoteMe($def['rowName']), $pad, ' ') . ' = 0';
            } else {
                $lines[] = str_pad($this->quoteMe($def['rowName']), $pad, ' ') . " = ''";
            }
            return $lines;
        }
        if (isset($def[self::OP_IS_NOT_BLANK])) {
            if (in_array($def['type'], self::NUMERIC_TYPES)) {
                $lines[] = str_pad($this->quoteMe($def['rowName']), $pad, ' ') . ' != 0';
            } else {
                $lines[] = str_pad($this->quoteMe($def['rowName']), $pad, ' ') . " != ''";
            }
            return $lines;
        }

        //-- EMPTY / NOT EMPTY
        if (isset($def[self::OP_IS_EMPTY])) {
            if (in_array($def['type'], self::NUMERIC_TYPES)) {
                $lines[] = '(' . str_pad($this->quoteMe($def['rowName']), $pad, ' ') . ' IS NULL OR ' . str_pad($this->quoteMe($def['rowName']), $pad, ' ') . ' = 0)';
            } else {
                $lines[] = '(' . str_pad($this->quoteMe($def['rowName']), $pad, ' ') . ' IS NULL OR ' . str_pad($this->quoteMe($def['rowName']), $pad, ' ') . " = '')";
            }
            return $lines;
        }
        if (isset($def[self::OP_IS_NOT_EMPTY])) {
            if (in_array($def['type'], self::NUMERIC_TYPES)) {
                $lines[] = '(' . str_pad($this->quoteMe($def['rowName']), $pad, ' ') . ' IS NOT NULL AND ' . str_pad($this->quoteMe($def['rowName']), $pad, ' ') . ' != 0)';
            } else {
                $lines[] = '(' . str_pad($this->quoteMe($def['rowName']), $pad, ' ') . ' IS NOT NULL AND ' . str_pad($this->quoteMe($def['rowName']), $pad, ' ') . " != '')";
            }
            return $lines;
        }

        //-- Literal (ehemals RAW)
        if (isset($def[self::OP_LITERAL])) {
            if ($mode === 'prepared') {
                $lines[] = str_pad($this->quoteMe($def['rowName']), $pad, ' ') . ' = ?';
                $this->addParam($def[self::OP_LITERAL], $this->typeToBindParam($def['type']));
            } else {
                $val = $this->db->escape($def[self::OP_LITERAL]);
                $lines[] = str_pad($this->quoteMe($def['rowName']), $pad, ' ') . " = '$val'";
            }
            return $lines;
        }

        $isNumeric = in_array($def['type'], self::NUMERIC_TYPES);
        $isFloat = $def['type'] === self::TYPE_FLOAT;

        $ops = [self::OP_EQUAL, self::OP_NOT_EQUAL, self::OP_LESS_THAN, self::OP_GREATER_THAN, self::OP_LESS_EQUAL, self::OP_GREATER_EQUAL];
        foreach ($ops as $op) {
            if (!isset($def[$op])) continue;
            $val = $def[$op];
            $sqlOp = $this->operatorToSql($op);

            if ($mode === 'prepared') {
                $lines[] = str_pad($this->quoteMe($def['rowName']), $pad, ' ') . " $sqlOp ?";
                $bindType = $isFloat ? 'd' : ($isNumeric ? 'i' : 's');
                $this->addParam($val, $bindType);
            } else {
                $escaped = $this->db->escape((string)$val);
                if ($isNumeric) {
                    $lines[] = str_pad($this->quoteMe($def['rowName']), $pad, ' ') . " $sqlOp $escaped";
                } else {
                    $lines[] = str_pad($this->quoteMe($def['rowName']), $pad, ' ') . " $sqlOp '$escaped'";
                }
            }
        }

        if (isset($def[self::OP_LIKE])) {
            $parts = explode('~', $def[self::OP_LIKE]);
            $escapedParts = [];
            foreach ($parts as $part) {
                $escapedParts[] = $this->getEscapedLikeString($part);
            }
            $pattern = implode('%', $escapedParts);
            if ($mode === 'prepared') {
                $lines[] = str_pad($this->quoteMe($def['rowName']), $pad, ' ') . ' LIKE ?';
                $this->addParam($pattern, 's');
            } else {
                $patternEscaped = $this->db->escape($pattern);
                $lines[] = str_pad($this->quoteMe($def['rowName']), $pad, ' ') . " LIKE '$patternEscaped'";
            }
        }
        return $lines;
    }

    ########################### QUERY-BAUSTEINE

    ##### Ermittelt die SELECT‑Klausel
    public function getSelectQuery(): string
    {
        $pad = 24;
        $parts = [];
        foreach ($this->keyManifest as $key => $def) {
            if (isset($def['rowName']) && $def['rowName'] !== null) {
                $quotedName = $this->quoteMe($def['rowName']);
                $parts[] = str_pad($quotedName, $pad, ' ') . ' AS ' . $key;
            }
        }
        return PHP_EOL . 'SELECT' . PHP_EOL . '    ' . implode(',' . PHP_EOL . '    ', $parts);
    }

    ##### Ermittelt die WHERE‑Klausel (inkl. Suche und sqlCondition)
    public function getWhereQuery(): string
    {
        $pad = 20;
        $conditions = [PHP_EOL . 'WHERE 1'];

        foreach ($this->keyManifestLiveOp as $def) {
            $lines = $this->buildWhereConditions($def, $pad, 'string');
            $conditions = array_merge($conditions, $lines);
        }

        $meta = $this->metaManifestLive;

        //-- Volltextsuche über mehrere Felder
        if (!empty($meta['searchString']) && is_string($meta['searchString']) && trim($meta['searchString'], '~') !== '') {
            $targets     = is_string($meta['searchTarget']) ? explode(',', $meta['searchTarget']) : [];
            $searchParts = [];
            $main        = $this->keyManifestLive;
            foreach ($targets as $target) {
                $target = trim($target);
                if (isset($main[$target]) && isset($main[$target]['rowName'])) {
                    $rowName = $this->quoteMe($main[$target]['rowName']);
                    $searchParts[] = str_pad($rowName, $pad - 3, ' ')
                        . " LIKE '%" . $this->db->escape($this->getEscapedLikeString($meta['searchString'])) . "%'";
                }
            }
            if (!empty($searchParts)) {
                $conditions[] = '(' . PHP_EOL . '           ' . implode(PHP_EOL . '        OR ', $searchParts) . PHP_EOL . '    )';
            }
        }

        //-- Benutzerdefinierter SQL‑Teil
        if (!empty($meta['sqlCondition']) && is_string($meta['sqlCondition'])) {
            $conditions[] = '(' . PHP_EOL . '         ' . $meta['sqlCondition'] . PHP_EOL . '        )';
        }
        return implode(PHP_EOL . '    AND ', $conditions);
    }

    ##### WHERE‑Teil für Prepared Statements (mit Platzhaltern)
    protected function getWherePrepared(): string
    {
        $pad = 20;
        $conditions = [PHP_EOL . 'WHERE 1'];

        foreach ($this->keyManifestLiveOp as $def) {
            $lines = $this->buildWhereConditions($def, $pad, 'prepared');
            $conditions = array_merge($conditions, $lines);
        }

        $meta = $this->metaManifestLive;

        if (!empty($meta['searchString']) && is_string($meta['searchString']) && trim($meta['searchString'], '~') !== '') {
            $targets     = is_string($meta['searchTarget']) ? explode(',', $meta['searchTarget']) : [];
            $searchParts = [];
            $main        = $this->keyManifestLive;
            foreach ($targets as $target) {
                $target = trim($target);
                if (isset($main[$target]) && isset($main[$target]['rowName'])) {
                    $rowName = $this->quoteMe($main[$target]['rowName']);
                    $searchParts[] = str_pad($rowName, $pad - 3, ' ') . " LIKE ?";
                    $this->addParam('%' . $this->getEscapedLikeString($meta['searchString']) . '%', 's');
                }
            }
            if (!empty($searchParts)) {
                $conditions[] = '(' . PHP_EOL . '           ' . implode(PHP_EOL . '        OR ', $searchParts) . PHP_EOL . '        )';
            }
        }

        if (!empty($meta['sqlCondition']) && is_string($meta['sqlCondition'])) {
            $conditions[] = '(' . PHP_EOL . '         ' . $meta['sqlCondition'] . PHP_EOL . '        )';
        }
        return implode(PHP_EOL . '    AND ', $conditions);
    }

    ##### Ermittelt die ORDER BY‑Klausel
    public function getOrderQuery(): string
    {
        if (empty($this->metaManifestLive['sort']) || !is_string($this->metaManifestLive['sort'])) return '';

        $pad = 24;
        $parts = [];
        $sortList = explode(',', $this->metaManifestLive['sort']);
        $main = $this->keyManifestLive;

        foreach ($sortList as $item) {
            $item = trim($item);
            if ($item === '') continue;
            $sign        = mb_substr($item, 0, 1);
            $logicalName = ($sign === self::SORT_DESC || $sign === self::SORT_ASC) ? mb_substr($item, 1) : $item;
            $direction   = $sign === self::SORT_DESC ? ' DESC' : ($sign === self::SORT_ASC ? ' ASC' : '');
            if (isset($main[$logicalName])) {
                $quotedField = $this->quoteMe($logicalName);
                $parts[] = str_pad($quotedField, $pad, ' ') . $direction;
            }
        }
        return empty($parts) ? '' : PHP_EOL . 'ORDER BY' . PHP_EOL . '    ' . implode(',' . PHP_EOL . '    ', $parts);
    }

    ##### Ermittelt die LIMIT‑Klausel (für Paginierung)
    public function getLimitQuery(): string|false
    {
        if (!isset($this->metaManifestLive['firstElemNumber']) || $this->metaManifestLive['firstElemNumber'] === false) {
            return false;
        }
        return PHP_EOL . 'LIMIT ' . PHP_EOL . '    ' . $this->metaManifestLive['firstElemNumber'] . ', ' . $this->metaManifestLive['pageSize'] . ' ';
    }

    ##### Baut den FROM‑Teil (Tabelle + Alias)
    public function getBodyQuery(): string|false
    {
        $table = $this->getTableQuery();
        if (!$table) return false;
        return $this->getSelectQuery() . PHP_EOL . 'FROM ' . PHP_EOL . '    ' . $table;
    }

    ##### Komplette Query ohne LIMIT
    public function getLimitlessQuery(): string|false
    {
        $body = $this->getBodyQuery();
        if (!$body) return false;
        return $body . PHP_EOL . $this->getJoinQuery() . $this->getWhereQuery();
    }

    ##### Baut eine COUNT‑Abfrage
    public function getCountQuery(bool $total = false): string|false
    {
        $table = $this->getTableQuery();
        if (!$table) return false;
        $where = $total === true ? PHP_EOL . 'WHERE 1' : $this->getWhereQuery();
        return 'SELECT' . PHP_EOL . '    count(*) as count FROM ' . PHP_EOL . '    ' . $table . $this->getJoinQuery() . $where;
    }

    ########################### PREPARED STATEMENTS

    ##### Bereitet die Query für Prepared Statements vor
    public function getPreparedQuery(): array|false
    {
        $this->preparedParams = [];
        $this->preparedTypes = '';

        $body = $this->getBodyQuery();
        if (!$body) return false;
        $join = $this->getJoinQuery();
        $where = $this->getWherePrepared();
        $order = $this->getOrderQuery();
        $limit = $this->getLimitQuery();

        $query = $body . $join . $where . $order . $limit;
        return [
            'query'  => $query,
            'params' => $this->preparedParams,
            'types'  => $this->preparedTypes,
        ];
    }

    ########################### AKTUALISIERUNG DER OPERATOREN (EPIC 4)

    protected function refreshOperators(): void
    {
        $this->keyManifestLiveOp = [];
        foreach ($this->keyManifestLive as $key => $def) {
            $this->keyManifestLiveOp[$key] = array_merge($def, $this->parseOperators($def));
        }
    }
}