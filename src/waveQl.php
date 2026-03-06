<?php

declare(strict_types=1);

namespace e2;

/**
 * waveQl – SQL-Query-Builder mit Operator-Parsing, Sicherheitsprüfung und optionalen Prepared Statements
 * =====================================================================================================
 *
 * Diese Klasse generiert aus fieldDefinitions, inputValues und tableInfo eine komplette SQL-Query.
 * Sie unterstützt komplexe Bedingungen durch intuitive Operatoren im Wert-String,
 * automatische Typbehandlung, Magic-Keys für Leerwerte, Bereichsabfragen, Joins,
 * Paginierung, Sortierung und Volltextsuche. Wahlweise mit Prepared Statements für erhöhte Sicherheit.
 *
 *
 * OPERATOREN IM WERT-STRING
 * -------------------------
 *   - >10              -> Feld > 10
 *   - >=10             -> Feld >= 10
 *   - <10              -> Feld < 10
 *   - <=10             -> Feld <= 10
 *   - !10              -> Feld != 10
 *   - ~text~           -> LIKE '%text%'
 *   - text~            -> LIKE 'text%'
 *   - ~text            -> LIKE '%text'
 *   - a~b~c            -> LIKE '%a%b%c%'
 *   - !NULL            -> IS NOT NULL
 *   - NULL             -> IS NULL
 *   - UNSET            -> wird ignoriert
 *
 *
 * MAGIC-KEYS (typabhängig)
 * ------------------------
 *   - BLANK            -> String: = '' | Numerisch: = 0 | Datum: = ''
 *   - !BLANK           -> String: != '' | Numerisch: != 0 | Datum: != ''
 *   - EMPTY            -> String: IS NULL OR = '' | Numerisch: IS NULL OR = 0 | Datum: IS NULL OR = ''
 *   - !EMPTY           -> String: IS NOT NULL AND != '' | Numerisch: IS NOT NULL AND != 0 | Datum: IS NOT NULL AND != ''
 *
 *
 * BEREICHSOPERATOREN (numerisch & Datum/Zeit)
 * -------------------------------------------
 *   - 10><20           -> 10 < Feld < 20      (exklusiv)
 *   - 10><=20          -> 10 < Feld <= 20     (exklusiv-inklusiv)
 *   - 10>=<20          -> 10 <= Feld < 20     (inklusiv-exklusiv)
 *   - 10=><=20         -> 10 <= Feld <= 20    (inklusiv)
 *   Vertauschte Grenzen werden automatisch korrigiert.
 *
 *
 * FILTER-PARAMETER (im '~filter~'-Array)
 * ---------------------------------------
 *   - sort          : z.B. '>name,<id' (absteigend name, aufsteigend id)
 *   - pageNumber    : Seitennummer (1‑basiert)
 *   - pageSize      : Einträge pro Seite
 *   - searchString  : Suchbegriff (wird in %...% gewrappt)
 *   - searchTarget  : Kommagetrennte Zielfelder
 *   - mysql         : Benutzerdefinierter SQL-Ausdruck (wird auf Sicherheit geprüft)
 *
 *
 * OR-GRUPPEN (flache Gruppe)
 * --------------------------
 *   '~or~' => [ 'feld' => 'wert', 'anderes' => '>5' ]
 *   Erzeugt: (feld = wert OR anderes > 5)
 *
 *
 * AUFBAU fieldDefinitions
 * -----------------------
 *   [
 *       'feldname' => [
 *           'rowName' => 'tbl.spalte',   // Spaltenname oder SQL-Ausdruck
 *           'type'    => 'string|integer|float|date|time|dateTime|...',
 *           'value'   => 'optionaler Standardwert'
 *       ],
 *       '~filter~' => [ ... ]   // optionale Filter-Standards
 *   ]
 *
 *
 * AUFBAU inputValues
 * ------------------
 *   [
 *       'feldname' => 'wert',          // überschreibt fieldDefinitions['value']
 *       '~filter~' => [ ... ],         // überschreibt fieldDefinitions['~filter~']
 *       '~or~'     => [ ... ]          // flache OR-Gruppe
 *   ]
 *
 *
 * AUFBAU tableInfo
 * ----------------
 *   [
 *       'tableName' => 'haupttabelle',
 *       'tableKey'  => 'alias',
 *       'joinList'  => [
 *           [
 *               'type'          => 'LEFT',   // LEFT, RIGHT, INNER, CROSS, STRAIGHT
 *               'tableName'     => 'tabelle',
 *               'tableKey'      => 'alias',
 *               'connectColumn' => 'spalte',
 *               'connectWith'   => 'alias.spalte'
 *           ]
 *       ]
 *   ]
 *
 *
 * AUTOMATISCHE FELDERWEITERUNGEN
 * ------------------------------
 * Für Datums‑/Zeit‑Felder werden zusätzliche virtuelle Felder generiert:
 *   - feldYEAR, feldMONTH, feldDAY, feldDATE, feldTIME, feldHOUR, feldMINUTE, feldUTS
 *
 *
 * VERWENDUNG
 * ----------
 *   $builder = new \e2\waveQl($db, $tableInfo, $fieldDefinitions, $inputValues, ['prepared' => true]);
 *   $rows = $builder->execute();          // Ergebnis abrufen
 *   $sql = $builder->getQuery();          // SQL-String für Debug
 *
 *
 * SICHERHEITSHINWEIS
 * ------------------
 *   Der 'mysql'-Parameter wird auf gefährliche Schlüsselwörter geprüft (DROP, DELETE, UNION, …).
 *   Bei Verdacht wird er ignoriert und ein Fehler geloggt. Verwende Prepared Statements,
 *   wenn du zusätzliche Sicherheit wünschst.
 *
 * =====================================================================================================
 */

class waveQl
{
    // --- Operator-Kürzel -------------------------------------------------------------
    private const OP_EQUAL         = 'e';
    private const OP_NOT_EQUAL     = 'ne';
    private const OP_LESS_THAN     = 'lt';
    private const OP_GREATER_THAN  = 'gt';
    private const OP_LESS_EQUAL    = 'lte';
    private const OP_GREATER_EQUAL = 'gte';
    private const OP_LIKE          = 'like';
    private const OP_RAW           = 'raw';

    // --- Spezielle Werte --------------------------------------------------------------
    private const VAL_UNSET       = 'UNSET';
    private const VAL_NULL        = 'NULL';
    private const VAL_NOT_NULL    = '!NULL';
    private const VAL_BLANK       = 'BLANK';
    private const VAL_NOT_BLANK   = '!BLANK';
    private const VAL_EMPTY       = 'EMPTY';
    private const VAL_NOT_EMPTY   = '!EMPTY';

    // --- Sortierrichtungen ------------------------------------------------------------
    private const SORT_DESC       = '>';
    private const SORT_ASC        = '<';

    // --- Feldtypen --------------------------------------------------------------------
    private const TYPE_STRING     = 'string';
    private const TYPE_INTEGER    = 'integer';
    private const TYPE_FLOAT      = 'float';
    private const TYPE_DATE       = 'date';
    private const TYPE_TIME       = 'time';
    private const TYPE_DATETIME   = 'dateTime';
    private const TYPE_YEAR       = 'year';
    private const TYPE_QUARTER    = 'quarter';
    private const TYPE_MONTH      = 'month';
    private const TYPE_DAY        = 'day';
    private const TYPE_HOUR       = 'hour';
    private const TYPE_MINUTE     = 'minute';
    private const TYPE_UTS        = 'uts';

    private const NUMERIC_TYPES = [
        self::TYPE_INTEGER,
        self::TYPE_FLOAT,
        self::TYPE_YEAR,
        self::TYPE_QUARTER,
        self::TYPE_MONTH,
        self::TYPE_DAY,
        self::TYPE_HOUR,
        self::TYPE_MINUTE,
        self::TYPE_UTS,
    ];

    private const DATETIME_TYPES = [
        self::TYPE_DATE,
        self::TYPE_TIME,
        self::TYPE_DATETIME,
    ];

    // --- Gruppenkonstante (flache OR-Gruppen) -----------------------------------------
    private const GROUP_OR     = '~or~';
    private const GROUP_FILTER = '~filter~';

    // --- Eigenschaften ----------------------------------------------------------------
    private array $fieldDefinitions;
    private array $inputValues;
    private array $tableInfo;
    private array $resolvedData = [];
    private $db; // kann mysqli oder MockDb sein
    private bool $usePrepared;

    private array $params = [];
    private string $types = '';



    ########################### KONSTRUKTOR & INITIALISIERUNG

    /**
     * Konstruktor: initialisiert den Builder mit Datenbankverbindung und Konfiguration.
     */
    public function __construct($db, array $tableInfo, array $fieldDefinitions, array $inputValues = [], array $options = [])
    {
        //-- Datenbankverbindung speichern (für Escaping und Prepared)
        $this->db               = $db;
        //-- Tabelleninformationen (inkl. Joins) übernehmen
        $this->tableInfo        = $tableInfo;
        //-- Felddefinitionen merken
        $this->fieldDefinitions = $fieldDefinitions;
        //-- Eingabewerte (überschreiben) speichern
        $this->inputValues      = $inputValues;
        //-- Soll mit Prepared Statements gearbeitet werden?
        $this->usePrepared      = $options['prepared'] ?? false;

        //-- Alte Datenstrukturen konvertieren (Abwärtskompatibilität)
        $this->migrateLegacyData();

        //-- Eigentliche Initialisierung: resolvedData aufbauen
        $this->initData();
    }



    ### migriert alte leftTableList und filter-Schlüssel (abwärtskompatibel)
    private function migrateLegacyData(): void
    {
        $tableName = $this->tableInfo['tableName'] ?? 'unbekannte Tabelle';

        //-- leftTableList → joinList
        if (isset($this->tableInfo['leftTableList']) && is_array($this->tableInfo['leftTableList']) && !isset($this->tableInfo['joinList'])) {
            error_log("waveQl (Tabelle $tableName): leftTableList ist veraltet, verwende joinList. Bitte aktualisieren.");
            $joinList = [];
            foreach ($this->tableInfo['leftTableList'] as $join) {
                //-- Standard-Join-Typ ist LEFT, falls nicht angegeben
                $join['type'] = $join['type'] ?? 'LEFT';
                $joinList[] = $join;
            }
            $this->tableInfo['joinList'] = $joinList;
            unset($this->tableInfo['leftTableList']);
        }

        //-- fieldDefinitions['filter'] → '~filter~'
        if (isset($this->fieldDefinitions['filter']) && !isset($this->fieldDefinitions[self::GROUP_FILTER])) {
            error_log("waveQl (Tabelle $tableName): fieldDefinitions['filter'] ist veraltet, verwende '~filter~'. Bitte aktualisieren.");
            $this->fieldDefinitions[self::GROUP_FILTER] = $this->fieldDefinitions['filter'];
            unset($this->fieldDefinitions['filter']);
        }

        //-- inputValues['filter'] → '~filter~'
        if (isset($this->inputValues['filter']) && !isset($this->inputValues[self::GROUP_FILTER])) {
            error_log("waveQl (Tabelle $tableName): inputValues['filter'] ist veraltet, verwende '~filter~'. Bitte aktualisieren.");
            $this->inputValues[self::GROUP_FILTER] = $this->inputValues['filter'];
            unset($this->inputValues['filter']);
        }

        //-- Doppelungen bereinigen
        if (isset($this->fieldDefinitions['filter']) && isset($this->fieldDefinitions[self::GROUP_FILTER])) {
            error_log("waveQl (Tabelle $tableName): fieldDefinitions enthält sowohl 'filter' als auch '~filter~'. 'filter' wird ignoriert. Bitte 'filter' entfernen.");
            unset($this->fieldDefinitions['filter']);
        }
        if (isset($this->inputValues['filter']) && isset($this->inputValues[self::GROUP_FILTER])) {
            error_log("waveQl (Tabelle $tableName): inputValues enthält sowohl 'filter' als auch '~filter~'. 'filter' wird ignoriert. Bitte 'filter' entfernen.");
            unset($this->inputValues['filter']);
        }
    }



    ### zentrale Dateninitialisierung: Felddefinitionen, Filter, OR-Gruppen
    private function initData(): void
    {
        $resolved = [];

        //-- Zuerst alle normalen Felder aus fieldDefinitions übernehmen
        foreach ($this->fieldDefinitions as $key => $config) {
            //-- Nur gültige Felddefinitionen verarbeiten (kein Filter, rowName vorhanden)
            if (!is_array($config) || $key === self::GROUP_FILTER) continue;
            if (empty($config['rowName']) || !is_string($config['rowName'])) continue;

            //-- Sicherheitshalber HTML-Tags entfernen
            $config['rowName'] = strip_tags($config['rowName']);
            //-- Typ normalisieren (falls nicht angegeben: string)
            $config['type'] = $this->normalizeType($config);
            //-- Standardwert setzen (leerer String falls nicht vorhanden)
            $config['value'] = isset($config['value']) && is_string($config['value']) ? $config['value'] : '';

            //-- Automatische Zusatzfelder für Datum/Zeit erzeugen (z.B. feldYEAR)
            $autoFields = $this->generateAutoFields($key, $config);
            foreach ($autoFields as $autoKey => $autoDef) {
                if (!isset($resolved[$autoKey])) {
                    $resolved[$autoKey] = $autoDef;
                }
            }

            //-- Ursprüngliches Feld hinzufügen
            $resolved[$key] = $config;
        }

        //-- Filter-Defaults aus fieldDefinitions und inputValues zusammenführen
        $filterDefaults = $this->fieldDefinitions[self::GROUP_FILTER] ?? [];
        $filterInput = $this->inputValues[self::GROUP_FILTER] ?? [];

        $resolved[self::GROUP_FILTER] = $this->buildFilter($filterDefaults, $filterInput);

        $this->resolvedData = $resolved;
        //-- inputValues in die resolvedData einarbeiten (überschreiben)
        $this->mergeInputValues();

        //-- OR-Gruppe parsen, falls vorhanden
        $this->parseInputGroups();

        //-- Filter weiterverarbeiten (Sortierung validieren, Paginierung, mysql-Sicherheit)
        $this->processFilter();
        //-- Operatoren parsen und firstElemNumber berechnen
        $this->enlargeData();
    }



    ### normalisiert den Typ eines Feldes (Fallback: string)
    private function normalizeType(array $config): string
    {
        return isset($config['type']) && is_string($config['type'])
            ? strip_tags($config['type'])
            : self::TYPE_STRING;
    }



    ### erzeugt virtuelle Felder für Datum/Zeit (z.B. feldYEAR, feldMONTH, …)
    private function generateAutoFields(string $key, array $config): array
    {
        $type = $config['type'];
        //-- Nur für Datums-/Zeit-Typen
        if (!in_array($type, self::DATETIME_TYPES)) return [];

        $funcs = [];
        //-- Datums-Funktionen
        if ($type === self::TYPE_DATETIME || $type === self::TYPE_DATE) {
            $funcs[self::TYPE_DATE] = 'DATE';
            $funcs[self::TYPE_YEAR] = 'YEAR';
            $funcs[self::TYPE_QUARTER] = 'QUARTER';
            $funcs[self::TYPE_MONTH] = 'MONTH';
            $funcs[self::TYPE_DAY] = 'DAY';
        }
        //-- Zeit-Funktionen
        if ($type === self::TYPE_DATETIME || $type === self::TYPE_TIME) {
            $funcs[self::TYPE_TIME] = 'TIME';
            $funcs[self::TYPE_HOUR] = 'HOUR';
            $funcs[self::TYPE_MINUTE] = 'MINUTE';
        }
        //-- Unix-Timestamp immer verfügbar
        $funcs[self::TYPE_UTS] = 'UNIX_TIMESTAMP';

        $auto = [];
        foreach ($funcs as $subType => $sqlFunc) {
            $autoKey = $key . strtoupper($subType);
            //-- SQL-Ausdruck: Funktion auf rowName anwenden
            $auto[$autoKey] = [
                'value'   => '',
                'rowName' => $sqlFunc . '(' . htmlspecialchars($config['rowName']) . ')',
                'type'    => $subType,
            ];
        }
        return $auto;
    }



    ### baut das Filter-Array aus defaults und input (nur erlaubte Felder)
    private function buildFilter(array $defaults, array $input): array
    {
        $fields = ['sort', 'pageNumber', 'pageSize', 'mysql', 'searchString', 'searchTarget'];
        $filter = [];

        foreach ($fields as $f) {
            //-- Input hat Vorrang vor Defaults
            if (isset($input[$f]) && (is_string($input[$f]) || is_numeric($input[$f]))) {
                $filter[$f] = trim((string)$input[$f]);
            } elseif (isset($defaults[$f]) && (is_string($defaults[$f]) || is_numeric($defaults[$f]))) {
                $filter[$f] = trim((string)$defaults[$f]);
            } else {
                $filter[$f] = false; // nicht gesetzt
            }
        }
        return $filter;
    }



    ### überschreibt Feldwerte mit inputValues (falls vorhanden)
    private function mergeInputValues(): void
    {
        foreach ($this->resolvedData as $key => $config) {
            //-- Filter nicht überschreiben, nur echte Felder
            if ($key === self::GROUP_FILTER) continue;
            //-- Wenn ein Wert in inputValues existiert, nimm ihn
            if (isset($this->inputValues[$key]) && (is_string($this->inputValues[$key]) || is_numeric($this->inputValues[$key]))) {
                $config['value'] = trim((string)$this->inputValues[$key]);
            } else {
                //-- Sonst den bisherigen Wert (aus fieldDefinitions) beibehalten
                $config['value'] = trim((string)($config['value'] ?? ''));
            }
            $this->resolvedData[$key] = $config;
        }
    }



    ### parst die flache OR-Gruppe aus inputValues
    private function parseInputGroups(): void
    {
        if (isset($this->inputValues[self::GROUP_OR]) && is_array($this->inputValues[self::GROUP_OR])) {
            $conditions = $this->inputValues[self::GROUP_OR];
            //-- Rekursive OR-Gruppen entfernen (nur flach erlaubt)
            foreach ($conditions as $key => $value) {
                if ($key === self::GROUP_OR) {
                    unset($conditions[$key]);
                }
            }
            $groupKey = self::GROUP_OR;
            //-- Spezielle Struktur für OR-Gruppe merken
            $this->resolvedData[$groupKey] = [
                '_type'      => 'or_group',
                'conditions' => $conditions,
                'rowName'    => null,
            ];
        }
    }



    ### verarbeitet die Filter-Parameter (Sortierung, Suche, Paginierung, Sicherheitscheck)
    private function processFilter(): void
    {
        $f = &$this->resolvedData[self::GROUP_FILTER];

        //-- Sortierung validieren: nur existierende Felder erlauben
        $sortItems = [];
        if (is_string($f['sort']) && $f['sort'] !== '') {
            $sortItems = explode(',', $f['sort']);
        }
        $validSorts = [];
        foreach ($sortItems as $item) {
            $item = trim($item);
            $sign = '';
            $maybeSign = mb_substr($item, 0, 1);
            if ($maybeSign === self::SORT_DESC || $maybeSign === self::SORT_ASC) {
                $sign = $maybeSign;
                $item = trim(mb_substr($item, 1));
            }
            //-- Prüfen, ob das Feld in fieldDefinitions existiert
            if (isset($this->fieldDefinitions[$item])) {
                $validSorts[] = $sign . $item;
            }
        }
        $f['sort'] = $validSorts ? implode(',', $validSorts) : ($this->fieldDefinitions[self::GROUP_FILTER]['sort'] ?? '');

        //-- Suchziele validieren
        $targetItems = [];
        if (is_string($f['searchTarget']) && $f['searchTarget'] !== '') {
            $targetItems = explode(',', $f['searchTarget']);
        }
        $validTargets = [];
        foreach ($targetItems as $item) {
            $item = trim($item);
            if (isset($this->fieldDefinitions[$item])) {
                $validTargets[] = $item;
            }
        }
        $f['searchTarget'] = $validTargets ? implode(',', $validTargets) : ($this->fieldDefinitions[self::GROUP_FILTER]['searchTarget'] ?? '');

        //-- Paginierung: Seitenzahlen müssen positiv sein
        $pageNumber = abs((int)$f['pageNumber']);
        $pageSize = abs((int)$f['pageSize']);
        if ($pageSize === 0 || $pageNumber === 0) {
            $f['pageNumber'] = false;
            $f['pageSize'] = false;
        } else {
            $f['pageNumber'] = $pageNumber;
            $f['pageSize'] = $pageSize;
        }

        //-- Benutzerdefinierten SQL-Check auf Sicherheit
        $mysql = '';
        if (is_string($f['mysql'])) {
            $mysql = trim($f['mysql']);
        }
        if ($mysql !== '') {
            if (!$this->isMysqlSafe($mysql)) {
                error_log("waveQl: Unsicherer mysql-Parameter blockiert: " . $mysql);
                $f['mysql'] = false;
            } else {
                //-- Platzhalter durch rowName ersetzen (z.B. 'feld' durch 'tbl.spalte')
                $mysql = ' ' . $mysql . ' ';
                foreach ($this->fieldDefinitions as $replaceName => $replaceArr) {
                    if ($replaceName !== self::GROUP_FILTER && isset($replaceArr['rowName'])) {
                        $mysql = str_replace(' ' . $replaceName . ' ', ' ' . $replaceArr['rowName'] . ' ', $mysql);
                    }
                }
                $mysql = trim($mysql);
                $f['mysql'] = $mysql !== '' ? $mysql : false;
            }
        } else {
            $f['mysql'] = false;
        }
    }



    ### erweitert die Daten um berechnete Werte (firstElemNumber, Operator-Arrays)
    private function enlargeData(): void
    {
        $main = $this->getMainParams();
        foreach ($main as $key => $def) {
            //-- Für normale Felder (nicht OR-Gruppe) Operatoren parsen
            if (!isset($def['_type'])) {
                $main[$key] = array_merge($def, $this->parseOperators($def));
            } else {
                $main[$key] = $def;
            }
        }

        $filter = $this->getFilterParams();
        //-- Berechnung des Offsets für LIMIT
        if ($filter['pageNumber'] !== false && $filter['pageSize'] !== false) {
            $filter['firstElemNumber'] = ($filter['pageSize'] * $filter['pageNumber']) - $filter['pageSize'];
        } else {
            $filter['firstElemNumber'] = false;
        }

        $this->resolvedData = $main;
        $this->resolvedData[self::GROUP_FILTER] = $filter;
    }



    ########################### OPERATOR-PARSING

    ### parst den Wert eines Feldes und ermittelt die verwendeten Operatoren
    private function parseOperators(array $def): array
    {
        $result = [
            self::OP_EQUAL         => false,
            self::OP_NOT_EQUAL     => false,
            self::OP_LESS_THAN     => false,
            self::OP_GREATER_THAN  => false,
            self::OP_LESS_EQUAL    => false,
            self::OP_GREATER_EQUAL => false,
            self::OP_LIKE          => false,
            self::OP_RAW           => false,
        ];

        $value = $def['value'];
        $type = $def['type'];

        //-- Leerer Wert oder UNSET ignoriert
        if ($value === '' || $value === self::VAL_UNSET) {
            return $result;
        }

        //-- Spezialfälle NULL / NOT NULL
        if ($value === self::VAL_NULL || $value === self::VAL_NOT_NULL) {
            $result[self::OP_EQUAL] = $value;
            return $result;
        }

        //-- Magic-Keys (BLANK, !BLANK, EMPTY, !EMPTY)
        if (in_array($value, [self::VAL_BLANK, self::VAL_NOT_BLANK, self::VAL_EMPTY, self::VAL_NOT_EMPTY], true)) {
            $result[self::OP_EQUAL] = $value;
            return $result;
        }

        //-- Je nach Typ unterschiedliche Parsing-Logik
        if (in_array($type, self::NUMERIC_TYPES)) {
            return $this->parseNumericOperators($value);
        } elseif (in_array($type, self::DATETIME_TYPES)) {
            return $this->parseDateTimeOperators($value);
        } else {
            return $this->parseStringOperators($value);
        }
    }

    ### parst Operatoren für numerische Typen
    private function parseNumericOperators(string $value): array
    {
        $result = [
            self::OP_EQUAL         => false,
            self::OP_NOT_EQUAL     => false,
            self::OP_LESS_THAN     => false,
            self::OP_GREATER_THAN  => false,
            self::OP_LESS_EQUAL    => false,
            self::OP_GREATER_EQUAL => false,
            self::OP_LIKE          => false,
            self::OP_RAW           => false,
        ];

        //-- Zuerst prüfen, ob ein Bereichsoperator vorliegt
        $this->parseRange($value, true, $result);

        if (
            $result[self::OP_GREATER_THAN] !== false ||
            $result[self::OP_LESS_THAN] !== false ||
            $result[self::OP_GREATER_EQUAL] !== false ||
            $result[self::OP_LESS_EQUAL] !== false
        ) {
            return $result;
        }

        $fl = mb_substr($value, 0, 1);
        $sl = mb_substr($value, 1, 1);

        //-- Vergleichsoperatoren >, >=, <, <=
        if ($fl === '>' || $fl === '<') {
            $rest = trim(mb_substr($value, 1));
            if ($fl === '>' && $sl === '=') {
                $val = trim(mb_substr($rest, 1));
                if (is_numeric($val)) {
                    $result[self::OP_GREATER_EQUAL] = (float)$val;
                }
            } elseif ($fl === '>') {
                if (is_numeric($rest)) {
                    $result[self::OP_GREATER_THAN] = (float)$rest;
                }
            } elseif ($fl === '<' && $sl === '=') {
                $val = trim(mb_substr($rest, 1));
                if (is_numeric($val)) {
                    $result[self::OP_LESS_EQUAL] = (float)$val;
                }
            } elseif ($fl === '<') {
                if (is_numeric($rest)) {
                    $result[self::OP_LESS_THAN] = (float)$rest;
                }
            }
            return $result;
        }

        //-- Ungleich-Operator !
        if ($fl === '!') {
            $rest = trim(mb_substr($value, 1));
            if ($rest !== '' && is_numeric($rest)) {
                $result[self::OP_NOT_EQUAL] = (float)$rest;
            }
            return $result;
        }

        //-- Einfacher Gleichheitswert (kein Operator)
        if (is_numeric($value)) {
            $result[self::OP_EQUAL] = (float)$value;
        }

        return $result;
    }

    ### parst Operatoren für Datums-/Zeit-Typen
    private function parseDateTimeOperators(string $value): array
    {
        $result = [
            self::OP_EQUAL         => false,
            self::OP_NOT_EQUAL     => false,
            self::OP_LESS_THAN     => false,
            self::OP_GREATER_THAN  => false,
            self::OP_LESS_EQUAL    => false,
            self::OP_GREATER_EQUAL => false,
            self::OP_LIKE          => false,
            self::OP_RAW           => false,
        ];

        $this->parseRange($value, false, $result);

        if (
            $result[self::OP_GREATER_THAN] !== false ||
            $result[self::OP_LESS_THAN] !== false ||
            $result[self::OP_GREATER_EQUAL] !== false ||
            $result[self::OP_LESS_EQUAL] !== false
        ) {
            return $result;
        }

        $fl = mb_substr($value, 0, 1);
        $sl = mb_substr($value, 1, 1);

        if ($fl === '>' || $fl === '<') {
            $rest = trim(mb_substr($value, 1));
            if ($fl === '>' && $sl === '=') {
                $val = trim(mb_substr($rest, 1));
                if ($val !== '') {
                    $result[self::OP_GREATER_EQUAL] = $val;
                }
            } elseif ($fl === '>') {
                if ($rest !== '') {
                    $result[self::OP_GREATER_THAN] = $rest;
                }
            } elseif ($fl === '<' && $sl === '=') {
                $val = trim(mb_substr($rest, 1));
                if ($val !== '') {
                    $result[self::OP_LESS_EQUAL] = $val;
                }
            } elseif ($fl === '<') {
                if ($rest !== '') {
                    $result[self::OP_LESS_THAN] = $rest;
                }
            }
            return $result;
        }

        if ($fl === '!') {
            $rest = trim(mb_substr($value, 1));
            if ($rest !== '') {
                $result[self::OP_NOT_EQUAL] = $rest;
            }
            return $result;
        }

        if ($value !== '') {
            $result[self::OP_EQUAL] = $value;
        }

        return $result;
    }

    ### parst Bereichsoperatoren wie ><, >=<, etc.
    private function parseRange(string $value, bool $asNumber, array &$result): void
    {
        //-- =><= (inklusiv-inklusiv)
        if (strpos($value, '=><=') !== false) {
            $parts = explode('=><=', $value);
            if (count($parts) === 2) {
                $a = $asNumber ? (is_numeric($parts[0]) ? (float)$parts[0] : null) : $parts[0];
                $b = $asNumber ? (is_numeric($parts[1]) ? (float)$parts[1] : null) : $parts[1];
                if ($a !== null && $b !== null) {
                    //-- Grenzen bei Bedarf tauschen
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

    ### parst Operatoren für String-Typen (LIKE, !, Raw-Escaping)
    private function parseStringOperators(string $value): array
    {
        $result = [
            self::OP_EQUAL         => false,
            self::OP_NOT_EQUAL     => false,
            self::OP_LESS_THAN     => false,
            self::OP_GREATER_THAN  => false,
            self::OP_LESS_EQUAL    => false,
            self::OP_GREATER_EQUAL => false,
            self::OP_LIKE          => false,
            self::OP_RAW           => false,
        ];

        $fl = mb_substr($value, 0, 1);
        $sl = mb_substr($value, 1, 1);

        if ($value === self::VAL_UNSET) {
            return $result;
        }

        if ($value === self::VAL_NULL || $value === self::VAL_NOT_NULL) {
            $result[self::OP_EQUAL] = $value;
            return $result;
        }

        //-- Backslash escaped den nachfolgenden Magic-Key
        if ($fl === '\\') {
            $rest = trim(mb_substr($value, 1));
            if ($rest !== '') {
                $result[self::OP_RAW] = $rest;
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
            //-- Doppelte Tilden ignorieren (leere Teile)
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

    ### zentrale Methode zum Bauen von WHERE-Bedingungen (für String und Prepared)
    private function buildWhereConditions(array $def, int $pad, string $mode): array
    {
        $lines = [];

        $tab = 4;
        //--- Behandlung von OR-Gruppen ---
        if (isset($def['_type']) && $def['_type'] === 'or_group') {

            $groupLines = [];
            foreach ($def['conditions'] as $field => $value) {

                $main = $this->getMainParams();
                if (!isset($main[$field]) || !isset($main[$field]['rowName'])) {
                    continue;
                }
                $fieldDef          = $main[$field];
                $fieldDef['value'] = $value;
                $fieldDef          = array_merge($fieldDef, $this->parseOperators($fieldDef));
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

        if (!isset($def['rowName']) || $def['rowName'] === null) {
            return $lines;
        }

        //-- UNSET bedeutet Feld ignorieren
        if ($def[self::OP_EQUAL] === self::VAL_UNSET) {
            return $lines;
        }

        //-- IS NULL / IS NOT NULL
        if ($def[self::OP_EQUAL] === self::VAL_NULL) {
            $lines[] = str_pad($def['rowName'], $pad, ' ') . ' IS NULL';
            return $lines;
        }
        if ($def[self::OP_EQUAL] === self::VAL_NOT_NULL) {
            $lines[] = str_pad($def['rowName'], $pad, ' ') . ' IS NOT NULL';
            return $lines;
        }

        //-- BLANK (leer oder 0 je nach Typ)
        if ($def[self::OP_EQUAL] === self::VAL_BLANK) {
            if (in_array($def['type'], self::NUMERIC_TYPES)) {
                $lines[] = str_pad($def['rowName'], $pad, ' ') . ' = 0';
            } else {
                $lines[] = str_pad($def['rowName'], $pad, ' ') . " = ''";
            }
            return $lines;
        }

        //-- NOT BLANK
        if ($def[self::OP_EQUAL] === self::VAL_NOT_BLANK) {
            if (in_array($def['type'], self::NUMERIC_TYPES)) {
                $lines[] = str_pad($def['rowName'], $pad, ' ') . ' != 0';
            } else {
                $lines[] = str_pad($def['rowName'], $pad, ' ') . " != ''";
            }
            return $lines;
        }

        //-- EMPTY (NULL ODER leer)
        if ($def[self::OP_EQUAL] === self::VAL_EMPTY) {
            if (in_array($def['type'], self::NUMERIC_TYPES)) {
                $lines[] = '(' . str_pad($def['rowName'], $pad, ' ') . ' IS NULL OR ' . str_pad($def['rowName'], $pad, ' ') . ' = 0)';
            } else {
                $lines[] = '(' . str_pad($def['rowName'], $pad, ' ') . ' IS NULL OR ' . str_pad($def['rowName'], $pad, ' ') . " = '')";
            }
            return $lines;
        }

        //-- NOT EMPTY (NOT NULL UND nicht leer)
        if ($def[self::OP_EQUAL] === self::VAL_NOT_EMPTY) {
            if (in_array($def['type'], self::NUMERIC_TYPES)) {
                $lines[] = '(' . str_pad($def['rowName'], $pad, ' ') . ' IS NOT NULL AND ' . str_pad($def['rowName'], $pad, ' ') . ' != 0)';
            } else {
                $lines[] = '(' . str_pad($def['rowName'], $pad, ' ') . ' IS NOT NULL AND ' . str_pad($def['rowName'], $pad, ' ') . " != '')";
            }
            return $lines;
        }

        //-- Raw-Wert (escapeter String, z.B. durch Backslash)
        if ($def[self::OP_RAW] !== false) {
            if ($mode === 'prepared') {
                $lines[] = str_pad($def['rowName'], $pad, ' ') . ' = ?';
                $this->addParam($def[self::OP_RAW], $this->typeToBindParam($def['type']));
            } else {
                $val = $this->db->real_escape_string($def[self::OP_RAW]);
                $lines[] = str_pad($def['rowName'], $pad, ' ') . " = '$val'";
            }
            return $lines;
        }

        $isNumeric = in_array($def['type'], self::NUMERIC_TYPES);
        $isFloat = $def['type'] === self::TYPE_FLOAT;

        $ops = [
            self::OP_EQUAL,
            self::OP_NOT_EQUAL,
            self::OP_LESS_THAN,
            self::OP_GREATER_THAN,
            self::OP_LESS_EQUAL,
            self::OP_GREATER_EQUAL,
        ];

        //-- Alle gesetzten Vergleichsoperatoren durchgehen
        foreach ($ops as $op) {
            if ($def[$op] === false) continue;
            $val = $def[$op];
            $sqlOp = $this->operatorToSql($op);

            if ($mode === 'prepared') {
                $lines[] = str_pad($def['rowName'], $pad, ' ') . " $sqlOp ?";
                $bindType = $isFloat ? 'd' : ($isNumeric ? 'i' : 's');
                $this->addParam($val, $bindType);
            } else {
                $escaped = $this->db->real_escape_string((string)$val);
                if ($isNumeric) {
                    $lines[] = str_pad($def['rowName'], $pad, ' ') . " $sqlOp $escaped";
                } else {
                    $lines[] = str_pad($def['rowName'], $pad, ' ') . " $sqlOp '$escaped'";
                }
            }
        }

        //-- LIKE-Operator
        if ($def[self::OP_LIKE] !== false) {
            $parts = explode('~', $def[self::OP_LIKE]);
            $escapedParts = [];
            foreach ($parts as $part) {
                $escapedParts[] = $this->getEscapedLikeString($part);
            }
            $pattern = implode('%', $escapedParts);

            if ($mode === 'prepared') {
                $lines[] = str_pad($def['rowName'], $pad, ' ') . ' LIKE ?';
                $this->addParam($pattern, 's');
            } else {
                $patternEscaped = $this->db->real_escape_string($pattern);
                $lines[] = str_pad($def['rowName'], $pad, ' ') . " LIKE '$patternEscaped'";
            }
        }

        return $lines;
    }



    ########################### ÖFFENTLICHE QUERY-METHODEN

    ### ermittelt die SELECT-Klausel
    public function getSelectQuery(): string
    {
        $pad = 24;
        $parts = [];
        foreach ($this->getMainParams() as $key => $def) {
            if (isset($def['rowName']) && $def['rowName'] !== null) {
                $quotedName = $this->quoteIdentifier($def['rowName']);
                $parts[] = str_pad($quotedName, $pad, ' ') . ' AS ' . $key;
            }
        }
        return PHP_EOL . 'SELECT' . PHP_EOL . '    ' . implode(',' . PHP_EOL . '    ', $parts);
    }

    ### ermittelt die WHERE-Klausel (inkl. Suche und mysql)
    public function getWhereQuery(): string
    {
        $pad = 20;
        $conditions = [PHP_EOL . 'WHERE 1'];

        foreach ($this->getMainParams() as $def) {
            $lines = $this->buildWhereConditions($def, $pad, 'string');
            $conditions = array_merge($conditions, $lines);
        }

        $filter = $this->getFilterParams();

        //-- Volltextsuche über mehrere Felder
        if (!empty($filter['searchString']) && is_string($filter['searchString']) && trim($filter['searchString'], '~') !== '') {
            $targets = is_string($filter['searchTarget']) ? explode(',', $filter['searchTarget']) : [];
            $searchParts = [];
            $main = $this->getMainParams();
            foreach ($targets as $target) {
                $target = trim($target);
                if (isset($main[$target]) && isset($main[$target]['rowName'])) {
                    $rowName = $this->quoteIdentifier($main[$target]['rowName']);
                    $searchParts[] = str_pad($rowName, $pad - 3, ' ')
                        . " LIKE '%" . $this->db->real_escape_string($this->getEscapedLikeString($filter['searchString'])) . "%'";
                }
            }
            if (!empty($searchParts)) {
                $conditions[] = '(' . PHP_EOL . '           ' . implode(PHP_EOL . '        OR ', $searchParts) . PHP_EOL . '    )';
            }
        }

        //-- Benutzerdefinierter SQL-Teil
        if (!empty($filter['mysql']) && is_string($filter['mysql'])) {
            $conditions[] = '(' . PHP_EOL . '         ' . $filter['mysql'] . PHP_EOL . '        )';
        }

        return implode(PHP_EOL . '    AND ', $conditions);
    }

    ### ermittelt die ORDER BY-Klausel
    public function getOrderQuery(): string
    {
        $filter = $this->getFilterParams();
        if (empty($filter['sort']) || !is_string($filter['sort'])) {
            return '';
        }

        $pad = 24;
        $parts = [];
        $sortList = explode(',', $filter['sort']);
        $main = $this->getMainParams();

        foreach ($sortList as $item) {
            $item = trim($item);
            if ($item === '') continue;

            $sign = mb_substr($item, 0, 1);
            $field = ($sign === self::SORT_DESC || $sign === self::SORT_ASC) ? mb_substr($item, 1) : $item;
            $direction = $sign === self::SORT_DESC ? ' DESC' : ($sign === self::SORT_ASC ? ' ASC' : '');

            if (isset($main[$field])) {
                $quotedField = $this->quoteIdentifier($field);
                $parts[] = str_pad($quotedField, $pad, ' ') . $direction;
            }
        }

        return empty($parts) ? '' : PHP_EOL . 'ORDER BY' . PHP_EOL . '    ' . implode(',' . PHP_EOL . '    ', $parts);
    }

    ### ermittelt die LIMIT-Klausel (für Paginierung)
    public function getLimitQuery()
    {
        $filter = $this->getFilterParams();
        if ($filter['firstElemNumber'] === false) {
            return false;
        }
        return PHP_EOL . 'LIMIT ' . PHP_EOL . '    ' . $filter['firstElemNumber'] . ', ' . $filter['pageSize'] . ' ';
    }

    ### baut den JOIN-Teil (aus joinList)
    private function getJoinQuery(string $defaultType = 'LEFT')
    {
        $joinList = $this->getJoinList();
        if (empty($joinList)) {
            return false;
        }

        $joins = [];
        foreach ($joinList as $info) {
            $type = strtoupper(trim($info['type'] ?? $defaultType));
            if (!in_array($type, ['LEFT', 'RIGHT', 'INNER', 'CROSS', 'STRAIGHT'], true)) {
                $type = 'LEFT';
            }

            $tab = $this->quoteIdentifier(trim($info['tableName']));
            $key = $this->quoteIdentifier(trim($info['tableKey']));
            $col = $this->quoteIdentifier(trim($info['connectColumn']));
            $with = $this->quoteIdentifier(trim($info['connectWith']), true);

            $joins[] = '        ' . $type . ' JOIN ' . PHP_EOL
                . '            ' . $tab . ' ' . $key . PHP_EOL
                . '            ON (' . $key . '.' . $col . ' = ' . $with . ')';
        }

        return implode(PHP_EOL, $joins);
    }

    ### liefert die joinList (oder leeres Array)
    private function getJoinList(): array
    {
        return $this->tableInfo['joinList'] ?? [];
    }

    ### ermittelt den FROM-Teil (Tabelle + Alias)
    public function getTableQuery()
    {
        if (empty($this->tableInfo['tableName']) || empty($this->tableInfo['tableKey'])) {
            return false;
        }
        $table = $this->quoteIdentifier(trim($this->tableInfo['tableName']));
        $alias = $this->quoteIdentifier(trim($this->tableInfo['tableKey']));
        return $table . ' ' . $alias;
    }

    ### baut den kompletten Körper (SELECT … FROM …)
    public function getBodyQuery()
    {
        $table = $this->getTableQuery();
        if (!$table) return false;
        return $this->getSelectQuery() . PHP_EOL . 'FROM ' . PHP_EOL . '    ' . $table;
    }

    ### Query ohne WHERE (für Totalabfragen)
    public function getTotalQuery()
    {
        $body = $this->getBodyQuery();
        if (!$body) return false;
        return $body . PHP_EOL . $this->getJoinQuery() . PHP_EOL . 'WHERE 1';
    }

    ### komplette Query ohne LIMIT
    public function getLimitlessQuery()
    {
        $body = $this->getBodyQuery();
        if (!$body) return false;
        return $body . PHP_EOL . $this->getJoinQuery() . $this->getWhereQuery();
    }

    ### baut eine COUNT-Abfrage (optional total = nur WHERE 1)
    public function getCountQuery(bool $total = false)
    {
        $table = $this->getTableQuery();
        if (!$table) return false;
        $where = $total === true ? PHP_EOL . 'WHERE 1' : $this->getWhereQuery();
        return 'SELECT' . PHP_EOL . '    count(*) as count FROM ' . PHP_EOL . '    ' . $table . $this->getJoinQuery() . $where;
    }

    ### liefert die vollständige SELECT-Query (mit ORDER und LIMIT)
    public function getQuery()
    {
        $limitless = $this->getLimitlessQuery();
        return $limitless ? $limitless . $this->getOrderQuery() . $this->getLimitQuery() : false;
    }



    ########################### PREPARED STATEMENTS

    ### bereitet die Query für Prepared Statements vor (gibt Query, Parameter, Typen zurück)
    public function getPreparedQuery()
    {
        $this->params = [];
        $this->types = '';

        $body = $this->buildBodyPrepared();
        if (!$body) return false;

        $order = $this->buildOrderPrepared();
        $limit = $this->buildLimitPrepared();

        $query = $body . $order . $limit;

        return [
            'query'  => $query,
            'params' => $this->params,
            'types'  => $this->types,
        ];
    }

    ### führt die Query aus (je nach usePrepared mit oder ohne Prepared)
    public function execute(string $fetchMode = 'assoc')
    {
        if ($this->usePrepared) {
            $prep = $this->getPreparedQuery();
            if (!$prep) throw new \Exception('Keine gültige Query.');
            $stmt = $this->db->prepare($prep['query']);
            if (!$stmt) throw new \Exception('Prepare fehlgeschlagen: ' . $this->db->error);
            if (!empty($prep['params'])) {
                $stmt->bind_param($prep['types'], ...$prep['params']);
            }
            $stmt->execute();
            if ($stmt->result_metadata()) {
                $result = $stmt->get_result();
                $data = $fetchMode === 'assoc' ? $result->fetch_all(MYSQLI_ASSOC) : $result->fetch_all(MYSQLI_NUM);
                $result->free();
                return $data;
            }
            return $stmt->affected_rows;
        } else {
            $query = $this->getQuery();
            if (!$query) throw new \Exception('Keine gültige Query.');
            $result = $this->db->query($query);
            if (!$result) throw new \Exception('Query fehlgeschlagen: ' . $this->db->error);
            if ($result instanceof \mysqli_result) {
                $data = $fetchMode === 'assoc' ? $result->fetch_all(MYSQLI_ASSOC) : $result->fetch_all(MYSQLI_NUM);
                $result->free();
                return $data;
            }
            return $this->db->affected_rows;
        }
    }

    ### baut den Körper der Query für Prepared Statements (SELECT … FROM … JOIN … WHERE)
    private function buildBodyPrepared()
    {
        $table = $this->getTableQuery();
        if (!$table) return false;
        return $this->buildSelectPrepared() . PHP_EOL . 'FROM ' . PHP_EOL . '    ' . $table
            . $this->buildJoinPrepared() . $this->buildWherePrepared();
    }

    ### SELECT-Teil für Prepared (identisch zu getSelectQuery)
    private function buildSelectPrepared(): string
    {
        return $this->getSelectQuery();
    }

    ### JOIN-Teil für Prepared
    private function buildJoinPrepared(): string
    {
        $join = $this->getJoinQuery();
        return $join ? PHP_EOL . $join : '';
    }

    ### WHERE-Teil für Prepared (mit Platzhaltern)
    private function buildWherePrepared(): string
    {
        $pad = 20;
        $conditions = [PHP_EOL . 'WHERE 1'];

        foreach ($this->getMainParams() as $def) {
            $lines = $this->buildWhereConditions($def, $pad, 'prepared');
            $conditions = array_merge($conditions, $lines);
        }

        $filter = $this->getFilterParams();

        if (!empty($filter['searchString']) && is_string($filter['searchString']) && trim($filter['searchString'], '~') !== '') {
            $targets = is_string($filter['searchTarget']) ? explode(',', $filter['searchTarget']) : [];
            $searchParts = [];
            $main = $this->getMainParams();
            foreach ($targets as $target) {
                $target = trim($target);
                if (isset($main[$target]) && isset($main[$target]['rowName'])) {
                    $rowName = $this->quoteIdentifier($main[$target]['rowName']);
                    $searchParts[] = str_pad($rowName, $pad - 3, ' ') . " LIKE ?";
                    $this->addParam('%' . $this->getEscapedLikeString($filter['searchString']) . '%', 's');
                }
            }
            if (!empty($searchParts)) {
                $conditions[] = '(' . PHP_EOL . '           ' . implode(PHP_EOL . '        OR ', $searchParts) . PHP_EOL . '        )';
            }
        }

        if (!empty($filter['mysql']) && is_string($filter['mysql'])) {
            $conditions[] = '(' . PHP_EOL . '         ' . $filter['mysql'] . PHP_EOL . '        )';
        }

        return implode(PHP_EOL . '    AND ', $conditions);
    }

    ### ORDER BY für Prepared (identisch)
    private function buildOrderPrepared(): string
    {
        return $this->getOrderQuery();
    }

    ### LIMIT für Prepared (identisch)
    private function buildLimitPrepared()
    {
        return $this->getLimitQuery();
    }

    ### fügt einen Parameter für Prepared Statements hinzu
    private function addParam($value, string $type): void
    {
        $this->params[] = $value;
        $this->types .= $type;
    }



    ########################### HILFSMETHODEN

    ### liefert die Hauptparameter (alle Felder außer Filter)
    private function getMainParams(): array
    {
        $data = $this->resolvedData;
        unset($data[self::GROUP_FILTER]);
        return $data;
    }

    ### liefert die Filter-Parameter
    private function getFilterParams(): array
    {
        return $this->resolvedData[self::GROUP_FILTER] ?? [];
    }

    ### quotet Identifier (Tabellen-/Spaltennamen) mit Backticks, außer bei Funktionen
    private function quoteIdentifier(string $name, bool $splitDot = false): string
    {
        if ($name === null) {
            return '';
        }
        $name = trim($name);
        if ($name === '') return '';

        //-- Wenn die Zeichenfolge eine Klammer enthält, nehmen wir an, es ist ein Funktionsaufruf -> nicht quoten
        if (strpos($name, '(') !== false) {
            return $name;
        }

        if ($splitDot || strpos($name, '.') !== false) {
            $parts = explode('.', $name);
            $quotedParts = [];
            foreach ($parts as $part) {
                $part = trim($part);
                if ($part !== '') {
                    $quotedParts[] = '`' . $part . '`';
                }
            }
            return implode('.', $quotedParts);
        }

        return '`' . $name . '`';
    }

    ### wandelt einen Operator-Konstante in SQL um
    private function operatorToSql(string $op): string
    {
        static $map = [
            self::OP_EQUAL         => '=',
            self::OP_NOT_EQUAL     => '!=',
            self::OP_LESS_THAN     => '<',
            self::OP_GREATER_THAN  => '>',
            self::OP_LESS_EQUAL    => '<=',
            self::OP_GREATER_EQUAL => '>=',
        ];
        return $map[$op] ?? '=';
    }

    ### escaped % und _ für LIKE-Abfragen
    private function getEscapedLikeString(string $string): string
    {
        return str_replace(['%', '_'], ['\%', '\_'], $string);
    }

    ### ermittelt den Bind-Typ für Prepared Statements anhand des Feldtyps
    private function typeToBindParam(string $type): string
    {
        if ($type === self::TYPE_FLOAT) return 'd';
        return in_array($type, self::NUMERIC_TYPES) ? 'i' : 's';
    }

    ### prüft, ob ein benutzerdefinierter SQL-Ausdruck sicher ist (keine DDL/DML, keine Kommentare, kein UNION)
    private function isMysqlSafe(string $sql): bool
    {
        //-- Kommentare entfernen
        while (preg_match('/\/\*.*?\*\//s', $sql)) {
            $sql = preg_replace('/\/\*.*?\*\//s', '', $sql);
        }
        $sql = preg_replace('/--.*$/m', '', $sql);
        $sql = preg_replace('/#.*$/m', '', $sql);

        $blacklist = [
            '/\bDELETE\b/i',
            '/\bINSERT\b/i',
            '/\bUPDATE\b/i',
            '/\bREPLACE\b/i',
            '/\bTRUNCATE\b/i',
            '/\bCREATE\b/i',
            '/\bALTER\b/i',
            '/\bDROP\b/i',
            '/\bRENAME\b/i',
            '/\bEXEC\b/i',
            '/\bEXECUTE\b/i',
            '/\bCALL\b/i',
            '/\bDO\b/i',
            '/\bHANDLER\b/i',
            '/\bFLUSH\b/i',
            '/\bRESET\b/i',
            '/\bPURGE\b/i',
            '/\bINSTALL\b/i',
            '/\bUNINSTALL\b/i',
            '/\bUNION\b/i',
            '/\bLOAD_FILE\b/i',
            '/\bINTO\s+OUTFILE\b/i',
            '/\bINTO\s+DUMPFILE\b/i',
            '/;/',
        ];

        foreach ($blacklist as $pattern) {
            if (preg_match($pattern, $sql)) {
                return false;
            }
        }

        if (strpos($sql, ';') !== false) {
            return false;
        }

        return true;
    }

    //--- Öffentliche Getter (für Debugging / Weiterverarbeitung)

    public function getResolvedData(): array
    {
        return $this->resolvedData;
    }

    public function getWinData(): array
    {
        return $this->resolvedData;
    }

    public function getFieldDefinitions(): array
    {
        return $this->fieldDefinitions;
    }

    public function getSkeletalData(): array
    {
        return $this->fieldDefinitions;
    }
}
