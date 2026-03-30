<?php

declare(strict_types=1);

namespace e2;

/**
 * waveQl – SQL-Query-Builder with operator parsing, security checks, and optional prepared statements
 * ====================================================================================================
 *
 * This class generates a complete SQL query from fieldDefinitions, inputValues, and tableInfo.
 * It supports complex conditions using intuitive operators in the value string,
 * automatic type handling, magic keys for empty values, range queries, joins,
 * pagination, sorting, and full‑text search. Optionally works with prepared statements for increased security.
 *
 *
 * OPERATORS IN THE VALUE STRING
 * -----------------------------
 *   - >10              -> field > 10
 *   - >=10             -> field >= 10
 *   - <10              -> field < 10
 *   - <=10             -> field <= 10
 *   - !10              -> field != 10
 *   - ~text~           -> LIKE '%text%'
 *   - text~            -> LIKE 'text%'
 *   - ~text            -> LIKE '%text'
 *   - a~b~c            -> LIKE '%a%b%c%'
 *   - !NULL            -> IS NOT NULL
 *   - NULL             -> IS NULL
 *   - UNSET            -> ignored
 *
 *
 * MAGIC KEYS (type‑dependent)
 * ---------------------------
 *   - BLANK            -> String: = '' | Numeric: = 0 | Date: = ''
 *   - !BLANK           -> String: != '' | Numeric: != 0 | Date: != ''
 *   - EMPTY            -> String: IS NULL OR = '' | Numeric: IS NULL OR = 0 | Date: IS NULL OR = ''
 *   - !EMPTY           -> String: IS NOT NULL AND != '' | Numeric: IS NOT NULL AND != 0 | Date: IS NOT NULL AND != ''
 *
 *
 * RANGE OPERATORS (numeric & date/time)
 * -------------------------------------
 *   - 10><20           -> 10 < field < 20      (exclusive)
 *   - 10><=20          -> 10 < field <= 20     (exclusive‑inclusive)
 *   - 10>=<20          -> 10 <= field < 20     (inclusive‑exclusive)
 *   - 10=><=20         -> 10 <= field <= 20    (inclusive)
 *   Swapped boundaries are automatically corrected.
 *
 *
 * FILTER PARAMETERS (in the '~filter~' array)
 * -------------------------------------------
 *   - sort          : e.g. '>name,<id' (descending name, ascending id)
 *   - pageNumber    : page number (1‑based)
 *   - pageSize      : entries per page
 *   - searchString  : search term (wrapped in %...%)
 *   - searchTarget  : comma‑separated target fields
 *   - sqlCondition  : custom SQL expression (checked for safety)
 *
 *
 * OR GROUPS (flat group)
 * ----------------------
 *   '~or~' => [ 'field' => 'value', 'other' => '>5' ]
 *   Produces: (field = value OR other > 5)
 *
 *
 * STRUCTURE OF fieldDefinitions
 * -----------------------------
 *   [
 *       'fieldname' => [
 *           'rowName' => 'table.column',   // column name or SQL expression
 *           'type'    => 'string|integer|float|date|time|dateTime|...',
 *           'value'   => 'optional default value'
 *       ],
 *       '~filter~' => [ ... ]   // optional filter defaults
 *   ]
 *
 *
 * STRUCTURE OF inputValues
 * ------------------------
 *   [
 *       'fieldname' => 'value',          // overrides fieldDefinitions['value']
 *       '~filter~' => [ ... ],           // overrides fieldDefinitions['~filter~']
 *       '~or~'     => [ ... ]            // flat OR group
 *   ]
 *
 *
 * STRUCTURE OF tableInfo
 * ----------------------
 *   [
 *       'tableName' => 'main_table',
 *       'tableKey'  => 'alias',
 *       'joinList'  => [
 *           [
 *               'type'          => 'LEFT',   // LEFT, RIGHT, INNER, CROSS, STRAIGHT
 *               'tableName'     => 'table',
 *               'tableKey'      => 'alias',
 *               'connectColumn' => 'column',
 *               'connectWith'   => 'alias.column'
 *           ]
 *       ]
 *   ]
 *
 *
 * AUTOMATIC FIELD EXTENSIONS
 * --------------------------
 * For date/time fields, additional virtual fields are generated:
 *   - fieldYEAR, fieldMONTH, fieldDAY, fieldDATE, fieldTIME, fieldHOUR, fieldMINUTE, fieldUTS
 *
 *
 * USAGE
 * -----
 *   $builder = new \e2\waveQl($db, $tableInfo, $fieldDefinitions, $inputValues, ['prepared' => true]);
 *   $rows = $builder->execute();          // retrieve result
 *   $sql = $builder->getQuery();          // SQL string for debugging
 *
 *
 * SECURITY NOTE
 * -------------
 *   The 'sqlCondition' parameter is checked for dangerous keywords (DROP, DELETE, UNION, …).
 *   If suspicious, it is ignored and an error is logged. Use prepared statements for extra safety.
 *
 * ====================================================================================================
 */
class waveQl
{

    ########################### OPERATOR SHORTCUTS

    private const OP_EQUAL         = 'e';
    private const OP_NOT_EQUAL     = 'ne';
    private const OP_LESS_THAN     = 'lt';
    private const OP_GREATER_THAN  = 'gt';
    private const OP_LESS_EQUAL    = 'lte';
    private const OP_GREATER_EQUAL = 'gte';
    private const OP_LIKE          = 'like';
    private const OP_RAW           = 'raw';


    ########################### SPECIAL VALUES

    private const VAL_UNSET       = 'UNSET';
    private const VAL_NULL        = 'NULL';
    private const VAL_NOT_NULL    = '!NULL';
    private const VAL_BLANK       = 'BLANK';
    private const VAL_NOT_BLANK   = '!BLANK';
    private const VAL_EMPTY       = 'EMPTY';
    private const VAL_NOT_EMPTY   = '!EMPTY';


    ########################### SORT DIRECTIONS

    private const SORT_DESC       = '>';
    private const SORT_ASC        = '<';


    ########################### FIELD TYPES

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


    ########################### GROUP CONSTANTS (FLAT OR GROUPS)

    private const GROUP_OR     = '~or~';
    private const GROUP_FILTER = '~filter~';


    ########################### PROPERTIES

    private array $fieldDefinitions;
    private array $inputValues;
    private array $tableInfo;
    private array $resolvedData = [];
    private $db; // can be mysqli or MockDb
    private bool $usePrepared;

    private array $params = [];
    private string $types = '';


    ########################### CONSTRUCTOR & INITIALISATION


    ##### CONSTRUCTOR: INITIALISES THE BUILDER WITH DATABASE CONNECTION AND CONFIGURATION.

    public function __construct($db, array $tableInfo, array $fieldDefinitions, array $inputValues = [], array $options = [])
    {
        //-- store database connection (for escaping and prepared)
        $this->db               = $db;
        //-- keep table information (including joins)
        $this->tableInfo        = $tableInfo;
        //-- keep field definitions
        $this->fieldDefinitions = $fieldDefinitions;
        //-- store input values (overwrites)
        $this->inputValues      = $inputValues;
        //-- use prepared statements?
        $this->usePrepared      = $options['prepared'] ?? false;

        //-- convert legacy data structures (backwards compatibility)
        $this->migrateLegacyData(true, false);

        //-- actual initialisation: build resolvedData
        $this->initData();
    }


    ##### MIGRATES OLD STRUCTURES (BACKWARDS COMPATIBLE)

    private function migrateLegacyData(bool $migrateTable = true, bool $inputOnly = false): void
    {
        $tableName = $this->tableInfo['tableName'] ?? 'unknown table';

        //-- 1. Table migration (only if requested)
        if ($migrateTable) {
            if (isset($this->tableInfo['leftTableList']) && is_array($this->tableInfo['leftTableList']) && !isset($this->tableInfo['joinList'])) {
                error_log("waveQl (table $tableName): leftTableList is deprecated, use joinList. Please update.");
                $joinList = [];
                foreach ($this->tableInfo['leftTableList'] as $join) {
                    //-- default join type is LEFT if not specified
                    $join['type'] = $join['type'] ?? 'LEFT';
                    $joinList[] = $join;
                }
                $this->tableInfo['joinList'] = $joinList;
                unset($this->tableInfo['leftTableList']);
            }
        }

        //-- Determine which arrays to process
        $targets = $inputOnly ? [&$this->inputValues] : [&$this->fieldDefinitions, &$this->inputValues];

        foreach ($targets as &$target) {
            //-- 2. Migrate filter key 'mysql' → 'sqlCondition'
            foreach (['filter', self::GROUP_FILTER] as $filterKey) {
                if (isset($target[$filterKey]) && is_array($target[$filterKey])) {
                    if (array_key_exists('mysql', $target[$filterKey]) && !array_key_exists('sqlCondition', $target[$filterKey])) {
                        $target[$filterKey]['sqlCondition'] = $target[$filterKey]['mysql'];
                        unset($target[$filterKey]['mysql']);
                        error_log("waveQl (table $tableName): In " . ($inputOnly ? 'inputValues' : 'fieldDefinitions') . "['$filterKey'] the key 'mysql' has been renamed to 'sqlCondition'. Please update.");
                    }
                }
            }

            //-- 3. Migrate filter key 'filter' → '~filter~' (top level)
            if (isset($target['filter']) && !isset($target[self::GROUP_FILTER])) {
                error_log("waveQl (table $tableName): " . ($inputOnly ? 'inputValues' : 'fieldDefinitions') . "['filter'] is deprecated, use '~filter~'. Please update.");
                $target[self::GROUP_FILTER] = $target['filter'];
                unset($target['filter']);
            }
            //-- Remove duplicates if both are set
            if (isset($target['filter']) && isset($target[self::GROUP_FILTER])) {
                error_log("waveQl (table $tableName): " . ($inputOnly ? 'inputValues' : 'fieldDefinitions') . " contains both 'filter' and '~filter~'. 'filter' will be ignored. Please remove 'filter'.");
                unset($target['filter']);
            }
        }
    }


    ##### CENTRAL DATA INITIALISATION: FIELD DEFINITIONS, FILTER, OR GROUPS

    private function initData(): void
    {
        $resolved = [];

        //-- first, take all normal fields from fieldDefinitions
        foreach ($this->fieldDefinitions as $key => $config) {
            //-- process only valid field definitions (no filter, rowName exists)
            if (!is_array($config) || $key === self::GROUP_FILTER) continue;
            if (empty($config['rowName']) || !is_string($config['rowName'])) continue;

            //-- remove HTML tags for safety
            $config['rowName'] = strip_tags($config['rowName']);
            //-- normalise type (default: string)
            $config['type'] = $this->normalizeType($config);
            //-- set default value (empty string if none)
            $config['value'] = isset($config['value']) && is_string($config['value']) ? $config['value'] : '';

            //-- generate automatic fields for date/time (e.g. fieldYEAR)
            $autoFields = $this->generateAutoFields($key, $config);
            foreach ($autoFields as $autoKey => $autoDef) {
                if (!isset($resolved[$autoKey])) {
                    $resolved[$autoKey] = $autoDef;
                }
            }

            //-- add original field
            $resolved[$key] = $config;
        }

        //-- merge filter defaults from fieldDefinitions and inputValues
        $filterDefaults = $this->fieldDefinitions[self::GROUP_FILTER] ?? [];
        $filterInput = $this->inputValues[self::GROUP_FILTER] ?? [];

        $resolved[self::GROUP_FILTER] = $this->buildFilter($filterDefaults, $filterInput);

        $this->resolvedData = $resolved;
        //-- incorporate inputValues into resolvedData (overwrite)
        $this->mergeInputValues();

        //-- parse OR group if present
        $this->parseInputGroups();

        //-- further process filter (validate sorting, pagination, sqlCondition safety)
        $this->processFilter();
        //-- parse operators and compute firstElemNumber
        $this->enlargeData();
    }


    ##### NORMALISES THE FIELD TYPE (DEFAULT: STRING)

    private function normalizeType(array $config): string
    {
        return isset($config['type']) && is_string($config['type'])
            ? strip_tags($config['type'])
            : self::TYPE_STRING;
    }


    ##### GENERATES VIRTUAL FIELDS FOR DATE/TIME (E.G. fieldYEAR, fieldMONTH, …)

    private function generateAutoFields(string $key, array $config): array
    {
        $type = $config['type'];
        //-- only for date/time types
        if (!in_array($type, self::DATETIME_TYPES)) return [];

        $funcs = [];
        //-- date functions
        if ($type === self::TYPE_DATETIME || $type === self::TYPE_DATE) {
            $funcs[self::TYPE_DATE] = 'DATE';
            $funcs[self::TYPE_YEAR] = 'YEAR';
            $funcs[self::TYPE_QUARTER] = 'QUARTER';
            $funcs[self::TYPE_MONTH] = 'MONTH';
            $funcs[self::TYPE_DAY] = 'DAY';
        }
        //-- time functions
        if ($type === self::TYPE_DATETIME || $type === self::TYPE_TIME) {
            $funcs[self::TYPE_TIME] = 'TIME';
            $funcs[self::TYPE_HOUR] = 'HOUR';
            $funcs[self::TYPE_MINUTE] = 'MINUTE';
        }
        //-- Unix timestamp always available
        $funcs[self::TYPE_UTS] = 'UNIX_TIMESTAMP';

        $auto = [];
        foreach ($funcs as $subType => $sqlFunc) {
            $autoKey = $key . strtoupper($subType);
            //-- SQL expression: apply function to rowName
            $auto[$autoKey] = [
                'value'   => '',
                'rowName' => $sqlFunc . '(' . htmlspecialchars($config['rowName']) . ')',
                'type'    => $subType,
            ];
        }
        return $auto;
    }


    ##### BUILDS THE FILTER ARRAY FROM DEFAULTS AND INPUT (ONLY ALLOWED FIELDS)

    private function buildFilter(array $defaults, array $input): array
    {
        $fields = ['sort', 'pageNumber', 'pageSize', 'sqlCondition', 'searchString', 'searchTarget'];
        $filter = [];

        foreach ($fields as $f) {
            //-- input takes precedence over defaults
            if (isset($input[$f]) && (is_string($input[$f]) || is_numeric($input[$f]))) {
                $filter[$f] = trim((string)$input[$f]);
            } elseif (isset($defaults[$f]) && (is_string($defaults[$f]) || is_numeric($defaults[$f]))) {
                $filter[$f] = trim((string)$defaults[$f]);
            } else {
                $filter[$f] = false; // not set
            }
        }
        return $filter;
    }


    ##### OVERWRITES FIELD VALUES WITH INPUTVALUES (IF PRESENT)

    private function mergeInputValues(): void
    {
        foreach ($this->resolvedData as $key => $config) {
            //-- do not overwrite filter, only real fields
            if ($key === self::GROUP_FILTER) continue;
            //-- if a value exists in inputValues, take it
            if (isset($this->inputValues[$key]) && (is_string($this->inputValues[$key]) || is_numeric($this->inputValues[$key]))) {
                $config['value'] = trim((string)$this->inputValues[$key]);
            } else {
                //-- otherwise keep the existing value (from fieldDefinitions)
                $config['value'] = trim((string)($config['value'] ?? ''));
            }
            $this->resolvedData[$key] = $config;
        }
    }


    ##### PARSES THE FLAT OR GROUP FROM INPUTVALUES

    private function parseInputGroups(): void
    {
        if (isset($this->inputValues[self::GROUP_OR]) && is_array($this->inputValues[self::GROUP_OR])) {
            $conditions = $this->inputValues[self::GROUP_OR];
            //-- remove recursive OR groups (only flat allowed)
            foreach ($conditions as $key => $value) {
                if ($key === self::GROUP_OR) {
                    unset($conditions[$key]);
                }
            }
            $groupKey = self::GROUP_OR;
            //-- store special structure for OR group
            $this->resolvedData[$groupKey] = [
                '_type'      => 'or_group',
                'conditions' => $conditions,
                'rowName'    => null,
            ];
        }
    }


    ##### PROCESSES FILTER PARAMETERS (SORTING, SEARCH, PAGINATION, SAFETY CHECK)

    private function processFilter(): void
    {
        $f = &$this->resolvedData[self::GROUP_FILTER];

        //-- validate sorting: allow only existing fields
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
            //-- check if the field exists in fieldDefinitions
            if (isset($this->fieldDefinitions[$item])) {
                $validSorts[] = $sign . $item;
            }
        }
        $f['sort'] = $validSorts ? implode(',', $validSorts) : ($this->fieldDefinitions[self::GROUP_FILTER]['sort'] ?? '');

        //-- validate search targets
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

        //-- pagination: page numbers must be positive
        $pageNumber = abs((int)$f['pageNumber']);
        $pageSize = abs((int)$f['pageSize']);
        if ($pageSize === 0 || $pageNumber === 0) {
            $f['pageNumber'] = false;
            $f['pageSize'] = false;
        } else {
            $f['pageNumber'] = $pageNumber;
            $f['pageSize'] = $pageSize;
        }

        //-- custom SQL safety check
        $sqlCondition = '';
        if (is_string($f['sqlCondition'])) {
            $sqlCondition = trim($f['sqlCondition']);
        }
        if ($sqlCondition !== '') {
            if (!$this->isSqlConditionSafe($sqlCondition)) {
                error_log("waveQl: unsafe sqlCondition parameter blocked: " . $sqlCondition);
                $f['sqlCondition'] = false;
            } else {
                //-- replace placeholders with rowName (e.g. 'field' → 'table.column')
                $sqlCondition = ' ' . $sqlCondition . ' ';
                foreach ($this->fieldDefinitions as $replaceName => $replaceArr) {
                    if ($replaceName !== self::GROUP_FILTER && isset($replaceArr['rowName'])) {
                        $sqlCondition = str_replace(' ' . $replaceName . ' ', ' ' . $replaceArr['rowName'] . ' ', $sqlCondition);
                    }
                }
                $sqlCondition = trim($sqlCondition);
                $f['sqlCondition'] = $sqlCondition !== '' ? $sqlCondition : false;
            }
        } else {
            $f['sqlCondition'] = false;
        }
    }


    ##### ENLARGES DATA WITH COMPUTED VALUES (firstElemNumber, OPERATOR ARRAYS)

    private function enlargeData(): void
    {
        $main = $this->getMainParams();
        foreach ($main as $key => $def) {
            //-- for normal fields (not OR group) parse operators
            if (!isset($def['_type'])) {
                $main[$key] = array_merge($def, $this->parseOperators($def));
            } else {
                $main[$key] = $def;
            }
        }

        $filter = $this->getFilterParams();
        //-- compute offset for LIMIT
        if ($filter['pageNumber'] !== false && $filter['pageSize'] !== false) {
            $filter['firstElemNumber'] = ($filter['pageSize'] * $filter['pageNumber']) - $filter['pageSize'];
        } else {
            $filter['firstElemNumber'] = false;
        }

        $this->resolvedData = $main;
        $this->resolvedData[self::GROUP_FILTER] = $filter;
    }


    ########################### OPERATOR PARSING


    ##### PARSES THE VALUE OF A FIELD AND DETERMINES THE OPERATORS USED

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

        //-- empty value or UNSET is ignored
        if ($value === '' || $value === self::VAL_UNSET) {
            return $result;
        }

        //-- special cases NULL / NOT NULL
        if ($value === self::VAL_NULL || $value === self::VAL_NOT_NULL) {
            $result[self::OP_EQUAL] = $value;
            return $result;
        }

        //-- magic keys (BLANK, !BLANK, EMPTY, !EMPTY)
        if (in_array($value, [self::VAL_BLANK, self::VAL_NOT_BLANK, self::VAL_EMPTY, self::VAL_NOT_EMPTY], true)) {
            $result[self::OP_EQUAL] = $value;
            return $result;
        }

        //-- different parsing logic depending on type
        if (in_array($type, self::NUMERIC_TYPES)) {
            return $this->parseNumericOperators($value);
        } elseif (in_array($type, self::DATETIME_TYPES)) {
            return $this->parseDateTimeOperators($value);
        } else {
            return $this->parseStringOperators($value);
        }
    }


    ##### PARSES OPERATORS FOR NUMERIC TYPES

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

        //-- first check if a range operator is present
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

        //-- comparison operators >, >=, <, <=
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

        //-- inequality operator !
        if ($fl === '!') {
            $rest = trim(mb_substr($value, 1));
            if ($rest !== '' && is_numeric($rest)) {
                $result[self::OP_NOT_EQUAL] = (float)$rest;
            }
            return $result;
        }

        //-- simple equality (no operator)
        if (is_numeric($value)) {
            $result[self::OP_EQUAL] = (float)$value;
        }

        return $result;
    }


    ##### PARSES OPERATORS FOR DATE/TIME TYPES

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


    ##### PARSES RANGE OPERATORS LIKE ><, >=<, ETC.

    private function parseRange(string $value, bool $asNumber, array &$result): void
    {
        //-- =><= (inclusive-inclusive)
        if (strpos($value, '=><=') !== false) {
            $parts = explode('=><=', $value);
            if (count($parts) === 2) {
                $a = $asNumber ? (is_numeric($parts[0]) ? (float)$parts[0] : null) : $parts[0];
                $b = $asNumber ? (is_numeric($parts[1]) ? (float)$parts[1] : null) : $parts[1];
                if ($a !== null && $b !== null) {
                    //-- swap boundaries if needed
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

        //-- =>< (inclusive-exclusive)
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

        //-- ><= (exclusive-inclusive)
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

        //-- >< (exclusive-exclusive)
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


    ##### PARSES OPERATORS FOR STRING TYPES (LIKE, !, RAW ESCAPING)

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

        //-- backslash escapes the following magic key
        if ($fl === '\\') {
            $rest = trim(mb_substr($value, 1));
            if ($rest !== '') {
                $result[self::OP_RAW] = $rest;
            }
            return $result;
        }

        //-- inequality
        if ($fl === '!') {
            $rest = trim(mb_substr($value, 1));
            if ($rest !== '') {
                $result[self::OP_NOT_EQUAL] = $rest;
            }
            return $result;
        }

        //-- LIKE operator: at least one tilde
        if (substr_count($value, '~') >= 1) {
            $parts = explode('~', $value);
            $string = implode('~', $parts);
            //-- ignore double tildes (empty parts)
            if (strpos($string, '~~') === false) {
                $result[self::OP_LIKE] = $string;
            }
            return $result;
        }

        //-- simple equality
        if ($value !== '') {
            $result[self::OP_EQUAL] = $value;
        }

        return $result;
    }


    ########################### BUILDING WHERE CONDITIONS


    ##### CENTRAL METHOD FOR BUILDING WHERE CONDITIONS (FOR STRING AND PREPARED)

    private function buildWhereConditions(array $def, int $pad, string $mode): array
    {
        $lines = [];

        $tab = 4;
        //--- OR group handling ---
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
        //--- end group handling ---

        if (!isset($def['rowName']) || $def['rowName'] === null) {
            return $lines;
        }

        //-- UNSET means ignore the field
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

        //-- BLANK (empty or 0 depending on type)
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

        //-- EMPTY (NULL OR empty)
        if ($def[self::OP_EQUAL] === self::VAL_EMPTY) {
            if (in_array($def['type'], self::NUMERIC_TYPES)) {
                $lines[] = '(' . str_pad($def['rowName'], $pad, ' ') . ' IS NULL OR ' . str_pad($def['rowName'], $pad, ' ') . ' = 0)';
            } else {
                $lines[] = '(' . str_pad($def['rowName'], $pad, ' ') . ' IS NULL OR ' . str_pad($def['rowName'], $pad, ' ') . " = '')";
            }
            return $lines;
        }

        //-- NOT EMPTY (NOT NULL AND not empty)
        if ($def[self::OP_EQUAL] === self::VAL_NOT_EMPTY) {
            if (in_array($def['type'], self::NUMERIC_TYPES)) {
                $lines[] = '(' . str_pad($def['rowName'], $pad, ' ') . ' IS NOT NULL AND ' . str_pad($def['rowName'], $pad, ' ') . ' != 0)';
            } else {
                $lines[] = '(' . str_pad($def['rowName'], $pad, ' ') . ' IS NOT NULL AND ' . str_pad($def['rowName'], $pad, ' ') . " != '')";
            }
            return $lines;
        }

        //-- raw value (escaped string, e.g. via backslash)
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

        //-- iterate over all set comparison operators
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

        //-- LIKE operator
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


    ########################### PUBLIC QUERY METHODS


    ##### SETS NEW INPUT VALUES AND REBUILDS THE INTERNAL STATE.

    public function setInput(array $newInputValues): void
    {
        //-- set new input values
        $this->inputValues = $newInputValues;

        //-- only input-specific migration (skip table migration, only inputValues)
        $this->migrateLegacyData(false, true);

        //-- completely rebuild data structure (resolvedData, filters, operators)
        $this->initData();

        //-- reset prepared statement parameters (belong to previous query)
        $this->params = [];
        $this->types = '';
    }


    ##### BUILDS THE SELECT CLAUSE

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


    ##### BUILDS THE WHERE CLAUSE (INCLUDING SEARCH AND sqlCondition)

    public function getWhereQuery(): string
    {
        $pad = 20;
        $conditions = [PHP_EOL . 'WHERE 1'];

        foreach ($this->getMainParams() as $def) {
            $lines = $this->buildWhereConditions($def, $pad, 'string');
            $conditions = array_merge($conditions, $lines);
        }

        $filter = $this->getFilterParams();

        //-- full‑text search over multiple fields
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

        //-- custom SQL part
        if (!empty($filter['sqlCondition']) && is_string($filter['sqlCondition'])) {
            $conditions[] = '(' . PHP_EOL . '         ' . $filter['sqlCondition'] . PHP_EOL . '        )';
        }

        return implode(PHP_EOL . '    AND ', $conditions);
    }


    ##### BUILDS THE ORDER BY CLAUSE

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


    ##### BUILDS THE LIMIT CLAUSE (FOR PAGINATION)

    public function getLimitQuery(): string|false
    {
        $filter = $this->getFilterParams();
        if ($filter['firstElemNumber'] === false) {
            return false;
        }
        return PHP_EOL . 'LIMIT ' . PHP_EOL . '    ' . $filter['firstElemNumber'] . ', ' . $filter['pageSize'] . ' ';
    }


    ##### BUILDS THE JOIN PART (FROM joinList)

    private function getJoinQuery(string $defaultType = 'LEFT'): string|false
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


    ##### RETURNS THE joinList (OR EMPTY ARRAY)

    private function getJoinList(): array
    {
        return $this->tableInfo['joinList'] ?? [];
    }


    ##### BUILDS THE FROM PART (TABLE + ALIAS)

    public function getTableQuery(): string|false
    {
        if (empty($this->tableInfo['tableName']) || empty($this->tableInfo['tableKey'])) {
            return false;
        }
        $table = $this->quoteIdentifier(trim($this->tableInfo['tableName']));
        $alias = $this->quoteIdentifier(trim($this->tableInfo['tableKey']));
        return $table . ' ' . $alias;
    }


    ##### BUILDS THE COMPLETE BODY (SELECT … FROM …)

    public function getBodyQuery(): string|false
    {
        $table = $this->getTableQuery();
        if (!$table) return false;
        return $this->getSelectQuery() . PHP_EOL . 'FROM ' . PHP_EOL . '    ' . $table;
    }


    ##### QUERY WITHOUT WHERE (FOR TOTAL QUERIES)

    public function getTotalQuery(): string|false
    {
        $body = $this->getBodyQuery();
        if (!$body) return false;
        return $body . PHP_EOL . $this->getJoinQuery() . PHP_EOL . 'WHERE 1';
    }


    ##### COMPLETE QUERY WITHOUT LIMIT

    public function getLimitlessQuery(): string|false
    {
        $body = $this->getBodyQuery();
        if (!$body) return false;
        return $body . PHP_EOL . $this->getJoinQuery() . $this->getWhereQuery();
    }


    ##### BUILDS A COUNT QUERY (OPTIONAL total = ONLY WHERE 1)

    public function getCountQuery(bool $total = false): string|false
    {
        $table = $this->getTableQuery();
        if (!$table) return false;
        $where = $total === true ? PHP_EOL . 'WHERE 1' : $this->getWhereQuery();
        return 'SELECT' . PHP_EOL . '    count(*) as count FROM ' . PHP_EOL . '    ' . $table . $this->getJoinQuery() . $where;
    }


    ##### RETURNS THE FULL SELECT QUERY (WITH ORDER AND LIMIT)

    public function getQuery(): string|false
    {
        $limitless = $this->getLimitlessQuery();
        return $limitless ? $limitless . $this->getOrderQuery() . $this->getLimitQuery() : false;
    }


    ########################### PREPARED STATEMENTS


    ##### PREPARES THE QUERY FOR PREPARED STATEMENTS (RETURNS QUERY, PARAMETERS, TYPES)

    public function getPreparedQuery(): array|false
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


    ##### EXECUTES THE QUERY (DEPENDING ON usePrepared)

    public function execute(string $fetchMode = 'assoc'): array|int
    {
        if ($this->usePrepared) {
            $prep = $this->getPreparedQuery();
            if (!$prep) throw new \Exception('No valid query.');
            $stmt = $this->db->prepare($prep['query']);
            if (!$stmt) throw new \Exception('Prepare failed: ' . $this->db->error);
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
            if (!$query) throw new \Exception('No valid query.');
            $result = $this->db->query($query);
            if (!$result) throw new \Exception('Query failed: ' . $this->db->error);
            if ($result instanceof \mysqli_result) {
                $data = $fetchMode === 'assoc' ? $result->fetch_all(MYSQLI_ASSOC) : $result->fetch_all(MYSQLI_NUM);
                $result->free();
                return $data;
            }
            return $this->db->affected_rows;
        }
    }


    ##### BUILDS THE BODY OF THE QUERY FOR PREPARED STATEMENTS (SELECT … FROM … JOIN … WHERE)

    private function buildBodyPrepared(): string|false
    {
        $table = $this->getTableQuery();
        if (!$table) return false;
        return $this->buildSelectPrepared() . PHP_EOL . 'FROM ' . PHP_EOL . '    ' . $table
            . $this->buildJoinPrepared() . $this->buildWherePrepared();
    }


    ##### SELECT PART FOR PREPARED (IDENTICAL TO getSelectQuery)

    private function buildSelectPrepared(): string
    {
        return $this->getSelectQuery();
    }


    ##### JOIN PART FOR PREPARED

    private function buildJoinPrepared(): string
    {
        $join = $this->getJoinQuery();
        return $join ? PHP_EOL . $join : '';
    }


    ##### WHERE PART FOR PREPARED (WITH PLACEHOLDERS)

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

        if (!empty($filter['sqlCondition']) && is_string($filter['sqlCondition'])) {
            $conditions[] = '(' . PHP_EOL . '         ' . $filter['sqlCondition'] . PHP_EOL . '        )';
        }

        return implode(PHP_EOL . '    AND ', $conditions);
    }


    ##### ORDER BY FOR PREPARED (IDENTICAL)

    private function buildOrderPrepared(): string
    {
        return $this->getOrderQuery();
    }


    ##### LIMIT FOR PREPARED (IDENTICAL)

    private function buildLimitPrepared(): string|false
    {
        return $this->getLimitQuery();
    }


    ##### ADDS A PARAMETER FOR PREPARED STATEMENTS

    private function addParam(mixed $value, string $type): void
    {
        $this->params[] = $value;
        $this->types .= $type;
    }


    ########################### HELPER METHODS


    ##### RETURNS THE MAIN PARAMETERS (ALL FIELDS EXCEPT FILTER)

    private function getMainParams(): array
    {
        $data = $this->resolvedData;
        unset($data[self::GROUP_FILTER]);
        return $data;
    }


    ##### RETURNS THE FILTER PARAMETERS

    private function getFilterParams(): array
    {
        return $this->resolvedData[self::GROUP_FILTER] ?? [];
    }


    ##### QUOTES IDENTIFIERS (TABLE/COLUMN NAMES) WITH BACKTICKS, EXCEPT FUNCTIONS

    private function quoteIdentifier(string $name, bool $splitDot = false): string
    {
        if ($name === '') {
            return '';
        }
        $name = trim($name);
        if ($name === '') return '';

        //-- if the string contains parentheses, assume it's a function call → do not quote
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


    ##### CONVERTS AN OPERATOR CONSTANT TO SQL

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


    ##### ESCAPES % AND _ FOR LIKE QUERIES

    private function getEscapedLikeString(string $string): string
    {
        return str_replace(['%', '_'], ['\%', '\_'], $string);
    }


    ##### DETERMINES THE BIND TYPE FOR PREPARED STATEMENTS BASED ON FIELD TYPE

    private function typeToBindParam(string $type): string
    {
        if ($type === self::TYPE_FLOAT) return 'd';
        return in_array($type, self::NUMERIC_TYPES) ? 'i' : 's';
    }


    ##### CHECKS IF A CUSTOM SQL EXPRESSION IS SAFE (NO DDL/DML, NO COMMENTS, NO UNION)

    private function isSqlConditionSafe(string $sql): bool
    {
        //-- remove comments
        while (preg_match('/\/\*.*?\*\//s', $sql)) {
            $sql = preg_replace('/\/\*.*?\*\//s', '', $sql);
        }
        $sql = preg_replace('/--.*$/m', '', $sql);
        $sql = preg_replace('/#.*$/m', '', $sql);

        $blacklist = [
            '/\bALTER\b/i',
            '/\bCALL\b/i',
            '/\bCREATE\b/i',
            '/\bDELETE\b/i',
            '/\bDO\b/i',
            '/\bDROP\b/i',
            '/\BENCHMARK\s*\(/i',
            '/\bEXEC\b/i',
            '/\bEXECUTE\b/i',
            '/\bFLUSH\b/i',
            '/\bHANDLER\b/i',
            '/\bINSERT\b/i',
            '/\bINSTALL\b/i',
            '/\bINTO\s+DUMPFILE\b/i',
            '/\bINTO\s+OUTFILE\b/i',
            '/\bLOAD_FILE\b/i',
            '/\bPG_SLEEP\b/i',
            '/\bPURGE\b/i',
            '/\bRENAME\b/i',
            '/\bREPLACE\b/i',
            '/\bRESET\b/i',
            '/\bSLEEP\s*\(/i',
            '/\bTRUNCATE\b/i',
            '/\bUNINSTALL\b/i',
            '/\bUNION\b/i',
            '/\bUPDATE\b/i',
            '/\bWAIT_FOR\b/i',
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


    ########################### PUBLIC GETTERS (FOR DEBUGGING / FURTHER PROCESSING)


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
