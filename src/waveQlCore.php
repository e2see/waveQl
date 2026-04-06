<?php

declare(strict_types=1);

namespace e2;

/**
 * =================================================================================================
 * waveQlCore – Abstract base class for read and write operations
 * =================================================================================================
 *
 * This class contains all common logic for all waveQl operations:
 *   - Management of field definitions, table information
 *   - Generation of virtual date fields (e.g. fieldYEAR, fieldMONTH)
 *   - Operator parsing (>, <, ~, NULL, BLANK, EMPTY, range operators)
 *   - Security checks for user-defined SQL conditions
 *   - Join handling, identifier quoting
 *   - Prepared statement parameter collection
 *
 * It is extended by waveQlRead and waveQlWrite and provides the basic
 * data structures as well as helper methods.
 *
 * -------------------------------------------------------------------------------------------------
 * Usage in subclasses:
 *   $core = new waveQlRead($adapter, $tableManifest, $keyManifest);
 *   $core->setValues(['age' => '>18'])->execute();
 * -------------------------------------------------------------------------------------------------
 *
 * =================================================================================================
 */
abstract class waveQlCore
{
    protected waveQlDbInterface $db;
    protected readonly array $tableManifest;
    protected readonly array $keyManifest;
    protected readonly array $metaManifest;

    protected array $keyManifestLive    = [];
    protected array $metaManifestLive   = [];
    protected array $keyManifestLiveOp  = [];
    protected array $metaManifestLiveOp = [];

    protected array $preparedParams = [];
    protected string $preparedTypes = '';

    protected readonly bool $optionVirtualDateFields;
    protected readonly bool $optionAllowSqlCondition;
    protected readonly array $sqlAllowedGroups;

    ########################### OPERATOR SHORTCUTS

    protected const OP_EQUAL         = 'e';
    protected const OP_NOT_EQUAL     = 'ne';
    protected const OP_LESS_THAN     = 'lt';
    protected const OP_GREATER_THAN  = 'gt';
    protected const OP_LESS_EQUAL    = 'lte';
    protected const OP_GREATER_EQUAL = 'gte';
    protected const OP_LIKE          = 'like';
    protected const OP_LITERAL       = 'literal';
    protected const OP_IS_NULL       = 'is_null';
    protected const OP_IS_NOT_NULL   = 'is_not_null';
    protected const OP_IS_BLANK      = 'is_blank';
    protected const OP_IS_NOT_BLANK  = 'is_not_blank';
    protected const OP_IS_EMPTY      = 'is_empty';
    protected const OP_IS_NOT_EMPTY  = 'is_not_empty';

    ########################### SPECIAL VALUES

    protected const VAL_UNSET       = 'UNSET';
    protected const VAL_NULL        = 'NULL';
    protected const VAL_NOT_NULL    = '!NULL';
    protected const VAL_BLANK       = 'BLANK';
    protected const VAL_NOT_BLANK   = '!BLANK';
    protected const VAL_EMPTY       = 'EMPTY';
    protected const VAL_NOT_EMPTY   = '!EMPTY';

    ########################### SORT DIRECTIONS

    protected const SORT_DESC       = '>';
    protected const SORT_ASC        = '<';

    ########################### FIELD TYPES

    protected const TYPE_STRING     = 'string';
    protected const TYPE_INTEGER    = 'integer';
    protected const TYPE_FLOAT      = 'float';
    protected const TYPE_DATE       = 'date';
    protected const TYPE_TIME       = 'time';
    protected const TYPE_DATETIME   = 'dateTime';
    protected const TYPE_YEAR       = 'year';
    protected const TYPE_QUARTER    = 'quarter';
    protected const TYPE_MONTH      = 'month';
    protected const TYPE_DAY        = 'day';
    protected const TYPE_HOUR       = 'hour';
    protected const TYPE_MINUTE     = 'minute';
    protected const TYPE_UTS        = 'uts';

    protected const NUMERIC_TYPES = [
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

    protected const DATETIME_TYPES = [
        self::TYPE_DATE,
        self::TYPE_TIME,
        self::TYPE_DATETIME,
    ];

    protected const ENTRY_TYPES = [
        self::TYPE_STRING,
        self::TYPE_INTEGER,
        self::TYPE_FLOAT,
        self::TYPE_DATE,
        self::TYPE_TIME,
        self::TYPE_DATETIME,
    ];

    ########################### GROUP CONSTANTS

    protected const GROUP_OR     = '~or~';
    protected const GROUP_META   = '~meta~';

    ########################### SQL BLACKLIST GROUPS (CamelCase)

    protected const SQL_BLACKLIST_GROUPS = [
        'basicDanger' => [
            '/\bALTER\b/i',
            '/\bCALL\b/i',
            '/\bCREATE\b/i',
            '/\bDO\b/i',
            '/\bDROP\b/i',
            '/\bFLUSH\b/i',
            '/\bHANDLER\b/i',
            '/\bINSTALL\b/i',
            '/\bPURGE\b/i',
            '/\bRENAME\b/i',
            '/\bREPLACE\b/i',
            '/\bRESET\b/i',
            '/\bTRUNCATE\b/i',
            '/\bUNINSTALL\b/i',
            '/\bWAIT_FOR\b/i',
        ],
        'systemTables' => [
            '/\bINFORMATION_SCHEMA\b/i',
            '/\bmysql\.(user|db|tables_priv|columns_priv)\b/i',
            '/\bperformance_schema\b/i',
            '/\bsys\b/i',
        ],
        'fileOperations' => [
            '/\bLOAD\s+DATA\s+INFILE\b/i',
            '/\bLOAD\s+XML\s+INFILE\b/i',
            '/\bINTO\s+DUMPFILE\b/i',
            '/\bINTO\s+OUTFILE\b/i',
            '/\bLOAD_FILE\b/i',
        ],
        'xmlError' => [
            '/\bEXTRACTVALUE\s*\(/i',
            '/\bUPDATEXML\s*\(/i',
            '/\bNAME_CONST\s*\(/i',
        ],
        'timingAttacks' => [
            '/\bGET_LOCK\s*\(/i',
            '/\bRELEASE_LOCK\s*\(/i',
            '/\bIS_FREE_LOCK\s*\(/i',
            '/\bIS_USED_LOCK\s*\(/i',
            '/\bSLEEP\s*\(/i',
            '/\bBENCHMARK\s*\(/i',
        ],
        'dynamicSql' => [
            '/\bPREPARE\s+.*\s+FROM\b/i',
            '/\bDEALLOCATE\s+PREPARE\b/i',
            '/\bEXECUTE\b/i',
        ],
        'systemInfo' => [
            '/\bVERSION\s*\(/i',
            '/\bDATABASE\s*\(/i',
            '/\bSCHEMA\s*\(/i',
            '/\bUSER\s*\(/i',
            '/\bCURRENT_USER\s*\(/i',
            '/\bSESSION_USER\s*\(/i',
            '/\bSYSTEM_USER\s*\(/i',
            '/@@[a-zA-Z_]+/i',
        ],
        'dataManipulation' => [
            '/\bINSERT\b/i',
            '/\bUPDATE\b/i',
            '/\bDELETE\b/i',
        ],
        'misc' => [
            '/\bPROCEDURE\s+ANALYSE\s*\(/i',
            '/\bUNION\b/i',
            '/;/',
        ],
    ];

    ########################### CONSTRUCTOR & INITIALIZATION

    public function __construct(waveQlDbInterface $db, array $tableManifest, array $keyManifest, array $options = [])
    {

        ksort($tableManifest);
        ksort($keyManifest);
        ksort($options);

        $this->optionVirtualDateFields = $options['virtualDateFields'] ?? true;
        $this->optionAllowSqlCondition = $options['allowSqlCondition'] ?? false;
        $this->sqlAllowedGroups        = $options['sqlAllowedGroups'] ?? [];


        $this->db            = $db;
        $Migrated            = $this->migrateLegacyData($tableManifest, $keyManifest);

        // -- Validate table manifest
        $this->tableManifest = $this->validateTableManifest($Migrated['tableManifest']);

        // -- Validate key manifest (removes ~meta~)
        $keyManifest = $this->validateKeyManifest($Migrated['keyManifest']);

        // -- Generate virtual date fields
        if ($this->optionVirtualDateFields) {
            $orderedKeyManifest = [];
            foreach ($keyManifest as $k => $cfg) {
                $orderedKeyManifest[$k] = $cfg;
                $autoFields = $this->generateAutoFields($k, $cfg);
                foreach ($autoFields as $autoKey => $autoDef) {
                    $orderedKeyManifest[$autoKey] = $autoDef;
                }
            }
            $keyManifest = $orderedKeyManifest;
        }

        // -- Set final keyManifest (without ~meta~)
        $this->keyManifest = $keyManifest;
        $this->keyManifestLive = $this->keyManifest;

        // -- Calculate meta manifest (now uses $this->keyManifest for validation)
        $rawMeta = $Migrated['keyManifest'][self::GROUP_META] ?? [];
        $this->metaManifest = $this->validateMergeFullfillMetaManifest($rawMeta, $Migrated['tableManifest'][self::GROUP_META] ?? []);
        $this->metaManifestLive = $this->metaManifest;

        $this->updateLive(null, null, true, true);
        unset($Migrated, $keyManifest);
    }

    ##### Updates live data (values, meta, operators).
    protected function updateLive(?array $values = null, ?array $meta = null, bool $resetValues = true, bool $resetMeta = true): void
    {
        //-- Update values
        if ($values !== null) {
            if ($resetValues) {
                $this->keyManifestLive = $this->keyManifest;
                unset($this->keyManifestLive[self::GROUP_OR]);
            }
            $this->updateValuesLive($values, !$resetValues);
        }

        //-- OR group special handling
        if ($values !== null && isset($values[self::GROUP_OR]) && is_array($values[self::GROUP_OR])) {

            $conditions = $values[self::GROUP_OR];
            unset($conditions[self::GROUP_OR]);

            ksort($conditions);

            $this->keyManifestLive[self::GROUP_OR] = [
                'type'       => self::GROUP_OR . 'item',
                'conditions' => $conditions,
            ];

        } elseif ($resetValues && isset($this->keyManifestLive[self::GROUP_OR])) {
            unset($this->keyManifestLive[self::GROUP_OR]);
        }

        //-- Update meta
        if ($meta !== null) {
            if ($resetMeta) {
                $this->metaManifestLive = $this->metaManifest;
            }
            $this->updateMetaLive($meta, !$resetMeta);
        }

        //-- Re-parse operators
        $this->refreshOperators();
    }

    ##### Overwrites values in keyManifestLive.
    protected function updateValuesLive(array $keyValArr, bool $merge = false): void
    {
        foreach ($this->keyManifest as $key => $config) {
            $val = ($merge) ? $config['value'] : '';
            if (isset($keyValArr[$key])) {
                $val = trim((string)$keyValArr[$key]);
            }
            $this->keyManifestLive[$key]['value'] = $val;
        }
    }

    ##### Overwrites meta values and validates pagination.
    protected function updateMetaLive(array $metaArr, bool $merge = true): void
    {
        if ($merge) {
            // New values overwrite live data, live data overwrite initial defaults
            $this->metaManifestLive = $this->validateMergeFullfillMetaManifest($metaArr, $this->metaManifestLive, $this->metaManifest);
        } else {
            // Reset: New values overwrite initial defaults
            $this->metaManifestLive = $this->validateMergeFullfillMetaManifest($metaArr, $this->metaManifest);
        }
    }

    ##### Fills missing meta fields with defaults.
    protected function mergeAndFullfillMeta(array $prio1, array $prio2 = [], array $prio3 = []): array
    {
        $fields = ['sort', 'pageNumber', 'pageSize', 'firstElemNumber', 'sqlCondition', 'searchString', 'searchTarget'];
        $meta = [];
        foreach ($fields as $f) {
            if (isset($prio1[$f]) && (is_string($prio1[$f]) || is_numeric($prio1[$f]))) {
                $meta[$f] = trim((string)$prio1[$f]);
            } elseif (isset($prio2[$f]) && (is_string($prio2[$f]) || is_numeric($prio2[$f]))) {
                $meta[$f] = trim((string)$prio2[$f]);
            } elseif (isset($prio3[$f]) && (is_string($prio3[$f]) || is_numeric($prio3[$f]))) {
                $meta[$f] = trim((string)$prio3[$f]);
            } else {
                $meta[$f] = '';
            }
            if ($meta[$f] === self::VAL_UNSET) {
                $meta[$f] = '';
            }
        }
        return $meta;
    }

    ##### Validates sorting information in the meta manifest.
    protected function validateMetaManifest_sort(string $sortValue = '', string $fallback = ''): string
    {
        $sortItems = [];
        if (is_string($sortValue) && trim($sortValue) !== '') {
            $sortItems = explode(',', $sortValue);
        }
        $validSorts = [];
        foreach ($sortItems as $item) {
            $item      = trim($item);
            $sign      = '';
            $maybeSign = mb_substr($item, 0, 1);
            if ($maybeSign === self::SORT_DESC || $maybeSign === self::SORT_ASC) {
                $sign = $maybeSign;
                $item = trim(mb_substr($item, 1));
            }
            if (isset($this->keyManifest[$item])) {
                $validSorts[] = $sign . $item;
            }
        }
        return $validSorts ? implode(',', $validSorts) : $fallback;
    }

    ##### Validates the entire meta manifest (sorting, search, pagination, SQL).
    protected function validateMergeFullfillMetaManifest(array $metaArr1 = [], array $metaArr2 = [], array $metaArr3 = []): array
    {
        $metaArr = $this->mergeAndFullfillMeta($metaArr1, $metaArr2, $metaArr3);

        $metaArr['sort'] = $this->validateMetaManifest_sort($metaArr['sort'] ?? '', $this->keyManifest[self::GROUP_META]['sort'] ?? '');

        //-- Validate search targets
        $targetItems = [];
        if (is_string($metaArr['searchTarget']) && $metaArr['searchTarget'] !== '') {
            $targetItems = explode(',', $metaArr['searchTarget']);
        }
        $validTargets = [];
        foreach ($targetItems as $item) {
            $item = trim($item);
            if (isset($this->keyManifest[$item])) {
                $validTargets[] = $item;
            }
        }
        $metaArr['searchTarget'] = $validTargets ? implode(',', $validTargets) : ($this->keyManifest[self::GROUP_META]['searchTarget'] ?? '');

        $this->validateMetaManifest_pagination($metaArr);
        $this->validateMetaManifest_sqlCondition($metaArr);

        return $metaArr;
    }


    ##### Calculates firstElemNumber from pageNumber and pageSize.
    protected function validateMetaManifest_sqlCondition(array &$input): void
    {
        //-- Security check for custom SQL
        $sqlCondition = '';
        if (is_string($input['sqlCondition'])) {
            $sqlCondition = trim($input['sqlCondition']);
        }
        if ($sqlCondition !== '') {
            if (!$this->optionAllowSqlCondition) {
                throw new waveQlSecurityException('Custom SQL conditions are disabled. Set allowSqlCondition=>true to enable.');
            }

            $sqlCondition = $this->replaceLogicalNamesInSql($sqlCondition);
            $this->isSqlConditionSafe($sqlCondition);
            $sqlCondition = trim($sqlCondition);
            $input['sqlCondition'] = $sqlCondition !== '' ? $sqlCondition : false;
        } else {
            $input['sqlCondition'] = false;
        }
    }



    protected function replaceLogicalNamesInSql(string $sql): string
    {
        $logicalNames = [];
        foreach ($this->keyManifest as $name => $config) {
            if ($name === self::GROUP_META || $name === self::GROUP_OR) {
                continue;
            }
            $logicalNames[] = $name;
        }
        if (empty($logicalNames)) return $sql;

        // Sort descending by length
        usort($logicalNames, fn($a, $b) => strlen($b) - strlen($a));

        $replacements = [];
        foreach ($logicalNames as $name) {
            $detail = $this->getFieldDetail($name);
            if (!$detail || empty($detail['fullQuoted'])) continue;
            $quotedFull = $detail['fullQuoted']; // e.g. `pr`.`extension`
            $pattern = '/\b' . preg_quote($name, '/') . '\b/i';
            $replacements[$pattern] = $quotedFull;
        }

        return preg_replace(array_keys($replacements), array_values($replacements), $sql);
    }



    ##### Calculates firstElemNumber from pageNumber and pageSize.
    protected function validateMetaManifest_pagination(array &$input): void
    {
        $pageSize = abs((int)($input['pageSize'] ?? 0));
        $pageNumber = abs((int)($input['pageNumber'] ?? 0));
        if ($pageSize > 0 && $pageNumber === 0) {
            $pageNumber = 1;
        }
        if ($pageSize === 0 || $pageNumber === 0) {
            $input['pageNumber'] = false;
            $input['pageSize'] = false;
            $input['firstElemNumber'] = false;
        } else {
            $input['pageNumber'] = $pageNumber;
            $input['pageSize'] = $pageSize;
            $input['firstElemNumber'] = ($pageSize * $pageNumber) - $pageSize;
        }
    }

    ##### Validates a single table element (main table or join).
    protected function validateTableManifestElem(array $userInput = [], bool $join = false): array
    {
        $return = [];
        $tKey     = ['tableName', 'tableKey'];
        $tKeyJoin = ['connectColumn', 'connectWith'];
        $keys = $join ? array_merge($tKey, $tKeyJoin) : $tKey;

        foreach ($keys as $key) {
            if (!isset($userInput[$key]) or !is_string($userInput[$key])) {
                if ($key === 'tableName') {
                    throw new waveQlInvalidArgumentException('Missing or invalid ' . $key . ' in tableManifest');
                }
                if ($join && in_array($key, ['connectColumn', 'connectWith'])) {
                    throw new waveQlInvalidArgumentException('Empty ' . $key . ' in tableManifest');
                }
                $value = null;
            } else {
                $value = trim($userInput[$key]);
                if (empty($value)) {
                    if ($key === 'tableName') {
                        throw new waveQlInvalidArgumentException('Empty ' . $key . ' in tableManifest');
                    }
                    if ($join && in_array($key, ['connectColumn', 'connectWith'])) {
                        throw new waveQlInvalidArgumentException('Empty ' . $key . ' in tableManifest');
                    }
                    $value = null;
                }
            }
            $return[$key] = $value;
        }
        return $return;
    }

    ##### Validates the entire table manifest including joinList.
    protected function validateTableManifest(array $userInput = []): array
    {
        $joinList = $userInput['joinList'] ?? [];
        unset($userInput['joinList']);
        $return = $this->validateTableManifestElem($userInput, false);
        if (!empty($joinList)) {
            $return['joinList'] = [];
            foreach ($joinList as $join) {
                $validatedJoin = $this->validateTableManifestElem($join, true);
                $return['joinList'][] = $validatedJoin;
            }
        }
        return $return;
    }

    ##### Validates the key manifest (field definitions).
    protected function validateKeyManifest(array $userInput = []): array
    {
        $return = [];
        $i = 0;
        foreach ($userInput as $key => $config) {
            $i++;
            if (!is_string($key) or !is_array($config)) continue;
            $key = trim($key);
            if (empty($key) || strtolower($key) === strtolower(self::GROUP_META)) continue;

            $cTypes = ['rowName', 'type', 'value'];
            foreach ($cTypes as $cType) {
                if (!isset($config[$cType]) or is_array($config[$cType]) or empty(trim((string) $config[$cType]))) {
                    $config[$cType] = null;
                    continue;
                }
            }
            $config['rowName'] = trim($config['rowName']);
            if ($config['rowName'] === null) {
                throw new waveQlInvalidArgumentException('Empty rowName in keyManifest (Key: ' . $i . ')');
            }
            if (!preg_match('/^[a-zA-Z0-9_.()]+$/', $config['rowName'])) {
                throw new waveQlInvalidArgumentException('Invalid rowName: ' . $config['rowName']);
            }
            if ($config['type'] === null) {
                $config['type'] = self::TYPE_STRING;
            }
            if (!in_array($config['type'], self::ENTRY_TYPES)) {
                error_log("waveQl: Invalid type for {$config['rowName']}: {$config['type']}. Converting to string.");
                $config['type'] = self::TYPE_STRING;
            }
            if ($config['value'] === null) {
                $config['value'] = '';
            }
            $return[$key] = $config;
        }
        return $return;
    }

    ##### Migrates legacy data structures (leftTableList, filter, mysql) to new keys.
    protected function migrateLegacyData(array $userTM, array $userKM): array
    {
        $tableName = $userTM['tableName'] ?? 'unknown table';

        //-- leftTableList → joinList
        if (isset($userTM['leftTableList']) && is_array($userTM['leftTableList']) && !isset($userTM['joinList'])) {
            error_log("waveQl (table $tableName): leftTableList is deprecated, use joinList. Please update.");
            $joinList = [];
            foreach ($userTM['leftTableList'] as $join) {
                $join['type'] = $join['type'] ?? 'LEFT';
                $joinList[] = $join;
            }
            $userTM['joinList'] = $joinList;
            unset($userTM['leftTableList']);
        }

        //-- Migration for keyManifest: filter → ~meta~
        $oldMetas = ['filter', '~filter~'];
        foreach ($oldMetas as $oMeta) {
            if (isset($userKM[$oMeta])) {
                if (isset($userKM[self::GROUP_META])) {
                    error_log("waveQl (table $tableName): keyManifest already contains '" . self::GROUP_META . "'. keyManifest['$oMeta'] will be ignored, please use only '" . self::GROUP_META . "'.");
                    $userKM[self::GROUP_META] = $userKM[$oMeta];
                    unset($userKM[$oMeta]);
                } else {
                    error_log("waveQl (table $tableName): keyManifest['$oMeta'] is deprecated, use '" . self::GROUP_META . "'.");
                    $userKM[self::GROUP_META] = $userKM[$oMeta];
                    unset($userKM[$oMeta]);
                }
            }
            //-- mysql → sqlCondition inside ~meta~ block
            if (isset($userKM[self::GROUP_META]) && is_array($userKM[self::GROUP_META])) {
                if (array_key_exists('mysql', $userKM[self::GROUP_META]) && !array_key_exists('sqlCondition', $userKM[self::GROUP_META])) {
                    $userKM[self::GROUP_META]['sqlCondition'] = $userKM[self::GROUP_META]['mysql'];
                    unset($userKM[self::GROUP_META]['mysql']);
                    error_log("waveQl (table $tableName): In keyManifest['" . self::GROUP_META . "'] 'mysql' was renamed to 'sqlCondition'.");
                }
            }
        }
        return [
            'tableManifest' => $userTM,
            'keyManifest'   => $userKM,
        ];
    }



    ########################### VIRTUAL DATE FIELDS

    ##### Returns the mapping of sub-types to SQL functions for a given date/time type.
    ##### Used internally for auto-field generation and externally for UI.
    public static function getVirtualDateFuncMap(string $type): array
    {

        if (!in_array($type, self::DATETIME_TYPES)) return [];

        $map = [];
        if (in_array($type, [self::TYPE_DATETIME, self::TYPE_DATE])) {
            $map[self::TYPE_DATE]    = 'DATE';
            $map[self::TYPE_YEAR]    = 'YEAR';
            $map[self::TYPE_QUARTER] = 'QUARTER';
            $map[self::TYPE_MONTH]   = 'MONTH';
            $map[self::TYPE_DAY]     = 'DAY';
        }
        if (in_array($type, [self::TYPE_DATETIME, self::TYPE_TIME])) {
            $map[self::TYPE_TIME]   = 'TIME';
            $map[self::TYPE_HOUR]   = 'HOUR';
            $map[self::TYPE_MINUTE] = 'MINUTE';
        }
        if (in_array($type, [self::TYPE_DATETIME, self::TYPE_DATE, self::TYPE_TIME])) {
            $map[self::TYPE_UTS] = 'UNIX_TIMESTAMP';
        }

        asort($map);

        return $map;
    }

    ##### Generates virtual fields for date/time (e.g. fieldYEAR).
    protected function generateAutoFields(string $key, array $config): array
    {

        $funcs = self::getVirtualDateFuncMap($config['type'] ?? '');
        $auto  = [];
        if (!empty($funcs)) {

            foreach ($funcs as $subType => $sqlFunc) {
                $autoKey = $key . strtoupper($subType);
                $auto[$autoKey] = [
                    'value'   => '',
                    'rowName' => $sqlFunc . '(' . $config['rowName'] . ')',
                    'type'    => $subType,
                ];
            }
        }
        return $auto;
    }


    ########################### HELPER METHODS

    ##### Quotes an identifier (table/column name) with backticks, except for functions.
    protected function quoteMe(string $name, bool $splitDot = false): string
    {
        return $this->db->quoteIdentifier($name, $splitDot);
    }

    ##### Returns the joinList (or empty array).
    protected function getJoinList(): array
    {
        return $this->tableManifest['joinList'] ?? [];
    }

    ##### Builds the JOIN clause from the joinList.
    protected function getJoinQuery(string $defaultType = 'LEFT'): string|false
    {
        $joinList = $this->getJoinList();
        if (empty($joinList)) return false;

        $joins = [];
        foreach ($joinList as $info) {
            $type = strtoupper(trim($info['type'] ?? $defaultType));
            if (!in_array($type, ['LEFT', 'RIGHT', 'INNER', 'CROSS', 'STRAIGHT'], true)) {
                $type = 'LEFT';
            }
            $tab  = $this->quoteMe(trim($info['tableName']));
            $key  = $this->quoteMe(trim($info['tableKey']));
            $col  = $this->quoteMe(trim($info['connectColumn']));
            $with = $this->quoteMe(trim($info['connectWith']), true);
            $joins[] = '        ' . $type . ' JOIN ' . PHP_EOL
                . '            ' . $tab . ' ' . $key . PHP_EOL
                . '            ON (' . $key . '.' . $col . ' = ' . $with . ')';
        }
        return implode(PHP_EOL, $joins);
    }

    ##### Returns the FROM part (table + alias).
    protected function getTableQuery(): string|false
    {
        $detail = $this->getTableDetail($this->tableManifest['tableName'] ?? '');
        if (!$detail) return false;
        if ($detail['aliasQuoted'] !== null) {
            return $detail['nameQuoted'] . ' ' . $detail['aliasQuoted'];
        }
        return $detail['nameQuoted'];
    }

    ########################### DETAIL METHODS

    ##### Returns a detail array for a name with optional alias.
    protected function getElemDetail(string $name, ?string $alias = null): array
    {
        $nameQuoted = $this->quoteMe($name);
        $aliasQuoted = $alias !== null ? $this->quoteMe($alias) : null;
        $full = $alias !== null ? $alias . '.' . $name : $name;
        $fullQuoted = $alias !== null ? $aliasQuoted . '.' . $nameQuoted : $nameQuoted;
        return [
            'alias'       => $alias,
            'aliasQuoted' => $aliasQuoted,
            'full'        => $full,
            'fullQuoted'  => $fullQuoted,
            'name'        => $name,
            'nameQuoted'  => $nameQuoted,
        ];
    }

    ##### Returns the components of a table by its name or alias.
    protected function getTableDetail(string $name): array|false
    {
        $search = trim($name);
        if ($search === '') return false;

        //-- Check main table
        $mainTable = $this->tableManifest['tableName'] ?? '';
        $mainAlias = $this->tableManifest['tableKey'] ?? '';
        if ($search === $mainTable || $search === $mainAlias) {
            return $this->getElemDetail($mainTable, $mainAlias);
        }

        //-- Search joins
        foreach ($this->getJoinList() as $join) {
            $joinTable = $join['tableName'] ?? '';
            $joinAlias = $join['tableKey'] ?? '';
            if ($search === $joinTable || $search === $joinAlias) {
                return $this->getElemDetail($joinTable, $joinAlias);
            }
        }
        return false;
    }

    ##### Returns the components of a field by its logical key.
    protected function getFieldDetail(string $logicalName): array|false
    {
        if (!isset($this->keyManifest[$logicalName]) || !is_array($this->keyManifest[$logicalName])) {
            return false;
        }
        $rowName = $this->keyManifest[$logicalName]['rowName'] ?? '';
        if (!is_string($rowName)) return false;
        $parts  = explode('.', $rowName, 2);
        $column = trim(end($parts));
        $alias  = count($parts) === 2 ? trim($parts[0]) : null;
        return $this->getElemDetail($column, $alias);
    }

    ########################### FURTHER HELPER METHODS

    ##### Adds a parameter for prepared statements.
    protected function addParam(string|int|float $value, string $type): void
    {
        $this->preparedParams[] = $value;
        $this->preparedTypes .= $type;
    }

    ##### Escapes % and _ for LIKE queries.
    protected function getEscapedLikeString(string $string): string
    {
        return str_replace(['%', '_'], ['\%', '\_'], $string);
    }

    ##### Checks whether a custom SQL expression is safe.
    protected function isSqlConditionSafe(string $sql): void
    {
        //-- Remove comments
        while (preg_match('/\/\*.*?\*\//s', $sql)) {
            $sql = preg_replace('/\/\*.*?\*\//s', '', $sql);
        }
        $sql = preg_replace('/--.*$/m', '|', $sql);
        $sql = preg_replace('/#.*$/m', '|', $sql);

        foreach (self::SQL_BLACKLIST_GROUPS as $groupKey => $patterns) {
            if (in_array($groupKey, $this->sqlAllowedGroups, true)) {
                continue;
            }
            foreach ($patterns as $pattern) {
                if (preg_match($pattern, $sql)) {
                    throw new waveQlSecurityException("Unsafe SQL condition detected (group: $groupKey): " . $sql);
                }
            }
        }
    }

    ##### Converts an operator shortcut to SQL.
    protected function operatorToSql(string $op): string
    {
        $map = [
            self::OP_EQUAL         => '=',
            self::OP_NOT_EQUAL     => '!=',
            self::OP_LESS_THAN     => '<',
            self::OP_GREATER_THAN  => '>',
            self::OP_LESS_EQUAL    => '<=',
            self::OP_GREATER_EQUAL => '>=',
        ];
        return $map[$op] ?? '=';
    }

    ##### Returns the bind type for prepared statements based on field type.
    protected function typeToBindParam(string $type): string
    {
        if ($type === self::TYPE_FLOAT) return 'd';
        return in_array($type, self::NUMERIC_TYPES) ? 'i' : 's';
    }



    ########################### MANIFEST GETTERS (EPIC 4)

    public function getManifest(string $type, string $status = 'live'): array
    {
        switch ($type) {
            case 'table':
                if ($status === 'initial' || $status === 'live') {
                    return $this->tableManifest;
                }
                break;
            case 'key':
                return match ($status) {
                    'initial' => $this->keyManifest,
                    'live'    => $this->keyManifestLive,
                    'liveOp'  => $this->keyManifestLiveOp,
                    default   => throw new waveQlInvalidArgumentException("Invalid status '$status' for key manifest."),
                };
            case 'meta':
                return match ($status) {
                    'initial' => $this->metaManifest,
                    'live', 'liveOp' => $this->metaManifestLive,
                    default   => throw new waveQlInvalidArgumentException("Invalid status '$status' for meta manifest."),
                };
            default:
                throw new waveQlInvalidArgumentException("Invalid manifest type '$type'. Allowed: 'table', 'key', 'meta'.");
        }
        throw new waveQlInvalidArgumentException("For type '$type' status '$status' is not available.");
    }

    ########################### OPERATOR REFRESH (EPIC 4)

    protected function refreshOperators(): void
    {
        // Will be implemented in waveQlRead
    }

    ########################### ABSTRACT METHODS

    abstract public function setMeta(array $meta);
    abstract public function setValues(array $values);
    abstract public function execute();
    abstract public function getQuery(): string|false;
}
