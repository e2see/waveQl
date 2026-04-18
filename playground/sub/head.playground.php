<?php

$initMessage = '';

// ----------------------------------------------------------------------
// waveQl path and autoload
// ----------------------------------------------------------------------
$waveQlDir     = __DIR__ . '/../../src/';
$requiredFiles = ['waveQl.php'];
$missing       = [];

foreach ($requiredFiles as $file) {
    if (!file_exists($waveQlDir . $file)) {
        $missing[] = $file;
    }
}
if (!empty($missing)) {
    die("Missing waveQl files: " . implode(', ', $missing));
}
foreach ($requiredFiles as $file) {
    require_once $waveQlDir . $file;
}

// ----------------------------------------------------------------------
// Field definitions (original manifest)
// ----------------------------------------------------------------------
$tableManifest = [
    'tableName' => 'countries',
    'tableKey'  => 'c',
    'joinList'  => [
        [
            'type'          => 'LEFT',
            'tableName'     => 'continents',
            'tableKey'      => 'cnt',
            'connectColumn' => 'id',
            'connectWith'   => 'c.continent_id'
        ]
    ]
];

$keyManifest = [
    'AreaKm2'       => ['rowName' => 'c.area_km2',      'type' => 'integer'],
    'Capital'       => ['rowName' => 'c.capital',       'type' => 'string'],
    'ContinentId'   => ['rowName' => 'cnt.id',          'type' => 'integer'],
    'ContinentName' => ['rowName' => 'cnt.name',        'type' => 'string'],
    'CountryName'   => ['rowName' => 'c.name',          'type' => 'string'],
    'FoundedDate'   => ['rowName' => 'c.founded_date',  'type' => 'date'],
    'Population'    => ['rowName' => 'c.population',    'type' => 'integer'],
    '~meta~'        => [
        'sort'         => '>CountryName',
        'pageSize'     => 20,
        'searchTarget' => 'CountryName,Capital,ContinentName'
    ],
];

// ----------------------------------------------------------------------
// Dynamische Strukturen aus den Manifesten (parameterisiert)
// ----------------------------------------------------------------------
function getReadStructure($keyManifest) {
    $read = [];
    foreach ($keyManifest as $logicalName => $config) {
        if ($logicalName === '~meta~') continue;
        $read[$logicalName] = [
            'rowName' => $config['rowName'],
            'type'    => $config['type'],
        ];
    }
    return $read;
}

/**
 * Ermittelt den logischen Namen des Fremdschlüssels (contentID) aus tableManifest und keyManifest.
 */
function getForeignKeyLogicalName($tableManifest, $keyManifest) {
    if (empty($tableManifest['joinList'])) {
        return null;
    }
    foreach ($tableManifest['joinList'] as $join) {
        $fullColumn = $join['tableKey'] . '.' . $join['connectColumn'];
        foreach ($keyManifest as $logicalName => $config) {
            if ($logicalName === '~meta~') continue;
            if (isset($config['rowName']) && $config['rowName'] === $fullColumn) {
                return $logicalName;
            }
        }
    }
    return null;
}

function getWriteStructure($tableManifest, $keyManifest) {
    $read = getReadStructure($keyManifest);
    $foreignLogicalName = getForeignKeyLogicalName($tableManifest, $keyManifest);
    $write = [];
    foreach ($read as $logicalName => $field) {
        $writeField = $field;
        if ($logicalName === $foreignLogicalName) {
            $writeField['isForeignKey'] = true;
            $writeField['required'] = true;
            $writeField['contentID'] = true;
        } else {
            $writeField['isForeignKey'] = false;
            $writeField['required'] = false;
        }
        $write[$logicalName] = $writeField;
    }
    return $write;
}

function getEnrichedFields($keyManifest) {
    $read = getReadStructure($keyManifest);
    $enriched = [];
    foreach ($read as $logicalName => $field) {
        $map = \e2\waveQlCore::getVirtualDateFuncMap($field['type']);
        if (!empty($map)) {
            $enriched[$logicalName] = [
                'type'  => $field['type'],
                'units' => array_map('strtoupper', array_keys($map)),
            ];
        }
    }
    return $enriched;
}

// ----------------------------------------------------------------------
// Dynamische Feldlisten aus Manifesten
// ----------------------------------------------------------------------
$readStructure         = getReadStructure($keyManifest);
$writeStructure        = getWriteStructure($tableManifest, $keyManifest);
$enrichedFields        = getEnrichedFields($keyManifest);
$foreignKeyLogicalName = getForeignKeyLogicalName($tableManifest, $keyManifest);
$originalFields        = array_keys($readStructure);                              // alle Felder außer ~meta~
$dateFields            = array_keys($enrichedFields);                             // nur date/dateTime/time Felder, die waveQl auch unterstützt

// ----------------------------------------------------------------------
// JavaScript-Konfiguration (dynamisch)
// ----------------------------------------------------------------------
$jsAllFields = $originalFields;

// Write-Felder: Alle Felder, die NICHT der Fremdschlüssel sind und NICHT vom Typ dateTime/date/time
$writeFieldNames = [];
foreach ($readStructure as $name => $field) {
    if (strpos($field['rowName'], '.') !== false && $name !== $foreignKeyLogicalName) continue;
    if (in_array($field['type'], ['date', 'dateTime', 'time'])) continue;
    $writeFieldNames[] = $name;
}
if ($foreignKeyLogicalName && !in_array($foreignKeyLogicalName, $writeFieldNames)) {
    $writeFieldNames[] = $foreignKeyLogicalName;
}

// Standard-Fulltext-Felder: dynamisch die ersten drei sinnvollen Felder
$defaultFulltextFields = [];
if (in_array('CountryName', $originalFields)) $defaultFulltextFields[] = 'CountryName';
if (in_array('Capital', $originalFields)) $defaultFulltextFields[]     = 'Capital';
foreach ($readStructure as $name => $field) {
    if (strpos($field['rowName'], '.') !== false && $field['type'] === 'string') {
        $defaultFulltextFields[] = $name;
        break;
    }
}

// Für JavaScript: Alle möglichen virtuellen Suffixe (Vereinigungsmenge aller unterstützten)
$allVirtualSuffixes = [];
foreach ($enrichedFields as $info) {
    $allVirtualSuffixes = array_merge($allVirtualSuffixes, $info['units']);
}
$allVirtualSuffixes = array_values(array_unique($allVirtualSuffixes));

// ----------------------------------------------------------------------
// Datalist suggestions (statisch, aber Feldnamen dynamisch)
// ----------------------------------------------------------------------
$sortExamples       = ['>Population', '<CountryName', '>FoundedDateYEAR', '<AreaKm2'];
$populationExamples = ['>1000000', '50000000><200000000', '!0', '10000000><50000000', '>100000000'];
$areaExamples       = ['>1000000', '5000000><10000000', '<500000', '>2000000'];
$yearExamples       = ['1800><1950', '>1900', '<0', '!NULL', '1500><=2000'];

$countryNames = $capitals = $continentNames = [];
$res = $mysqli->query("SELECT DISTINCT name FROM countries ORDER BY name LIMIT 100");
if ($res) while ($row = $res->fetch_assoc()) $countryNames[] = $row['name'];
$res = $mysqli->query("SELECT DISTINCT capital FROM countries WHERE capital IS NOT NULL AND capital != '' ORDER BY capital LIMIT 100");
if ($res) while ($row = $res->fetch_assoc()) $capitals[] = $row['capital'];
$res = $mysqli->query("SELECT DISTINCT name FROM continents ORDER BY name");
if ($res) while ($row = $res->fetch_assoc()) $continentNames[] = $row['name'];

// ----------------------------------------------------------------------
// Form processing (GET)
// ----------------------------------------------------------------------
$mode   = ($_GET['mode'] ?? '') === 'write' ? 'write' : 'read';
$action = $_GET['action'] ?? '';

// ----------------------------------------------------------------------
// Server-side access restrictions
// ----------------------------------------------------------------------
if ($action !== '') {
    if (($mode === 'read' && !$allowRead) || ($mode === 'write' && !$allowWrite)) {
        $action = '';
    }
}

// Neue Logik: Kein die() mehr, sondern schöne Meldung im Template
$modeError = null;  // Fehlermeldung, falls beide Modi deaktiviert sind

if (!$allowRead && !$allowWrite) {
    // Beide Modi deaktiviert → Fehlermeldung, kein gültiger Modus
    $modeError = 'Read and write modes are both disabled. Please enable at least one in the configuration.';
    $mode = null;
} else {
    // Stelle sicher, dass der gewählte Modus erlaubt ist, sonst fallback
    if ($mode === 'read' && !$allowRead) {
        $mode = 'write';
    } elseif ($mode === 'write' && !$allowWrite) {
        $mode = 'read';
    }
}

$filter       = [];
$metaManifest = [];
$writeData    = [];
$errorMsg     = '';
$sqlOutput    = '';
$resultOutput = '';
$result       = null;
$sql          = null;
$execError    = null;

$options               = $_GET['options'] ?? [];
$opt_virtualDateFields = !isset($options['virtualDateFields']);
$opt_allowSqlCondition = !isset($options['allowSqlCondition']);
$opt_prepared          = !isset($options['prepared']);

$waveOptions = [
    'virtualDateFields' => $opt_virtualDateFields,
    'allowSqlCondition' => $opt_allowSqlCondition,
    'prepared'          => $opt_prepared,
];

// ----------------------------------------------------------------------
// Read mode: gather filter values, meta, fulltext
// ----------------------------------------------------------------------
try {
    if ($mode === 'read') {
        foreach ($originalFields as $field) {
            if (in_array($field, $dateFields)) continue;
            if (isset($_GET[$field]) && $_GET[$field] !== '') {
                $filter[$field] = trim($_GET[$field]);
            }
        }
        foreach ($dateFields as $baseField) {
            $value = trim($_GET[$baseField] ?? '');
            if ($value === '') continue;
            $function = trim($_GET[$baseField . '_function'] ?? 'Original');
            if (!$opt_virtualDateFields) $function = 'Original';
            if ($function === 'Original') {
                $filter[$baseField] = $value;
            } else {
                $filter[$baseField . $function] = $value;
            }
        }
        if (isset($_GET['sort']) && $_GET['sort'] !== '') $metaManifest['sort']                        = trim($_GET['sort']);
        if (isset($_GET['pageSize']) && is_numeric($_GET['pageSize'])) $metaManifest['pageSize']       = (int)$_GET['pageSize'];
        if (isset($_GET['pageNumber']) && is_numeric($_GET['pageNumber'])) $metaManifest['pageNumber'] = (int)$_GET['pageNumber'];
           $fulltextFields                                                                     = $_GET['fulltext_fields'] ?? [];
        if (!is_array($fulltextFields)) $fulltextFields                                        = [];
           $fulltextSearchString                                                               = trim($_GET['fulltext_search_string'] ?? '');
        if (!empty($fulltextFields)) $metaManifest['searchTarget']                                     = implode(',', $fulltextFields);
        if (!empty($fulltextSearchString)) $metaManifest['searchString']                               = $fulltextSearchString;
        if ($opt_allowSqlCondition && isset($_GET['sqlCondition']) && trim($_GET['sqlCondition']) !== '') {
            $metaManifest['sqlCondition'] = trim($_GET['sqlCondition']);
        }
    } else {
        $writeFields = $writeFieldNames;
        foreach ($writeFields as $field) {
            if (isset($_GET[$field]) && $_GET[$field] !== '') {
                $writeData[$field] = trim($_GET[$field]);
            }
        }
        $metaManifest['uniqueKey'] = 'id';
        $metaManifest['returning'] = true;
    }
} catch (Exception $e) {
    $errorMsg = $e->getMessage();
}



// ----------------------------------------------------------------------
// Execution (or preview) with redirect after successful write
// ----------------------------------------------------------------------
$blinkSql    = false;
$blinkResult = false;

if ($action !== '' && $mode !== null) {
    try {
        $wave = \e2\waveQl::create($mysqli, $tableManifest, $keyManifest, $waveOptions);

        if ($mode === 'read') {

            $builder = $wave->read();

            if (!empty($metaManifest)) $builder->setMeta($metaManifest);
            if (!empty($filter)) $builder->setValues($filter);

            if ($action === 'preview') {

                $sql      = $builder->getQuery();
                $blinkSql = true;

            } else {

                $result      = $builder->execute();
                $sql         = $builder->getQuery();
                $blinkSql    = true;
                $blinkResult = true;
            }
        } else {

            $builder = $wave->write();

            if (!empty($metaManifest)) $builder->setMeta($metaManifest);
            if (!empty($writeData)) $builder->setValues($writeData);
            if ($action === 'preview') {
                $sql      = $builder->getQuery();
                $blinkSql = true;
            } else {
                $result      = $builder->execute();
                $sql         = $builder->getQuery();
                $blinkSql    = true;
                $blinkResult = true;

                if ($result !== null) {
                    header('Location: ?mode=read');
                    exit;
                }

            }
        }
    } catch (Exception $e) {
        $execError = $e->getMessage();
    }
}

if ($sql) $sqlOutput = trim(htmlspecialchars($sql));
if ($execError) $errorMsg = $execError;

// Result output for read
if ($result !== null && $mode === 'read' && $action === 'execute') {
    ob_start();
    if (count($result) > 0) {
        echo '<table class="result-table"><thead><tr>';
        foreach (array_keys($result[0]) as $col) echo '<th>' . htmlspecialchars($col) . '</th>';
        echo '</thead><tbody>';
        foreach ($result as $row) {
            echo '<tr>';
            foreach ($row as $value) echo '<td>' . htmlspecialchars($value ?? 'NULL') . '</td>';
            echo '</tr>';
        }
        echo '</tbody></table>';
    } else echo '<p>No records found.</p>';
    $resultOutput = ob_get_clean();
} elseif ($result !== null && $mode === 'write' && $action === 'execute') {
    ob_start();
    if (is_array($result)) echo '<pre>' . print_r($result, true) . '</pre>';
    else echo '<p>Affected/inserted ID: ' . htmlspecialchars($result) . '</p>';
    $resultOutput = ob_get_clean();
}