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
// Datalist suggestions
// ----------------------------------------------------------------------
$sortExamples       = ['>Population', '<CountryName', '>FoundedYear', '<AreaKm2', '>FoundedDateYEAR'];
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
// Field definitions (original manifest, NO virtual fields here)
// ----------------------------------------------------------------------
$baseKeyManifest = [
    'CountryName'    => ['rowName' => 'c.name',          'type' => 'string'],
    'Population'     => ['rowName' => 'c.population',    'type' => 'integer'],
    'AreaKm2'        => ['rowName' => 'c.area_km2',      'type' => 'integer'],
    'Capital'        => ['rowName' => 'c.capital',       'type' => 'string'],
    'FoundedYear'    => ['rowName' => 'c.founded_year',  'type' => 'integer'],
    'FoundedDate'    => ['rowName' => 'c.founded_date',  'type' => 'dateTime'],
    'ContinentName'  => ['rowName' => 'cnt.name',        'type' => 'string'],
    '~meta~'         => [
        'sort'         => '>CountryName',
        'pageSize'     => 20,
        'searchTarget' => 'CountryName,Capital,ContinentName'
    ],
];

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

// ----------------------------------------------------------------------
// Form processing
// ----------------------------------------------------------------------
$mode         = ($_POST['mode'] ?? '') === 'write' ? 'write' : 'read';
$action       = $_POST['action'] ?? '';          // 'preview' or 'execute'
$filter       = [];
$meta         = [];
$writeData    = [];
$errorMsg     = '';
$sqlOutput    = '';
$resultOutput = '';
$result       = null;
$sql          = null;
$execError    = null;

// Construct options: default = true (active). Checkbox "off" means user wants to disable.
$opt_virtualDateFields = !isset($_POST['opt_virtualDateFields']);
$opt_allowSqlCondition = !isset($_POST['opt_allowSqlCondition']);
$opt_prepared          = !isset($_POST['opt_prepared']);

$waveOptions = [
    'virtualDateFields' => $opt_virtualDateFields,
    'allowSqlCondition' => $opt_allowSqlCondition,
    'prepared'          => $opt_prepared,
];

// ----------------------------------------------------------------------
// Build list of filter fields for UI (including virtual ones for display only)
// ----------------------------------------------------------------------
$uiFilterFields = array_keys(array_diff_key($baseKeyManifest, ['~meta~' => 0]));
$dateFields = [];
foreach ($baseKeyManifest as $key => $def) {
    if (isset($def['type']) && in_array($def['type'], ['date', 'dateTime'])) {
        $dateFields[] = $key;
    }
}
$virtualSuffixes = ['YEAR', 'MONTH', 'DAY', 'QUARTER', 'DATE', 'TIME', 'HOUR', 'MINUTE', 'UTS'];
$allUiFilterFields = $uiFilterFields;
foreach ($dateFields as $df) {
    foreach ($virtualSuffixes as $suf) {
        $allUiFilterFields[] = $df . $suf;
    }
}

$originalFields = $uiFilterFields;

// ----------------------------------------------------------------------
// Server-side access restrictions
// ----------------------------------------------------------------------
if ($action !== '') {
    if (($mode === 'read' && !$allowRead) || ($mode === 'write' && !$allowWrite)) {
        $action = '';
    }
}
if ($mode === 'write' && !$allowWrite) {
    $mode = $allowRead ? 'read' : '';
} elseif ($mode === 'read' && !$allowRead) {
    $mode = $allowWrite ? 'write' : '';
}

// ----------------------------------------------------------------------
// Read mode: gather filter values, meta, fulltext
// ----------------------------------------------------------------------
try {
    if ($mode === 'read') {
        // Filterwerte sammeln – für normale Felder direkt, für Datumsfelder kombiniert
        $filter = [];

        // 1. Normale Felder (keine Datumsfelder)
        foreach ($originalFields as $field) {
            if (in_array($field, $dateFields)) continue;
            if (isset($_POST[$field]) && $_POST[$field] !== '') {
                $filter[$field] = trim($_POST[$field]);
            }
        }

        // 2. Datumsfelder – aus Basiswert + Funktion den waveQl‑Schlüssel bauen
        foreach ($dateFields as $baseField) {
            $value = trim($_POST[$baseField] ?? '');
            if ($value === '') continue;
            $function = trim($_POST[$baseField . '_function'] ?? 'Original');
            // Serverseitige Absicherung: Wenn virtuelle Felder deaktiviert sind, ignoriere Funktion
            if (!$opt_virtualDateFields) {
                $function = 'Original';
            }
            if ($function === 'Original') {
                $filter[$baseField] = $value;
            } else {
                $waveQlKey = $baseField . $function;
                $filter[$waveQlKey] = $value;
            }
        }

        if (isset($_POST['sort']) && $_POST['sort'] !== '') $meta['sort'] = trim($_POST['sort']);
        if (isset($_POST['pageSize']) && is_numeric($_POST['pageSize'])) $meta['pageSize'] = (int)$_POST['pageSize'];
        if (isset($_POST['pageNumber']) && is_numeric($_POST['pageNumber'])) $meta['pageNumber'] = (int)$_POST['pageNumber'];

        $fulltextFields = $_POST['fulltext_fields'] ?? [];
        if (!is_array($fulltextFields)) $fulltextFields = [];
        $fulltextSearchString = trim($_POST['fulltext_search_string'] ?? '');

        if (!empty($fulltextFields)) {
            $meta['searchTarget'] = implode(',', $fulltextFields);
        }

        if (!empty($fulltextSearchString)) {
            $meta['searchString'] = $fulltextSearchString;
        }

        if ($opt_allowSqlCondition && isset($_POST['sqlCondition']) && trim($_POST['sqlCondition']) !== '') {
            $meta['sqlCondition'] = trim($_POST['sqlCondition']);
        }
    } else {
        $writeFields = ['CountryName', 'Population', 'AreaKm2', 'Capital', 'FoundedYear'];
        foreach ($writeFields as $field) {
            if (isset($_POST[$field]) && $_POST[$field] !== '') {
                $writeData[$field] = trim($_POST[$field]);
            }
        }
        if (isset($_POST['continent_id']) && is_numeric($_POST['continent_id'])) {
            $writeData['continent_id'] = (int)$_POST['continent_id'];
        }
        $meta['uniqueKey'] = 'id';
        $meta['returning'] = true;
    }
} catch (Exception $e) {
    $errorMsg = $e->getMessage();
}

// ----------------------------------------------------------------------
// Create waveQl instance to get live manifests (without executing a query)
// ----------------------------------------------------------------------
$liveKeyManifest = $baseKeyManifest;
$liveMetaManifest = $meta;
$liveTableManifest = $tableManifest;

try {
    $waveForManifest = \e2\waveQl::create($mysqli, $tableManifest, $baseKeyManifest, $waveOptions);
    $readInstance = $waveForManifest->read();
    if (!empty($filter)) $readInstance->setValues($filter);
    if (!empty($meta)) $readInstance->setMeta($meta);
    $liveKeyManifest = $readInstance->getManifest('key', 'live');
    $liveMetaManifest = $readInstance->getManifest('meta', 'live');
} catch (Exception $e) {
    // Fallback
}

// ----------------------------------------------------------------------
// Execution (or preview)
// ----------------------------------------------------------------------
$blinkSql    = false;
$blinkResult = false;

if ($action !== '') {
    try {
        $wave = \e2\waveQl::create($mysqli, $tableManifest, $baseKeyManifest, $waveOptions);

        if ($mode === 'read') {
            $builder = $wave->read();
            if (!empty($meta)) $builder->setMeta($meta);
            if (!empty($filter)) $builder->setValues($filter);

            if ($action === 'preview') {
                $sql = $builder->getQuery();
                $blinkSql = true;
            } else {
                $result = $builder->execute();
                $sql = $builder->getQuery();
                $blinkSql = true;
                $blinkResult = true;
            }
        } else {
            $builder = $wave->write();
            if (!empty($meta)) $builder->setMeta($meta);
            if (!empty($writeData)) $builder->setValues($writeData);
            if ($action === 'preview') {
                $sql = $builder->getQuery();
                $blinkSql = true;
            } else {
                $result = $builder->execute();
                $sql = $builder->getQuery();
                $blinkSql = true;
                $blinkResult = true;
            }
        }
    } catch (Exception $e) {
        $execError = $e->getMessage();
    }
}

if ($sql) {
    $sqlOutput = htmlspecialchars($sql);
}
if ($execError) {
    $errorMsg = $execError;
}

// Result output for read
if ($result !== null && $mode === 'read' && $action === 'execute') {
    ob_start();
    if (count($result) > 0) {
        echo '<table class="result-table">';
        echo '<thead><tr>';
        foreach (array_keys($result[0]) as $col) {
            echo '<th>' . htmlspecialchars($col) . '</th>';
        }
        echo '</thead><tbody>';
        foreach ($result as $row) {
            echo '<tr>';
            foreach ($row as $value) {
                echo '<td>' . htmlspecialchars($value ?? 'NULL') . '</td>';
            }
            echo '</tr>';
        }
        echo '</tbody></table>';
    } else {
        echo '<p>No records found.</p>';
    }
    $resultOutput = ob_get_clean();
} elseif ($result !== null && $mode === 'write' && $action === 'execute') {
    ob_start();
    if (is_array($result)) {
        echo '<pre>' . print_r($result, true) . '</pre>';
    } else {
        echo '<p>Affected/inserted ID: ' . htmlspecialchars($result) . '</p>';
    }
    $resultOutput = ob_get_clean();
}

// Provide JavaScript globals for preset handling
?>
<script>
    var dateFieldNames = <?= json_encode($dateFields) ?>;
    var virtualSuffixes = <?= json_encode($virtualSuffixes) ?>;
</script>
<?php