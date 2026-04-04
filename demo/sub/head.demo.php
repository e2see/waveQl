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
// Field definitions
// ----------------------------------------------------------------------
$waveOptions = [
    'prepared'          => false,
    'virtualDateFields' => true,
    'allowSqlCondition' => false,
];

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

$filterFields = array_keys(array_diff_key($baseKeyManifest, ['~meta~' => 0]));
$dateFields = [];
$virtualSuffixes = ['YEAR', 'MONTH', 'DAY', 'HOUR', 'MINUTE', 'QUARTER', 'DATE', 'TIME', 'UTS'];
foreach ($baseKeyManifest as $key => $def) {
    if (isset($def['type']) && in_array($def['type'], ['date', 'dateTime'])) {
        $dateFields[] = $key;
        foreach ($virtualSuffixes as $suffix) {
            $virtualName = $key . $suffix;
            if (!in_array($virtualName, $filterFields)) {
                $filterFields[] = $virtualName;
            }
        }
    }
}

$keyManifest = $baseKeyManifest;

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
$action       = $_POST['action'] ?? '';
$filter       = [];
$meta         = [];
$writeData    = [];
$errorMsg     = '';
$sqlOutput    = '';
$resultOutput = '';
$result       = null;
$sql          = null;
$execError    = null;

// ----------------------------------------------------------------------
// Server-side access restrictions (no exception – just block action)
// ----------------------------------------------------------------------
if ($action !== '') {
    if (($mode === 'read' && !$allowRead) || ($mode === 'write' && !$allowWrite)) {
        $action = ''; // abort action, no extra error message (template will show warning)
    }
}
// If current mode is not allowed, switch to first allowed one (for UI display)
if ($mode === 'write' && !$allowWrite) {
    $mode = $allowRead ? 'read' : '';
} elseif ($mode === 'read' && !$allowRead) {
    $mode = $allowWrite ? 'write' : '';
}

try {
    if ($mode === 'read') {
        foreach ($filterFields as $field) {
            if (isset($_POST[$field]) && $_POST[$field] !== '') {
                $filter[$field] = trim($_POST[$field]);
            }
        }
        if (isset($_POST['sort']) && $_POST['sort'] !== '') $meta['sort'] = trim($_POST['sort']);
        if (isset($_POST['pageSize']) && is_numeric($_POST['pageSize'])) $meta['pageSize'] = (int)$_POST['pageSize'];
        if (isset($_POST['pageNumber']) && is_numeric($_POST['pageNumber'])) $meta['pageNumber'] = (int)$_POST['pageNumber'];
        if (isset($_POST['searchString']) && $_POST['searchString'] !== '') $meta['searchString'] = trim($_POST['searchString']);
        if (isset($_POST['searchTarget']) && $_POST['searchTarget'] !== '') $meta['searchTarget'] = trim($_POST['searchTarget']);
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
// Execution
// ----------------------------------------------------------------------
if ($action !== '') {
    try {
        $wave = \e2\waveQl::create($mysqli, $tableManifest, $keyManifest, $waveOptions);

        if ($mode === 'read') {
            $builder = $wave->read();
            if (!empty($meta)) $builder->setMeta($meta);
            if (!empty($filter)) $builder->setValues($filter);
            if ($action === 'query') {
                $sql = $builder->getQuery();
            } else {
                $result = $builder->execute();
                $sql = $builder->getQuery();
            }
        } else {
            $builder = $wave->write();
            if (!empty($meta)) $builder->setMeta($meta);
            if (!empty($writeData)) $builder->setValues($writeData);
            if ($action === 'query') {
                $sql = $builder->getQuery();
            } else {
                $result = $builder->execute();
                $sql = $builder->getQuery();
            }
        }
    } catch (Exception $e) {
        $execError = $e->getMessage();
    }
}

// ----------------------------------------------------------------------
// Output preparation
// ----------------------------------------------------------------------
if ($sql) {
    $sqlOutput = htmlspecialchars($sql);
}
if ($execError) {
    $errorMsg = $execError;
}
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