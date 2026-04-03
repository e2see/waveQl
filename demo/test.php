<?php
error_reporting(E_ALL);
ini_set('display_errors', 1);

require_once __DIR__ . '/config.php';

// ----------------------------------------------------------------------
// Pfade
// ----------------------------------------------------------------------
$waveQlDir = __DIR__ . '/../src/';
$testDir   = __DIR__ . '/';

// ----------------------------------------------------------------------
// Prüfen, ob die waveQl-Dateien vorhanden sind
// ----------------------------------------------------------------------
$requiredFiles = ['waveQl.php'];
$missing = [];
foreach ($requiredFiles as $file) {
    if (!file_exists($waveQlDir . $file)) {
        $missing[] = $file;
    }
}

$waveQlAvailable = empty($missing);
$errorMsg        = '';
$sqlOutput       = '';
$resultOutput    = '';
$initMessage     = '';

if (!$waveQlAvailable) {
    $errorMsg = "Fehlende waveQl-Dateien: " . implode(', ', $missing) . "<br>Bitte kopiere alle waveQl-Dateien in das Verzeichnis <code>$waveQlDir</code>.";
} else {
    foreach ($requiredFiles as $file) {
        require_once $waveQlDir . $file;
    }
}

// ----------------------------------------------------------------------
// Auto‑init database when ?initSQL=1 is present
// ----------------------------------------------------------------------
if (isset($_GET['initSQL']) && $_GET['initSQL'] == '1') {
    $initMessage = initDatabase($mysqli, $testDir);
    if (strpos($initMessage, 'successfully') !== false) {
        header('Location: ' . strtok($_SERVER['REQUEST_URI'], '?'));
        exit;
    }
}

// ----------------------------------------------------------------------
// Datalist‑Vorschläge für Autovervollständigung (Beispiele für waveQl‑Syntax)
// ----------------------------------------------------------------------
$sortExamples = ['>Population', '<CountryName', '>FoundedYear', '<AreaKm2', '>FoundedDateYEAR'];
$populationExamples = ['>1000000', '50000000><200000000', '!0', '10000000><50000000', '>100000000'];
$areaExamples = ['>1000000', '5000000><10000000', '<500000', '>2000000'];
$yearExamples = ['1800><1950', '>1900', '<0', '!NULL', '1500><=2000'];

// Echte Daten aus der DB für Ländernamen, Hauptstädte, Kontinente
$countryNames = $capitals = $continentNames = [];
if ($waveQlAvailable) {
    $res = $mysqli->query("SELECT DISTINCT name FROM countries ORDER BY name LIMIT 100");
    while ($row = $res->fetch_assoc()) $countryNames[] = $row['name'];
    $res = $mysqli->query("SELECT DISTINCT capital FROM countries WHERE capital IS NOT NULL AND capital != '' ORDER BY capital LIMIT 100");
    while ($row = $res->fetch_assoc()) $capitals[] = $row['capital'];
    $res = $mysqli->query("SELECT DISTINCT name FROM continents ORDER BY name");
    while ($row = $res->fetch_assoc()) $continentNames[] = $row['name'];
}

// ----------------------------------------------------------------------
// Field definitions (nur wenn waveQl verfügbar)
// ----------------------------------------------------------------------
if ($waveQlAvailable) {
    // Basis-Manifest (ohne Autofelder)
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

    // --- Automatische Zusatzfelder für dateTime-Felder generieren (wie im Core) ---
    $autoFieldMap = [
        'DATE'    => 'date',
        'YEAR'    => 'year',
        'QUARTER' => 'quarter',
        'MONTH'   => 'month',
        'DAY'     => 'day',
        'TIME'    => 'time',
        'HOUR'    => 'hour',
        'MINUTE'  => 'minute',
        'UNIX_TIMESTAMP' => 'uts',
    ];

    $keyManifest = $baseKeyManifest;
    $dateFields = []; // speichert Basis-Feldnamen, für die Autofelder existieren
    foreach ($baseKeyManifest as $key => $def) {
        if (isset($def['type']) && in_array($def['type'], ['date', 'dateTime'])) {
            $dateFields[] = $key;
            $baseRowName = $def['rowName'];
            foreach ($autoFieldMap as $sqlFunc => $subType) {
                $autoKey = $key . strtoupper($subType);
                if (!isset($keyManifest[$autoKey])) {
                    $keyManifest[$autoKey] = [
                        'rowName' => "$sqlFunc($baseRowName)",
                        'type'    => $subType,
                    ];
                }
            }
        }
    }

    // Alle Filterfelder (alle Schlüssel außer ~meta~)
    $filterFields = array_keys(array_diff_key($keyManifest, ['~meta~' => 0]));

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
}

// ----------------------------------------------------------------------
// Process form input
// ----------------------------------------------------------------------
$mode   = (!empty($_POST['mode'])) ? $_POST['mode'] : 'read';
$action = $_POST['action'] ?? '';
$filter = [];
$meta   = [];
$writeData = [];

if ($waveQlAvailable) {
    try {
        if ($mode === 'read') {
            // Dynamisch alle Filterfelder aus dem POST holen
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
}


// ----------------------------------------------------------------------
// Execute with waveQl
// ----------------------------------------------------------------------
$result    = null;
$sql       = null;
$execError = null;

if ($waveQlAvailable && $action !== '') {
    try {
        $wave = \e2\waveQl::create($mysqli, $tableManifest, $keyManifest, ['prepared' => false]);

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
        echo '<table>';
        echo '<thead><tr>';
        foreach (array_keys($result[0]) as $col) {
            echo '<th>' . htmlspecialchars($col) . '</th>';
        }
        echo '</thead><tbody>';
        foreach ($result as $row) {
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

// ----------------------------------------------------------------------
// Helper function to initialise database
// ----------------------------------------------------------------------
function initDatabase($mysqli, $testDir)
{
    $sqlFile = $testDir . 'setup.sql';
    if (!file_exists($sqlFile)) {
        return "setup.sql not found. Please place it in the same directory as test.php.";
    }
    $sql = file_get_contents($sqlFile);
    if ($sql === false) {
        return "Could not read setup.sql";
    }
    $queries = array_filter(array_map('trim', explode(';', $sql)));
    foreach ($queries as $query) {
        if (empty($query)) continue;
        if (!$mysqli->query($query)) {
            return "Error executing query: " . $mysqli->error . "<br>Query: " . htmlspecialchars($query);
        }
    }
    return "Database initialised successfully!";
}





// ----------------------------------------------------------------------
// Include template
// ----------------------------------------------------------------------
include __DIR__ . '/template.php';