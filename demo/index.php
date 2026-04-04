<?php
error_reporting(E_ALL);
ini_set('display_errors', 1);

require_once __DIR__ . '/config.php';

// -------------------------------------------------------------
// Check connection and table existence
// -------------------------------------------------------------
$connectionOk = ($mysqli !== null);
$tableExists = false;

if ($connectionOk) {
    $res = $mysqli->query("SHOW TABLES LIKE 'countries'");
    $tableExists = ($res && $res->num_rows > 0);
}

// Auto-init only if connection works, explicitly called, AND allowed
if ($connectionOk && isset($_GET['initSQL']) && $_GET['initSQL'] == '1') {
    if (!$allowInitSQL) {
        $initMessage = 'Database initialisation is disabled in the configuration.';
    } else {
        $sqlFile = __DIR__ . '/setup.sql';
        if (file_exists($sqlFile)) {
            $sql = file_get_contents($sqlFile);
            if ($sql !== false) {
                $queries = array_filter(array_map('trim', explode(';', $sql)));
                $error = false;
                foreach ($queries as $query) {
                    if (empty($query)) continue;
                    if (!$mysqli->query($query)) {
                        $error = true;
                        break;
                    }
                }
                if (!$error) {
                    header('Location: ' . strtok($_SERVER['REQUEST_URI'], '?'));
                    exit;
                }
            }
        }
    }
}

// -------------------------------------------------------------
// Load header and template depending on status
// -------------------------------------------------------------
if (!$connectionOk || !$tableExists) {
    // Setup mode
    $initMessage = isset($_GET['initSQL']) ? 'Initialisation failed. Please run manually.' : '';
    include __DIR__ . '/sub/head.setup.php';
    include __DIR__ . '/sub/template.setup.php';
} else {
    // Demo mode
    include __DIR__ . '/sub/head.demo.php';
    include __DIR__ . '/sub/template.demo.php';
}