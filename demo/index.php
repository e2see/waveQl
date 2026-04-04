<?php
error_reporting(E_ALL);
ini_set('display_errors', 1);

require_once __DIR__ . '/config.php';

// -------------------------------------------------------------
// Weiche: Prüft Verbindung und Tabelle
// -------------------------------------------------------------
$connectionOk = ($mysqli !== null);
$tableExists = false;

if ($connectionOk) {
    $res = $mysqli->query("SHOW TABLES LIKE 'countries'");
    $tableExists = ($res && $res->num_rows > 0);
}

// Auto-Init nur bei Verbindung und explizitem Aufruf
if ($connectionOk && isset($_GET['initSQL']) && $_GET['initSQL'] == '1') {
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

// -------------------------------------------------------------
// Lade Kopf und Template je nach Zustand
// -------------------------------------------------------------
if (!$connectionOk || !$tableExists) {
    // Setup-Modus
    $initMessage = isset($_GET['initSQL']) ? 'Initialisierung fehlgeschlagen. Bitte manuell ausführen.' : '';
    include __DIR__ . '/sub/head.setup.php';
    include __DIR__ . '/sub/template.setup.php';
} else {
    // Demo-Modus
    include __DIR__ . '/sub/head.demo.php';
    include __DIR__ . '/sub/template.demo.php';
}
