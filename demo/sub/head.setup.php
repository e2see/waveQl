<?php

/**
 * Kopf für Setup-Modus: Stellt maskierte Config-Werte und Meldung bereit.
 */

function maskValue($value)
{
    $len = strlen($value);
    if ($len <= 2) return str_repeat('*', $len);
    return substr($value, 0, 2) . str_repeat('*', $len - 2);
}

$maskedHost = maskValue($host);
$maskedUser = maskValue($user);
$maskedDb   = maskValue($dbname);

$setupMessage = '';
if (!$connectionOk) {
    $setupMessage = '❌ Datenbankverbindung fehlgeschlagen. Bitte überprüfen Sie die Zugangsdaten.';
} elseif (!$tableExists) {
    $setupMessage = '⚠️ Die Tabelle <code>countries</code> existiert noch nicht. Bitte initialisieren Sie die Datenbank.';
}
