<?php

/**
 * Header for setup mode: Provides masked config values and message.
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
    $setupMessage = '❌ Database connection failed. Please check your credentials.';
} elseif (!$tableExists) {
    $setupMessage = '⚠️ The table <code>countries</code> does not exist yet. Please initialise the database.';
}
