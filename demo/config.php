<?php
/*
 *  ██╗    ██╗ █████╗ ██╗   ██╗███████╗ ██████╗ ██╗
 *  ██║    ██║██╔══██╗██║   ██║██╔════╝██╔═══██╗██║
 *  ██║ █╗ ██║███████║██║   ██║█████╗  ██║   ██║██║
 *  ██║███╗██║██╔══██║╚██╗ ██╔╝██╔══╝  ██║▄▄ ██║██║
 *  ╚███╔███╔╝██║  ██║ ╚████╔╝ ███████╗╚██████╔╝███████╗
 *   ╚══╝╚══╝ ╚═╝  ╚═╝  ╚═══╝  ╚══════╝ ╚══▀▀═╝ ╚══════╝
 *
 *                    W A V E  Q L
 *                    ~~~~~~~~~~~~
 *                      by e2see
 *
 *
 */

$host   = 'localhost';
$user   = 'root';
$pass   = '';
$dbname = 'waveql_test';

// ----- Security settings for demo mode -----
$allowRead    = true;   // Read mode (SELECT) allowed
$allowWrite   = false;  // Write mode (INSERT/UPDATE/DELETE) allowed
$allowInitSQL = true;  // Automatic table initialisation via ?initSQL=1 allowed

$mysqli            = null;
$dbConnectionError = '';

try {
    @$mysqli = new mysqli($host, $user, $pass);
    if ($mysqli->connect_error) {
        throw new Exception($mysqli->connect_error);
    }
    if (!$mysqli->query("CREATE DATABASE IF NOT EXISTS $dbname")) {
        throw new Exception($mysqli->error);
    }
    $mysqli->select_db($dbname);
    $mysqli->set_charset('utf8');
} catch (Exception $e) {
    $dbConnectionError = $e->getMessage();
    $mysqli = null;
}