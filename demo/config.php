<?php
$host = 'localhost';
$user = 'root';
$pass = '';
$dbname = 'waveql_test';

// Verbindung ohne Datenbank herstellen
$mysqli = new mysqli($host, $user, $pass);
if ($mysqli->connect_error) {
    die("Connection failed: " . $mysqli->connect_error);
}
// Datenbank erstellen, falls nicht vorhanden
if (!$mysqli->query("CREATE DATABASE IF NOT EXISTS $dbname")) {
    die("Could not create database: " . $mysqli->error);
}
$mysqli->select_db($dbname);
$mysqli->set_charset('utf8');