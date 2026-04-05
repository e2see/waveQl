<?php

declare(strict_types=1);

namespace e2;

/*
 *                     ██╗    ██╗ █████╗ ██╗   ██╗███████╗ ██████╗ ██╗
 *                     ██║    ██║██╔══██╗██║   ██║██╔════╝██╔═══██╗██║
 *                     ██║ █╗ ██║███████║██║   ██║█████╗  ██║   ██║██║
 *                     ██║███╗██║██╔══██║╚██╗ ██╔╝██╔══╝  ██║▄▄ ██║██║
 *                     ╚███╔███╔╝██║  ██║ ╚████╔╝ ███████╗╚██████╔╝███████╗
 *                      ╚══╝╚══╝ ╚═╝  ╚═╝  ╚═══╝  ╚══════╝ ╚══▀▀═╝ ╚══════╝
 *
 *                                      W A V E  Q L
 *                                      ~~~~~~~~~~~~
 *                                        by e2see
 *
 *
 * =================================================================================================
 * waveQl – Entry point / Factory for the SQL Query Builder
 * =================================================================================================
 *
 * This is the central entry point. The static create() method creates an instance
 * that then provides Read and Write objects.
 *
 * -------------------------------------------------------------------------------------------------
 * Usage:
 *   $wave = \e2\waveQl::create($mysqli, $tableManifest, $keyManifest, ['prepared' => true]);
 *
 *   // Read
 *   $rows = $wave->read()
 *                ->setMeta(['sort' => '>name', 'pageSize' => 20])
 *                ->setValues(['age' => '>18'])
 *                ->execute();
 *
 *   // Write (INSERT)
 *   $newId = $wave->write()
 *                 ->setMeta(['uniqueKey' => 'id'])
 *                 ->setValues(['name' => 'Max', 'age' => 30])
 *                 ->execute();
 *
 *   // Write (UPDATE)
 *   $affected = $wave->write()
 *                    ->setMeta(['uniqueKey' => 'id'])
 *                    ->setValues(['id' => 42, 'name' => 'Max (new)'])
 *                    ->execute();
 *
 *   // Write (DELETE)
 *   $deleted = $wave->write()
 *                   ->setMeta(['uniqueKey' => 'id', 'safe' => true])
 *                   ->setValues(['id' => 42])
 *                   ->delete();
 * -------------------------------------------------------------------------------------------------
 *
 * =================================================================================================
 */

$waveQlAllowlist = [
    'dbAdapterMysqli',
    'waveQlCore',
    'waveQlDbInterface',
    'waveQlRead',
    'waveQlWrite',
];

// Exception classes, all defined in waveQlException.php
$exceptionClasses = [
    'waveQlException',
    'waveQlInvalidArgumentException',
    'waveQlQueryException',
    'waveQlMetaException',
    'waveQlSecurityException',
];

spl_autoload_register(function ($className) use ($waveQlAllowlist, $exceptionClasses) {
    if (strpos($className, __NAMESPACE__ . '\\') !== 0) {
        return;
    }
    $shortName = substr($className, strlen(__NAMESPACE__) + 1);

    // For exception classes load the common file
    if (in_array($shortName, $exceptionClasses, true)) {
        $file = __DIR__ . '/waveQlException.php';
        if (file_exists($file)) {
            require_once $file;
        }
        return;
    }

    if (!in_array($shortName, $waveQlAllowlist, true)) {
        return;
    }
    if (class_exists($className, false) || interface_exists($className, false)) {
        return;
    }
    $file = __DIR__ . '/' . $shortName . '.php';
    if (file_exists($file)) {
        require_once $file;
    } else {
        throw new \Exception("waveQl: Required file not found – {$file}");
    }
});

class waveQl
{
    private waveQlDbInterface $db;
    private array $tableManifest;
    private array $keyManifest;
    private array $options;

    ########################### CONSTRUCTOR & FACTORY

    ##### Creates a new waveQl instance
    private function __construct(waveQlDbInterface $db, array $tableManifest, array $keyManifest, array $options = [])
    {
        $this->db            = $db;
        $this->tableManifest = $tableManifest;
        $this->keyManifest   = $keyManifest;
        $this->options       = $options;
    }

    ##### Static factory – creates the appropriate adapter and returns an instance
    public static function create($db, array $tableManifest, array $keyManifest, array $options = []): self
    {
        //-- mysqli instance is automatically wrapped into the adapter
        if ($db instanceof \mysqli) {
            $adapter = new dbAdapterMysqli($db);
        } elseif ($db instanceof waveQlDbInterface) {
            $adapter = $db;
        } else {
            throw new waveQlInvalidArgumentException('Only mysqli is currently supported.');
        }
        return new self($adapter, $tableManifest, $keyManifest, $options);
    }

    ########################### PUBLIC METHODS

    ##### Returns a Read object
    public function read(): waveQlRead
    {
        return new waveQlRead($this->db, $this->tableManifest, $this->keyManifest, $this->options);
    }

    ##### Returns a Write object
    public function write(): waveQlWrite
    {
        return new waveQlWrite($this->db, $this->tableManifest, $this->keyManifest, $this->options);
    }
}