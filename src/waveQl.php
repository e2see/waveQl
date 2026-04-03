<?php
declare(strict_types=1);

namespace e2;

/**
 * =================================================================================================
 * waveQl – Einstiegspunkt / Factory für den SQL‑Query‑Builder
 * =================================================================================================
 *
 * Dies ist der zentrale Einstiegspunkt. Über die statische Methode create() wird eine Instanz
 * erzeugt, die dann Read‑ und Write‑Objekte bereitstellt.
 *
 * -------------------------------------------------------------------------------------------------
 * Verwendung:
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
 *                    ->setValues(['id' => 42, 'name' => 'Max (neu)'])
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

// Exception-Klassen, die alle in waveQlException.php definiert sind
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

    // Für Exception-Klassen die gemeinsame Datei laden
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

    ########################### KONSTRUKTOR & FACTORY

    ##### Erzeugt eine neue waveQl‑Instanz
    private function __construct(waveQlDbInterface $db, array $tableManifest, array $keyManifest, array $options = [])
    {
        $this->db            = $db;
        $this->tableManifest = $tableManifest;
        $this->keyManifest   = $keyManifest;
        $this->options       = $options;
    }

    ##### Statische Factory – erzeugt den passenden Adapter und gibt eine Instanz zurück
    public static function create($db, array $tableManifest, array $keyManifest, array $options = []): self
    {
        //-- mysqli-Instanz wird automatisch in den Adapter gewrappt
        if ($db instanceof \mysqli) {
            $adapter = new dbAdapterMysqli($db);
        } elseif ($db instanceof waveQlDbInterface) {
            $adapter = $db;
        } else {
            throw new waveQlInvalidArgumentException('Nur mysqli wird derzeit unterstützt.');
        }
        return new self($adapter, $tableManifest, $keyManifest, $options);
    }

    ########################### ÖFFENTLICHE METHODEN

    ##### Gibt ein Read‑Objekt zurück
    public function read(): waveQlRead
    {
        return new waveQlRead($this->db, $this->tableManifest, $this->keyManifest, $this->options);
    }

    ##### Gibt ein Write‑Objekt zurück
    public function write(): waveQlWrite
    {
        return new waveQlWrite($this->db, $this->tableManifest, $this->keyManifest, $this->options);
    }
}