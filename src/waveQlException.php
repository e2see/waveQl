<?php
declare(strict_types=1);

namespace e2;

/**
 * =================================================================================================
 * waveQl – Exception-Klassen für die waveQl-Komponente
 * =================================================================================================
 *
 * Diese Datei definiert die Exception-Hierarchie für alle waveQl-Fehler.
 * Die Basisklasse ist waveQlException, von der alle spezifischen Exception-Typen erben.
 * Dadurch können Fehler je nach Schweregrad abgefangen und behandelt werden.
 *
 * -------------------------------------------------------------------------------------------------
 * Verwendung:
 *   try {
 *       $wave->read()->execute();
 *   } catch (waveQlQueryException $e) {
 *       // Fehler bei der SQL-Ausführung
 *       echo "Query fehlgeschlagen: " . $e->getMessage();
 *   } catch (waveQlInvalidArgumentException $e) {
 *       // Ungültige Parameter
 *   } catch (waveQlMetaException $e) {
 *       // Fehlende oder falsche Meta-Informationen (z. B. uniqueKey)
 *   } catch (waveQlSecurityException $e) {
 *       // Unsichere SQL-Bedingung erkannt
 *   } catch (waveQlException $e) {
 *       // Allgemeiner Fehler
 *   }
 * -------------------------------------------------------------------------------------------------
 *
 * =================================================================================================
 */

##### Basis-Exception für die waveQl-Komponente.
class waveQlException extends \Exception {}

##### Wird bei ungültigen Argumenten geworfen (z. B. fehlerhafte Felddefinitionen).
class waveQlInvalidArgumentException extends waveQlException {}

##### Wird bei Fehlern während der SQL-Ausführung geworfen.
class waveQlQueryException extends waveQlException {}

##### Wird bei fehlenden oder falschen Meta-Informationen geworfen (z. B. uniqueKey fehlt).
class waveQlMetaException extends waveQlException {}

##### Wird bei Sicherheitsverletzungen geworfen (z. B. unsichere sqlCondition).
class waveQlSecurityException extends waveQlException {}