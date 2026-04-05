<?php
declare(strict_types=1);

namespace e2;

/**
 * =================================================================================================
 * waveQl – Exception classes for the waveQl component
 * =================================================================================================
 *
 * This file defines the exception hierarchy for all waveQl errors.
 * The base class is waveQlException, from which all specific exception types inherit.
 * This allows errors to be caught and handled according to severity.
 *
 * -------------------------------------------------------------------------------------------------
 * Usage:
 *   try {
 *       $wave->read()->execute();
 *   } catch (waveQlQueryException $e) {
 *       // Error during SQL execution
 *       echo "Query failed: " . $e->getMessage();
 *   } catch (waveQlInvalidArgumentException $e) {
 *       // Invalid parameters
 *   } catch (waveQlMetaException $e) {
 *       // Missing or incorrect meta information (e.g. uniqueKey)
 *   } catch (waveQlSecurityException $e) {
 *       // Unsafe SQL condition detected
 *   } catch (waveQlException $e) {
 *       // General error
 *   }
 * -------------------------------------------------------------------------------------------------
 *
 * =================================================================================================
 */

##### Base exception for the waveQl component.
class waveQlException extends \Exception {}

##### Thrown for invalid arguments (e.g. malformed field definitions).
class waveQlInvalidArgumentException extends waveQlException {}

##### Thrown for errors during SQL execution.
class waveQlQueryException extends waveQlException {}

##### Thrown for missing or incorrect meta information (e.g. uniqueKey missing).
class waveQlMetaException extends waveQlException {}

##### Thrown for security violations (e.g. unsafe sqlCondition).
class waveQlSecurityException extends waveQlException {}