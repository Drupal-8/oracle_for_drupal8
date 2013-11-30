<?php

/**
 * @file
 * Definition of Drupal\Core\Database\Driver\oracle\StatementBase
 */

namespace Drupal\Core\Database\Driver\oracle;

use Drupal\Core\Database\StatementPrefetch;
use Drupal\Core\Database\StatementInterface;

/**
 * Specific Oracle implementation of DatabaseConnection.
 */
class Statement extends StatementPrefetch implements \Iterator, StatementInterface {
  private $bindArgs = array();

  /**
   * Oracle specific implementation of getStatement().
   */
  protected function getStatement($query, &$args = array()) {
		if (count($args)) {
			// Check if $args is a simple numeric array.
			if (range(0, count($args) - 1) === array_keys($args)) {
				// In that case, we have unnamed placeholders.
				$count = 0;
				$new_args = array();
				foreach ($args as $value) {
					if (is_float($value) || is_int($value)) {
						if (is_float($value)) {
							// Force the conversion to float so as not to loose precision
							// in the automatic cast.
							$value = sprintf('%F', $value);
						}
						$query = substr_replace($query, $value, strpos($query, '?'), 1);
					}
					else {
						$placeholder = ':db_statement_placeholder_' . $count++;
						$query = substr_replace($query, $placeholder, strpos($query, '?'), 1);
						$new_args[$placeholder] = $value;
					}
				}
				$args = $new_args;
			}
			else {
				// Else, this is using named placeholders.
				foreach ($args as $placeholder => $value) {
					if (is_float($value) || is_int($value)) {
						if (is_float($value)) {
							// Force the conversion to float so as not to loose precision
							// in the automatic cast.
							$value = sprintf('%F', $value);
						}

						// We will remove this placeholder from the query as PDO throws an
						// exception if the number of placeholders in the query and the
						// arguments does not match.
						unset($args[$placeholder]);
						// PDO allows placeholders to not be prefixed by a colon. See
						// http://marc.info/?l=php-internals&m=111234321827149&w=2 for
						// more.
						if ($placeholder[0] != ':') {
							$placeholder = ":$placeholder";
						}
						// When replacing the placeholders, make sure we search for the
						// exact placeholder. For example, if searching for
						// ':db_placeholder_1', do not replace ':db_placeholder_11'.
						$query = preg_replace('/' . preg_quote($placeholder) . '\b/', $value, $query);
					}
				}
			}
		}

    return $this->dbh->prepare($query);
  }

  public function bindValue($parameter, $value, $data_type = PDO::PARAM_STR) {
    $value = $this->dbh->cleanupArgValue($value);
    parent::bindValue($parameter, $value, $data_type);
  }

  public function bindParam($parameter, &$variable, $data_type = PDO::PARAM_STR, $length = 0, $driver_options = array()) {
    $variable = $this->dbh->cleanupArgValue($variable);
    $this->bindArgs[$parameter] = &$variable;
    parent::bindParam($parameter, $variable, $data_type, $length, $driver_options);
  }

  public function getArgs() {
    return $this->bindArgs;
  }

  public function execute($args = array(), $options = array()) {
    if (isset($options['fetch'])) {
      if (is_string($options['fetch'])) {
        // Default to an object. Note: db fields will be added to the object
        // before the constructor is run. If you need to assign fields after
        // the constructor is run, see http://drupal.org/node/315092.
        $this->setFetchMode(\PDO::FETCH_CLASS, $options['fetch']);
      }
      else {
        $this->setFetchMode($options['fetch']);
      }
    }
    $this->dbh->lastStatement = $this;

    $logger = $this->dbh->getLogger();
    if (!empty($logger)) {
      $query_start = microtime(TRUE);
    }

    // Prepare the query.
    $statement = $this->getStatement($this->queryString, $args);
    if (!$statement) {
      throw new \PDOException('Message', 'error query string');
    }

    $return = $statement->execute($args, $options, TRUE);
    if (empty($this->queryString)) {
			if (!$return) {
			  throw new \PDOException('Message', 'error query string');
	    }
    }
    if (!$return) {
      throw new \PDOException('Message', 'error query string');
    }

    /*if (is_array($args) && count($args)) {
      $return = parent::execute($args);
    }
    else {
      $return = parent::execute();
    }*/

    // Fetch all the data from the reply, in order to release any lock
    // as soon as possible.
    $this->rowCount = $statement->rowCount();
    
    $return = NULL;
    
    try {
      $return = $this->data = $statement->fetchAll(\PDO::FETCH_ASSOC);
    }
    catch (\Exception $e) {
      // Ignore non-fetchable statements errors.
      if (!(isset($e->errorInfo) && is_array($e->errorInfo) && $e->errorInfo[1] == '24374')) {
        throw $e;
      }
    }
    
    unset($statement);
    
    $this->resultRowCount = count($this->data);
    
    if ($this->resultRowCount) {
      $this->columnNames = array_keys($this->data[0]);
    }
    else {
      $this->columnNames = array();
    }

    if (!empty($logger)) {
      $query_end = microtime(TRUE);
      $logger->log($this, $args, $query_end - $query_start);
    }

    // Initialize the first row in $this->currentRow.
    $this->next();

    return $return;
  }

  /**
   * @return $f cleaned up from:
   *
   *  1) long identifiers place holders (may occur in queries like:
   *               select 1 as myverylongidentifier from mytable
   *     this is transalted on query submission as e.g.:
   *               select 1 as L#321 from mytable
   *     so when we fetch this object (or array) we will have
   *     stdClass ( "L#321" => 1 ) or Array ( "L#321" => 1 ).
   *     but the code is especting to access the field as myobj->myverylongidentifier,
   *     so we need to translate the "L#321" back to "myverylongidentifier").
   *
   *  2) blob placeholders:
   *     we can find values like B^#2354, and we have to translate those values
   *     back to their original long value so we read blob id 2354 of table blobs
   *
   *  3) removes the rwn column from queryRange queries
   *
   *  4) translate empty string replacement back to empty string
   *
   */
  private function cleanupFetched($f) {
    if ($this->dbh->external) {
      return $f;
    }

    if (is_array($f)) {
      foreach ($f as $key => $value) {
        if ((string) $key == strtolower(ORACLE_ROWNUM_ALIAS)) {
          unset($f[$key]);
        }
        // Long identifier.
        elseif (Connection::isLongIdentifier($key)) {
          $f[$this->dbh->lih->longIdentifierKey($key)] = $this->cleanupFetched($value);
          unset($f[$key]);
        }
        else {
          $f[$key] = $this->cleanupFetched($value);
        }
      }
    }
    elseif (is_object($f)) {
      foreach ($f as $key => $value) {
        if ((string) $key == strtolower(ORACLE_ROWNUM_ALIAS)) {
          unset($f->{$key});
        }
        // Long identifier.
        elseif (Connection::isLongIdentifier($key)) {
          $f->{$this->dbh->lih->longIdentifierKey($key)} = $this->cleanupFetched($value);
          unset($f->{$key});
        }
        else {
          $f->{$key} = $this->cleanupFetched($value);
        }
      }
    }
    else {
      $f = $this->dbh->cleanupFetchedValue($f);
    }

    return $f;
  }


  //// @todo Check this.
  //public function next() {
  //  if (!empty($this->data)) {
  //    $this->currentRow = reset($this->data);
  //    $this->currentKey = key($this->data);
  //    unset($this->data[$this->currentKey]);
  //  }
  //  else {
  //    $this->currentRow = NULL;
  //  }
  //}

  public function fetch($fetch_style = NULL, $cursor_orientation = PDO::FETCH_ORI_NEXT, $cursor_offset = 0) {
    return $this->cleanupFetched(parent::fetch($fetch_style, $cursor_orientation, $cursor_offset));
  }

  public function fetchField($index = 0) {
    return $this->cleanupFetched(parent::fetchField($index));
  }

  public function fetchObject($class_name = "stdClass", $constructor_args = NULL) {
    //if (isset($this->currentRow)) {
    //  if (!isset($class_name)) {
    //    // Directly cast to an object to avoid a function call.
    //    $result = (object) $this->currentRow;
    //  }
    //  else {
    //    $this->fetchStyle = PDO::FETCH_CLASS;
    //    $this->fetchOptions = array('constructor_args' => $constructor_args);
    //    // Grab the row in the format specified above.
    //    $result = $this->current();
    //    // Reset the fetch parameters to the value stored using setFetchMode().
    //    $this->fetchStyle = $this->defaultFetchStyle;
    //    $this->fetchOptions = $this->defaultFetchOptions;
    //  }
    //
    //  $this->next();
    //
    //  return $this->cleanupFetched($result);
    //}
    //else {
    //  return FALSE;
    //}

    return $this->cleanupFetched(parent::fetchObject($class_name, $constructor_args));
  }

  public function fetchAssoc() {
    return $this->cleanupFetched(parent::fetchAssoc());
  }

  public function fetchAll($fetch_style = NULL, $fetch_column = NULL, $constructor_args = NULL) {
      $result = parent::fetchAll();
//      $result = parent::fetchAll($fetch_style, $fetch_column, $constructor_args);

    return $this->cleanupFetched($result);
  }

  public function fetchCol($index = 0) {
    return $this->cleanupFetched(parent::fetchCol($index));
  }

  public function fetchAllKeyed($key_index = 0, $value_index = 1) {
    return $this->cleanupFetched(parent::fetchAllKeyed($key_index, $value_index));
  }

  public function fetchAllAssoc($key, $fetch_style = NULL) {
    return $this->cleanupFetched(parent::fetchAllAssoc($key, $fetch_style));
  }

}
