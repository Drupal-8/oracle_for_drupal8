<?php

/**
 * @file
 * Definition of Drupal\Core\Database\Driver\oracle\Update
 */

namespace Drupal\Core\Database\Driver\oracle;

use Drupal\Core\Database\Database;
use Drupal\Core\Database\Query\Update as QueryUpdate;

use PDO;

class Update extends QueryUpdate {
  private $updateArgs = array();
  private $conditionArgs = array();
  private $expArgs = array();
  private $rowCount = 0;

  public function execute() {
    //$max_placeholder = 0;
    //$blobs = array();
    //$blob_count = 0;
    //
    //// Needed for BLOBS.
    ////$transaction = $this->connection->startTransaction();
    //
    //// Because we filter $fields the same way here and in __toString(), the
    //// placeholders will all match up properly.
    //$stmt = $this->connection->prepareQuery((string) $this)->getStatement(NULL);
    //
    //// Fetch the list of blobs and sequences used on that table.
    //$info = $this->connection->schema()->getTableInfo($this->table);
    //
    //// Expressions take priority over literal fields, so we process those first
    //// and remove any literal fields that conflict.
    //$fields = $this->fields;
    //$expression_fields = array();
    //foreach ($this->expressionFields as $field => $data) {
    //  if (!empty($data['arguments'])) {
    //    foreach ($data['arguments'] as $placeholder => $argument) {
    //      // We assume that an expression will never happen on a BLOB field,
    //      // which is a fairly safe assumption to make since in most cases
    //      // it would be an invalid query anyway.
    //      $this->expArgs[$placeholder] = $this->connection->cleanupArgValue($argument);
    //      $stmt->bindParam($placeholder, $this->expArgs[$placeholder]);
    //    }
    //  }
    //  unset($fields[$field]);
    //}
    //
    //foreach ($fields as $field => $value) {
    //  $placeholder = ':db_update_placeholder_' . ($max_placeholder++);
    //  $this->updateArgs[$placeholder] = $this->connection->cleanupArgValue($value);
    //  $stmt->bindParam($placeholder, $this->updateArgs[$placeholder]);
    //}
    //
    //if (count($this->condition)) {
    //  $this->condition->compile($this->connection, $this);
    //  $arguments = $this->condition->arguments();
    //  foreach ($arguments as $placeholder => $value) {
    //    $this->conditionArgs[$placeholder] = $this->connection->cleanupArgValue($value);
    //    $stmt->bindParam($placeholder, $this->conditionArgs[$placeholder]);
    //  }
    //}
    //
    //// @TODO: check this.
    //$stmt->bindParam(':db_sql_rowcount', $this->rowCount, PDO::PARAM_INT | PDO::PARAM_INPUT_OUTPUT, 32);
    //
    //$options = $this->queryOptions;
    //$options['already_prepared'] = TRUE;
    //$this->connection->query($stmt, array(), $options);
    //
    //return (int) $this->rowCount;

//---------------------------------------- NEW EXECUTE METHOD ----------------//
    $max_placeholder = 0;
    $blobs = array();
    $blob_count = 0;
    
    // Needed for BLOBS.
    //$transaction = $this->connection->startTransaction();
    
    //// Because we filter $fields the same way here and in __toString(), the
    //// placeholders will all match up properly.
    //$stmt = $this->connection->prepareQuery((string) $this)->getStatement(NULL);
    
    $stmt = $this->connection->prepareQuery((string) $this);
    
    // Fetch the list of blobs and sequences used on that table.
    $info = $this->connection->schema()->getTableInfo($this->table);
    
    // Expressions take priority over literal fields, so we process those first
    // and remove any literal fields that conflict.
    $fields = $this->fields;
    $update_values = array();
    foreach ($this->expressionFields as $field => $data) {
      if (!empty($data['arguments'])) {
        foreach ($data['arguments'] as $placeholder => $argument) {
          // We assume that an expression will never happen on a BLOB field,
          // which is a fairly safe assumption to make since in most cases
          // it would be an invalid query anyway.
          $this->expArgs[$placeholder] = $this->connection->cleanupArgValue($argument);
          $stmt->bindParam($placeholder, $this->expArgs[$placeholder]);
          // @todo this needs to be corrected and optimized.
          $update_values[$placeholder] = $this->expArgs[$placeholder];
        }
      }
      unset($fields[$field]);
    }
    
    foreach ($fields as $field => $value) {
      $placeholder = ':db_update_placeholder_' . ($max_placeholder++);
      $this->updateArgs[$placeholder] = $this->connection->cleanupArgValue($value);
      $stmt->bindParam($placeholder, $this->updateArgs[$placeholder]);
      // @todo this needs to be corrected and optimized.
      $update_values[$placeholder] = $this->updateArgs[$placeholder];
    }
    
    if (count($this->condition)) {
      $this->condition->compile($this->connection, $this);
      $arguments = $this->condition->arguments();
      foreach ($arguments as $placeholder => $value) {
        $this->conditionArgs[$placeholder] = $this->connection->cleanupArgValue($value);
        $stmt->bindParam($placeholder, $this->conditionArgs[$placeholder]);
        // @todo this needs to be corrected and optimized.
        $update_values[$placeholder] = $this->conditionArgs[$placeholder];
      }
    }
    
    // @TODO: check this.
    $stmt->bindParam(':db_sql_rowcount', $this->rowCount, PDO::PARAM_INT | PDO::PARAM_INPUT_OUTPUT, 32);
    $update_values[':db_sql_rowcount'] = $this->rowCount;
    
    //return $this->connection->query($stmt, $update_values, $this->queryOptions);
    $options = $this->queryOptions;
    $options['already_prepared'] = TRUE;
    $this->connection->query($stmt, array(), $options);

    return (int) $this->rowCount;
  }

  public function __toString() {
    // Create a comments string to prepend to the query.
    $comments = (!empty($this->comments)) ? '/* ' . implode('; ', $this->comments) . ' */ ' : '';

    // Expressions take priority over literal fields, so we process those first
    // and remove any literal fields that conflict.
    $fields = $this->fields;
    $update_fields = array();
    foreach ($this->expressionFields as $field => $data) {
      $update_fields[] = $field . '=' . $data['expression'];
      unset($fields[$field]);
    }

    $max_placeholder = 0;
    foreach ($fields as $field => $value) {
      $update_fields[] = $field . '=:db_update_placeholder_' . ($max_placeholder++);
    }

    $query = $comments . 'UPDATE {' . $this->connection->escapeTable($this->table) . '} SET ' . implode(', ', $update_fields);

    if (count($this->condition)) {
      $this->condition->compile($this->connection, $this);
      // There is an implicit string cast on $this->condition.
      $query .= "\nWHERE " . $this->condition;
    }

    // FIXME: remove the PL/SQL when PDO_OCI returns the correct number of
    // rows updated now it returns only 1 (updated) or 0 (not)
    return 'begin ' . $query . '; :db_sql_rowcount := sql%rowcount; end;';
  }
}
