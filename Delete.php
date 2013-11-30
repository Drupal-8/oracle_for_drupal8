<?php

/**
 * @file
 * Definition of Drupal\Core\Database\Driver\oracle\Delete
 */

namespace Drupal\Core\Database\Driver\oracle;

use Drupal\Core\Database\Query\Delete as QueryDelete;
use Drupal\Core\Database\Driver\oracle\StatementBase as StatementBase;

use \PDO as PDO;
use \PDOStatement as PDOStatement;
use Drupal\Core\Database\Driver\oracle\Connection as DeimosConnection;

class Delete extends QueryDelete {
  /**
   * Executes the DELETE query.
   *
   * @return
   *   The return value is dependent on the database connection.
   */
  public function execute() {
    // @todo needs to fix issue with creating transaction SAVEPOINT.
    //$this->connection->startTransaction();

    // Build Statement class.
    //$query_old = $this->connection->prepareQuery((string) $this);

    $values = array();
    if (count($this->condition)) {
      $this->condition->compile($this->connection, $this);
      $values = $this->condition->arguments();
      //foreach ($values as $idx => $field) {
        //$values[$idx] = $this->connection->cleanupArgValue($field);
        //$query->bindParam($idx, $values[$idx]);
      //}
    }

    // @todo Find out why $this->connection doesn't work and isn't the same as we do below.
    $connection_options = array(
      'database' => 'XE',
      'username' => 'drupal',
      'password' => 'oracle',
      'host' => 'localhost',
      'port' => 1521,
      'use_cache' => 0,
      'namespace' => 'Drupal\Core\Database\Driver\oracle',
      'driver' => 'oracle',
      'prefix' => array('default' => ''),
      'pdo' => array(
        17 => 1,
        8 => 2,
        3 => 2,
      ),
    );
    $dsn = 'oci:dbname=//' . $connection_options['host'] . ':' . $connection_options['port'] . '/' . $connection_options['database'] . ';charset=AL32UTF8';
    $pdo = new PDO($dsn, $connection_options['username'], $connection_options['password'], $connection_options['pdo']);
    $connection = new DeimosConnection($pdo, $connection_options);
    // TEST DELETE.
    $query = (string) $this;
    $stmp = $connection->prepareQuery($query);
    $args = $values;
    foreach ($args as $id => $arg) {
      $stmp->bindParam($id, $arg);
    }
    $this->queryOptions = array(
      'target' => 'default',
      'return' => 2,
    );

    return $connection->query($stmp, $args, $this->queryOptions);
  }
}
