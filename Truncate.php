<?php

/**
 * @file
 * Definition of Drupal\Core\Database\Driver\oracle\Truncate
 */

namespace Drupal\Core\Database\Driver\oracle;

use Drupal\Core\Database\Query\Truncate as QueryTruncate;

class Truncate extends QueryTruncate {
  public function __toString() {
    return 'TRUNCATE TABLE {' . $this->connection->escapeTable($this->table) . '} ';
  }
}
