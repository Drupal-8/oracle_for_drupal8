<?php

/**
 * @file
 * Definition of Drupal\Core\Database\Driver\oracle\Install\Tasks
 */

namespace Drupal\Core\Database\Driver\oracle\Install;

use Drupal\Core\Database\Database;
use Drupal\Core\Database\Install\Tasks as InstallTasks;
use Drupal\Core\Database\Driver\oracle\Connection;
use Drupal\Core\Database\DatabaseNotFoundException;

/**
 * Specifies installation tasks for Oracle and equivalent databases.
 */
class Tasks extends InstallTasks {
  /**
   * The PDO driver name for Oracle and equivalent databases.
   *
   * @var string
   */
  protected $pdoDriver = 'oci';

  protected $ORACLE_MAX_PDO_BIND_LENGTH_LIMITS = array(4000, 1332, 665);

  /**
   * Constructs a \Drupal\Core\Database\Driver\oracle\Install\Tasks object.
   */
  public function __construct() {
    $this->tasks[] = array(
      'function' => 'initializeDatabase',
      'arguments' => array(),
    );
  }

  /**
   * Returns a human-readable name string for Oracle and equivalent databases.
   */
  public function name() {
    return t('Oracle');
  }

  /**
   * Returns the minimum version for Oracle.
   */
  public function minimumVersion() {
    return NULL;
  }

  public function getFormOptions($database) {
    $form = parent::getFormOptions($database);

    $form['use_cache'] = array(
      '#title' => t('Use cache'),
      '#type'  => 'checkbox',
    );

    return $form;
  }

  /**
   * Check if we can connect to the database.
   */
  protected function connect() {
    try {
      // This doesn't actually test the connection.
      db_set_active();
      // Now actually do a check.
      //$test = Database::getConnection();
      //$this->pass('Drupal can CONNECT to the database ok.');

      syslog(LOG_ERR, "current dir: " . getcwd());
      $dir = getcwd() . '/core/lib/Drupal/Core/Database/Driver/oracle/resources';
      $this->determineSupportedBindSize();
      $this->createFailsafeObjects("{$dir}/table");
      $this->createFailsafeObjects("{$dir}/index");
      $this->createFailsafeObjects("{$dir}/sequence");
      $this->createObjects("{$dir}/function");
      $this->createObjects("{$dir}/procedure");
      $this->createSpObjects("{$dir}/type");
      $this->createSpObjects("{$dir}/package");
      $this->oracleQuery("begin dbms_utility.compile_schema(user); end;");
      
			Database::getConnection('default')->makePrimary();
      $this->pass('Drupal can CONNECT to the database Oracle ok.');
    }
    catch (\Exception $e) {
      // Attempt to create the database if it is not found.
      if ($e->getCode() == Connection::DATABASE_NOT_FOUND) {
        // Remove the database string from connection info.
        $connection_info = Database::getConnectionInfo();
        $database = $connection_info['default']['database'];
        unset($connection_info['default']['database']);

        // In order to change the Database::$databaseInfo array, need to remove
        // the active connection, then re-add it with the new info.
        Database::removeConnection('default');
        Database::addConnectionInfo('default', 'default', $connection_info['default']);

        try {
          // Now, attempt the connection again; if it's successful, attempt to
          // create the database.
          Database::getConnection()->createDatabase($database);
          Database::closeConnection();

          // Now, restore the database config.
          Database::removeConnection('default');
          $connection_info['default']['database'] = $database;
          Database::addConnectionInfo('default', 'default', $connection_info['default']);

          // Check the database connection.
          Database::getConnection();
          $this->pass('Drupal can CONNECT to the database ok.');
        }
        catch (DatabaseNotFoundException $e) {
          // Still no dice; probably a permission issue. Raise the error to the
          // installer.
          $this->fail(t('Database %database not found. The server reports the following message when attempting to create the database: %error.', array('%database' => $database, '%error' => $e->getMessage())));
        }
      }
      else {
        // Database connection failed for some other reason than the database
        // not existing.
        $this->fail(t('Failed to connect to your database server. The server reports the following message: %error.<ul><li>Is the database server running?</li><li>Does the database exist, and have you entered the correct database name?</li><li>Have you entered the correct username and password?</li><li>Have you entered the correct database hostname?</li></ul>', array('%error' => $e->getMessage())));
        return FALSE;
      }
    }
    return TRUE;
  }

  /**
   * Make Oracle Drupal friendly.
   */  
  public function initializeDatabase() {
  	
    try  {
      // This doesn't actually test the connection.
      db_set_active();

      syslog(LOG_ERR,"current dir: ".getcwd()); 	
      $dir = getcwd() . '/core/lib/Drupal/Core/Database/Driver/oracle/resources';
      $this->determineSupportedBindSize();
      $this->createFailsafeObjects("{$dir}/table");
      $this->createFailsafeObjects("{$dir}/index");
      $this->createFailsafeObjects("{$dir}/sequence");
      $this->createObjects("{$dir}/function");
      $this->createObjects("{$dir}/procedure");
      $this->createSpObjects("{$dir}/type");
      $this->createSpObjects("{$dir}/package");
      $this->oracle_query("begin dbms_utility.compile_schema(user); end;");
   
  	  $this->pass(t('Oracle has initialized itself.'));
        
      Database::getConnection('default')->makePrimary();
    }
    catch (\Exception $e) {
      syslog(LOG_ERR, $e->getMessage());
      $this->fail(t('Drupal could not be correctly setup with the existing database. Revise any errors.'));
    }
  }

  private function oracleQuery($sql, $args = NULL) {
    return Database::getConnection()->oracleQuery($sql, $args);
  }

  private function determineSupportedBindSize() {
    $this->failsafeDdl('create table bind_test (val varchar2(4000 char))');
    $ok = FALSE;

    foreach ($this->ORACLE_MAX_PDO_BIND_LENGTH_LIMITS as $length) {
      try {
        syslog(LOG_ERR, "trying to bind $length bytes...");
        $determined_size = $length;
        $this->oracleQuery('insert into bind_test values (?)', array(str_pad('a', $length, 'a')));
        syslog(LOG_ERR, "bind succeeded.");
        $ok = TRUE;
        break;
      }
      catch (\Exception $e) {}
    }

    if (!$ok) {
      throw new Exception('unable to determine PDO maximum bind size');
    }

    $this->failsafeDdl("drop table oracle_bind_size");        
    $this->failsafeDdl("create table oracle_bind_size as select $determined_size val from dual");
  }

  private function createSpObjects($dir_path) {
    $dir = opendir($dir_path);

    while($name = readdir($dir)) {
      if (in_array($name, array('.', '..', '.DS_Store', 'CVS'))) {
        continue;
      }
      if (is_dir($dir_path . "/" . $name)) {
        $this->createSpObject($dir_path . "/" . $name);
      }
    }
  }

  private function createSpObject($dir_path) {
    $dir = opendir($dir_path);
    $spec = $body = "";

    while ($name = readdir($dir)) {
      if (substr($name, -4) == '.pls') {
        $spec = $name;
      }
      elseif (substr($name, -4) == '.plb') {
        $body = $name;
      }
    }

    $this->createObject($dir_path . "/" . $spec);
    if ($body) {
      $this->createObject($dir_path . "/" . $body);
    }
  }

  private function createObjects($dir_path) {
    $dir = opendir($dir_path);
    while($name = readdir($dir)) {
      if (in_array($name, array('.', '..', '.DS_Store', 'CVS'))) {
        continue;
      }
      $this->createObject($dir_path . "/" . $name);
    }
  }

  private function createObject($file_path) {
    syslog(LOG_ERR, "creating object: $file_path");

    try {
      $this->oracleQuery($this->getPhpContents($file_path));
    }
    catch (\Exception $e) {
      syslog(LOG_ERR, "object $file_path created with errors");         
    }
  }

  private function createFailsafeObjects($dir_path) {
	  $dir = opendir($dir_path);

    while ($name = readdir($dir)) {
      if (in_array($name, array('.', '..', '.DS_Store', 'CVS'))) {
        continue;
      }
      syslog(LOG_ERR, "creating object: $dir_path/$name");
      $this->failsafeDdl($this->getPhpContents($dir_path . "/" . $name));
    }
  }

  private function failsafeDdl($ddl) {
    $this->oracleQuery("begin execute immediate '" . str_replace("'", "''", $ddl) . "'; exception when others then null; end;");
  }

  private function getPhpContents($filename) {
    if (is_file($filename)) {
      ob_start();
      require_once $filename;
      $contents = ob_get_contents();
      ob_end_clean();
      return $contents;
    }
    else {
      syslog(LOG_ERR, "error: file " . $filename . " does not exists");
    }
    return FALSE;
  }
}
