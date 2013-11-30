<?php

/**
 * @file
 * Definition of Drupal\Core\Database\Driver\oracle\Connection
 */

namespace Drupal\Core\Database\Driver\oracle;

use Drupal\Core\Database\Database;
use Drupal\Core\Database\Connection as DatabaseConnection;
use Drupal\Core\Database\DatabaseNotFoundException;
use Drupal\Core\Database\TransactionCommitFailedException;
use Drupal\Core\Database\StatementInterface;
use Drupal\Core\Database\Driver\oracle\Statement;
use Drupal\Core\Database\IntegrityConstraintViolationException;

/**
 * @addtogroup database
 * @{
 */

class Connection extends DatabaseConnection {
  /**
   * Error code for "Unknown database" error.
   */
  const DATABASE_NOT_FOUND = 0;

	/**
	 * used to replace '' character in queries.
   */
  const ORACLE_EMPTY_STRING_REPLACER = '^'; 
	
	/**
	 * maximum oracle identifier length (e.g. table names cannot exceed this length)
   */
  const ORACLE_IDENTIFIER_MAX_LENGTH = 30; 

  /**
	 *  prefix used for long identifier keys
	 */
  const ORACLE_LONG_IDENTIFIER_PREFIX = 'L#'; 

  /**
	 *  prefix used for BLOB values
	 */
  const ORACLE_BLOB_PREFIX = 'B^#'; 

  /**
	 * maximum length for a string value in a table column in oracle (affects schema.inc table creation)
   */
  const ORACLE_MAX_VARCHAR2_LENGTH = 4000; 

  /**
	 * maximum length of a string that PDO_OCI can handle (affects runtime blob creation) @FIXME this should be 4000 once PDO_OCI is fixed
	 */
  const ORACLE_MIN_PDO_BIND_LENGTH = 3999; 

  /**
	 * alias used for queryRange filtering (we have to remove that from resultsets)
	 */
  const ORACLE_ROWNUM_ALIAS = 'RWN_TO_REMOVE'); 
  
	/**
	 * long identifier handler class.
	 */
  public $lih;

  private $use_cache = FALSE;

  /**
	 * we are being use to connect to an external oracle database.
	 */
  public $external = FALSE;

  private $oraclePrefix = array();

  private $max_varchar2_bind_size = ORACLE_MIN_PDO_BIND_LENGTH;

  /**
   * Constructs a \Drupal\Core\Database\Driver\oracle\Connection object.
   */
  public function __construct(\PDO $connection, array $connection_options = array()) {
    global $oracle_user;

    parent::__construct($connection, $connection_options);

    // We don't need a specific PDOStatement class here, we simulate it below.
    $this->statementClass = NULL;

  	// This driver defaults to transaction support, except if explicitly passed FALSE.
    $this->transactionSupport = !isset($connection_options['transactions']) || ($connection_options['transactions'] !== FALSE);

    // Transactional DDL is not available in Oracle,
    $this->transactionalDDLSupport = FALSE;

    // needed by DatabaseConnection.getConnectionOptions
    $this->connectionOptions = $connection_options;

    // Default to TCP connection on port 1521.
    if (empty($connection_options['port'])) 
      $connection_options['port'] = 1521;
   
    if (isset($connection_options['use_cache'])) 
     $this->use_cache= $connection_options['use_cache'];
     
    $oracle_user= $connection_options['username'];

    // Use database as TNSNAME
    if ($connection_options['host'] == 'USETNS') 
      $dsn = 'oci:dbname='.$connection_options['database'] .';charset=AL32UTF8';
    else  // Use host/port/database
      $dsn = 'oci:dbname=//'.$connection_options['host'].':'.$connection_options['port'].'/' . $connection_options['database'].';charset=AL32UTF8';
        
    parent::__construct($dsn, $connection_options['username'], $connection_options['password'], array(

      // Convert numeric values to strings when fetching.
      \PDO::ATTR_STRINGIFY_FETCHES => TRUE,
      
      // Force column names to lower case.
      \PDO::ATTR_CASE => \PDO::CASE_LOWER
      
    ));
    
    // FIXME: already done by DatabaseConnection but anyway seems not to be hold
    $this->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);
    $this->setAttribute(\PDO::ATTR_CASE, \PDO::CASE_LOWER);
    $this->setAttribute(\PDO::ATTR_STRINGIFY_FETCHES, TRUE);
    
    $options= array('return' => Database::RETURN_NULL);
    
    // setup session attributes
    try {
  	   $stmt= $this->dbh->prepare("begin ? := setup_session; end;");
       $stmt->bindParam(1, $this->max_varchar2_bind_size, \PDO::PARAM_INT | \PDO::PARAM_INPUT_OUTPUT, 32);

       $stmt->execute();
    }
    catch (\Exception $ex) {
       // ignore at install time or external databases 
       
       // fallback to minimum bind size
       $this->max_varchar2_bind_size= ORACLE_MIN_PDO_BIND_LENGTH;
       $this->external= true; // connected to an external oracle database (not necessarly a drupal schema)
    }    
    
    // initialize the long identifier handler
    if (!$this->external) $this->lih= new DatabaseLongIdentifierHandlerOracle($this);
    
    // initialize db_prefix cache
    $this->oraclePrefix= array();
  }

  /**
   * {@inheritdoc}
   */
  public static function open(array &$connection_options = array()) {
    // Default to TCP connection on port 1521.
    if (empty($connection_options['port'])) {
      $connection_options['port'] = 1521;
    }

    // Use database as TNSNAME
    if ($connection_options['host'] == 'USETNS') {
      $dsn = 'oci:dbname=' . $connection_options['database'] . ';charset=AL32UTF8';
    }
    else { // Use host/port/database
      $dsn = 'oci:dbname=//' . $connection_options['host'] . ':' . $connection_options['port'] . '/' . $connection_options['database'] . ';charset=AL32UTF8';
    }

    // Allow PDO options to be overridden.
    $connection_options += array(
      'pdo' => array(),
    );
    $connection_options['pdo'] += array(
      \PDO::ATTR_STRINGIFY_FETCHES => TRUE,
      \PDO::ATTR_CASE => \PDO::CASE_LOWER,
      \PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION,
    );

    $pdo = new \PDO($dsn, $connection_options['username'], $connection_options['password'], $connection_options['pdo']);

    return $pdo;
  }

  /**
   * Oracle compatibility implementation for the query() SQL function.
   */
  public function query($query, array $args = array(), $options = array(), $retried = 0) {
    global $oracle_debug;

  	// Use default values if not already set.
    $options += $this->defaultOptions();

    try {
      if ($query instanceof StatementInterface) {
				if ($oracle_debug) syslog(LOG_ERR, "query: " . $query->queryString . " args: " . print_r($args, true));
        $stmt = $query;
        $stmt->execute(empty($args) ? NULL : (array) $args, $options);
      }
      else {
      	$modified = $this->expandArguments($query, $args);
        $stmt = $this->prepareQuery($query);
        $stmt->execute($this->cleanupArgs($args), $options);
      }

      switch ($options['return']) {
        case Database::RETURN_STATEMENT:
          return $stmt;
        case Database::RETURN_AFFECTED:
          return $stmt->rowCount();
        case Database::RETURN_INSERT_ID:
          return (isset($options['sequence_name']) ? $this->lastInsertId($options['sequence_name']) : FALSE);
        case Database::RETURN_NULL:
          return;
        default:
          throw new \PDOException('Invalid return directive: ' . $options['return']);
      }
    }
    catch (\PDOException $e) {
      if ($query instanceof StatementInterface) {
        $query_string = $stmt->queryString;
      }
      else {
        $query_string = $query;
      }

      if ($this->exceptionQuery($query_string) && $retried != 1) {
  	  	return $this->query($query_string, $args, $options, 1);
      }

      // Catch long identifier errors for alias columns.
      if (isset($e->errorInfo) && is_array($e->errorInfo) && $e->errorInfo[1] == '00972' && $retried !=2 && !$this->external) {
        $this->lih->findAndRemoveLongIdentifiers($query_string);
        return $this->query($query_string, $args, $options, 2);
      }

      try {
        $this->rollBack();
      }
      catch (\Exception $rex) {
        syslog(LOG_ERR, "rollback ex: " . $rex->getMessage());
      }

      if ($options['throw_exception']) {
        syslog(LOG_ERR, "error query: " . $query_string . (isset($stmt) && $stmt instanceof DatabaseStatementOracle ? " (prepared: ".$stmt->getQueryString() . " )" : "") . " e: " . $e->getMessage() . " args: " . print_r($args, TRUE));

        $ex = new \PDOException($query_string . (isset($stmt) && $stmt instanceof DatabaseStatementOracle ? " (prepared: " . $stmt->getQueryString() . " )" : "") . " e: " . $e->getMessage() . " args: " . print_r($args, TRUE));
        $ex->errorInfo = $e->errorInfo;

        if ($ex->errorInfo[1] == '1') {
        	$ex->errorInfo[0] = '23000';
        }
        throw $ex;
      }

      return NULL;
    }
  }

  public function queryRange($query, $from, $count, array $args = array(), array $options = array()) {
    $start = (int) $from + 1;
    $end = (int) $count + (int) $from;

    $query_string = 'SELECT * FROM (SELECT TAB.*, ROWNUM ' . ORACLE_ROWNUM_ALIAS . ' FROM (' . $query . ') TAB) WHERE ' . ORACLE_ROWNUM_ALIAS . ' BETWEEN ';
    if (Connection::isAssoc($args)) {
    	$args['oracle_rwn_start'] = $start;
    	$args['oracle_rwn_end'] = $end;
      $query_string .= ':oracle_rwn_start AND :oracle_rwn_end';
    }
    else {
    	$args[] = $start;
    	$args[] = $end;
      $query_string .= '? AND ?';
    }

    return $this->query($query_string, $args, $options);
  }

  public function queryTemporary($query, array $args = array(), array $options = array()) {
    $tablename = $this->generateTemporaryTableName();
    try {
      db_query("DROP TABLE {". $tablename ."}");
    }
    catch (\Exception $ex) {
      /* ignore drop errors */
    }
    $this->query('CREATE GLOBAL TEMPORARY TABLE {' . $tablename . '} ON COMMIT PRESERVE ROWS AS ' . $query, $args, $options);
    return $tablename;
  }

  public function driver() {
    return 'oracle';
  }

  public function databaseType() {
    return 'oracle';
  }

  /**
   * Overrides \Drupal\Core\Database\Connection::createDatabase().
   *
   * @param string $database
   *   The name of the database to create.
   *
   * @throws DatabaseNotFoundException
   */
  public function createDatabase($database) {
    // Database can be created manualy only.
  }

  public function mapConditionOperator($operator) {
    // We don't want to override any of the defaults.
    return NULL;
  }

  public function nextId($existing_id = 0) {
    // Retrive the name of the sequence. This information cannot be cached
    // because the prefix may change, for example, like it does in simpletests.
    $sequence_name = str_replace('"', '', $this->makeSequenceName('sequences', 'value'));
    $id = $this->query("SELECT " . $sequence_name . ".nextval FROM DUAL")->fetchField();
    if ($id > $existing_id) {
      return $id;
    }

    // Reset the sequence to a higher value than the existing id.
    $this->query("DROP SEQUENCE " . $sequence_name);
    $this->query("CREATE SEQUENCE " . $sequence_name . " START WITH " . ($existing + 1));

    // Retrive the next id. We know this will be as high as we want it.
    $id = $this->query("SELECT " . $sequence_name . ".nextval FROM DUAL")->fetchField();

    return $id;
  }

  // Help method to check if array is associative.
  public static function isAssoc($array) {
    return (is_array($array) && 0 !== count(array_diff_key($array, array_keys(array_keys($array)))));
  }

  public function oraclePrepare($query, $args = NULL) {
    return $this->dbh->prepare($query);
  }

  public function makePrimary() {
    $this->lih = new DatabaseLongIdentifierHandlerOracle($this);
    $this->external = FALSE; // ok we are installing a primary database.
  }

  public function oracleQuery($query, $args = NULL) {
  	$stmt = $this->dbh->prepare($query);

    try {
      $stmt->execute($args);
    }
    catch (\Exception $e) {
      syslog(LOG_ERR, "error: {$e->getMessage()} {$query}");
      throw $e;
    }

    return $stmt;
  }

  private function exceptionQuery(&$unformattedQuery) {
    global $oracle_exception_queries;

  	if (!is_array($oracle_exception_queries)) {
      return FALSE;
  	}

    $count = 0;
    $oracle_unformatted_query = preg_replace(
      array_keys($oracle_exception_queries),
      array_values($oracle_exception_queries),
      $oracle_unformatted_query,
      -1,
      $count
    );

    return $count;
  }

  public function lastInsertId($name = NULL) {
    if (!$name) {
      throw new Exception('The name of the sequence is mandatory for Oracle');
    }

  	try {
  	  return $this->oracleQuery($this->prefixTables("select " . $name . ".currval from dual", TRUE))->fetchColumn();
  	}
  	catch (\Exception $e) {
      // Ignore if CURRVAL not set (may be an insert that specified the serial field).
      syslog(LOG_ERR, " currval: " . print_r(debug_backtrace(FALSE), TRUE));
    }
  }

  public function generateTemporaryTableName() {
    // FIXME: create a cleanup job
    return "TMP_" . $this->oracleQuery("SELECT userenv('sessionid') FROM dual")->fetchColumn() . "_" . $this->temporaryNameIndex++;
  }

  public function quote($string, $parameter_type = PDO::PARAM_STR) {
  	return "'" . str_replace("'", "''", $string) . "'";
  }

  public function version() {
    try {
      return $this->getAttribute(PDO::ATTR_SERVER_VERSION);
    }
    catch (\Exception $e) {
      return $this->oracleQuery("SELECT regexp_replace(banner,'[^0-9\.]','') FROM v\$version WHERE banner LIKE 'CORE%'")->fetchColumn();
    }
  }

  /**
   * @todo Remove this as soon as db_rewrite_sql() has been exterminated.
   */
  public function distinctField($table, $field, $query) {
    $field_to_select = 'DISTINCT(' . $table . '.' . $field . ')';
    // (?<!text) is a negative look-behind (no need to rewrite queries that already use DISTINCT).
    return preg_replace('/(SELECT.*)(?:' . $table . '\.|\s)(?<!DISTINCT\()(?<!DISTINCT\(' . $table . '\.)' . $field . '(.*FROM )/AUsi', '\1 ' . $field_to_select . '\2', $query);
  }

  public function checkDbPrefix($db_prefix) {
	  if (empty($db_prefix)) {
      return;
    }
    if (!isset($this->oraclePrefix[$db_prefix])) {
      $this->oraclePrefix[$db_prefix] = $this->oracleQuery("SELECT identifier.check_db_prefix(?) FROM dual", array($db_prefix))->fetchColumn();
    }

    return $this->oraclePrefix[$db_prefix];
  }

  public function prefixTables($sql, $quoted = FALSE) {
	  $quote = '';
	  $ret = '';

	  if (!$quoted) {
	    $quote = '"';
	  }

    // Replace specific table prefixes first.
    foreach ($this->prefixes as $key => $val) {
      $dp = $this->checkDbPrefix($val);
      if (is_object($sql)) {
        $sql = $sql->getQueryString();
      }
      $sql = strtr($sql, array('{' . strtoupper($key) . '}' => $quote . (empty($dp) ? strtoupper($key) : strtoupper($dp) . '"."' . strtoupper($key)) . $quote));
    }

	  $dp = $this->checkDbPrefix($this->tablePrefix());
	  $ret = strtr($sql, array('{' => $quote . (empty($dp) ? '' : strtoupper($dp) . '"."'), '}' => $quote));

	  return $this->escapeAnsi($ret);
  }

  public function prepareQuery($query) {
	  $iquery = md5(($this->external ? 'E|' : '') . $this->prefixTables($query, TRUE));

    if (empty($this->preparedStatements[$iquery])) {
      $oquery = "";
      if ($this->use_cache) {
         $cached = cache_get($iquery, 'cache_oracle');
         if ($cached) {
            $oquery = $cached->data;
         }
      }

      if (!$oquery) {
        $oquery = $query;
        $oquery = $this->escapeEmptyLiterals($oquery);
        $oquery = $this->escapeAnsi($oquery);

        if (!$this->external) {
          $oquery = $this->lih->escapeLongIdentifiers($oquery);
        }

        $oquery = $this->escapeReserved($oquery);
        $oquery = $this->escapeCompatibility($oquery);
        $oquery = $this->prefixTables($oquery, TRUE);
        $oquery = $this->escapeIfFunction($oquery);

        if ($this->use_cache) {
          cache_set($iquery, $oquery, 'cache_oracle');
        }
      }
      $this->preparedStatements[$iquery] = $this->dbh->prepare($oquery);
    }
	  return $this->preparedStatements[$iquery];
  }

  /**
   * Oracle-specific implementation of DatabaseConnection::prepare().
   *
   * We don't use prepared statements at all at this stage. We just create
   * a Statement object, that will create a PDOStatement
   * using the semi-private PDOPrepare() method below.
   */
  public function prepare($statement, array $driver_options = array()) {
    return new Statement($this->connection, $this, $statement, $driver_options);
  }

  // @todo I don't like this too.
  /*public function PDOPrepare($query, array $options = array()) {
    return parent::prepare($query, $options);
  }*/

  private function escapeAnsi($query) {
  	if (preg_match("/^select /i", $query) && !preg_match("/^select(.*)from/ims", $query)) {
  	  $query .= ' FROM DUAL';
  	}

    $search = array(
      '/("\w+?")/e',
      "/([^\s\(]+) & ([^\s]+) = ([^\s\)]+)/",
      "/([^\s\(]+) & ([^\s]+) <> ([^\s\)]+)/", // bitand
      '/^RELEASE SAVEPOINT (.*)$/',
    );

    $replace = array(
      "strtoupper('\\1')",
      "BITAND(\\1,\\2) = \\3",
      "BITAND(\\1,\\2) <> \\3",
      "begin null; end;",
    );

    return str_replace('\\"', '"', preg_replace($search, $replace, $query));
  }

  private function escapeEmptyLiteral($match) {
	  if ($match[0] == "''") {
	    return "'" . ORACLE_EMPTY_STRING_REPLACER . "'";
	  }
	  else {
	    return $match[0];
	  }
  }

  private function escapeEmptyLiterals($query) {
    if (is_object($query)) {
      $query = $query->getQueryString();
    }
    return preg_replace_callback("/'.*?'/", array($this, 'escapeEmptyLiteral'), $query);
  }

  private function escapeIfFunction($query) {
    if (is_object($query)) {
      $query = $query->getQueryString();
    }
    return preg_replace("/IF\s*\((.*?),(.*?),(.*?)\)/", 'case when \1 then \2 else \3 end', $query);
  }

  private function escapeReserved($query) {
    if (is_object($query)) {
      $query = $query->getQueryString();
    }
    $ddl = !((boolean) preg_match('/^(select|insert|update|delete)/i', $query));

    $search = array(
      "/({)(\w+)(})/e", // escapes all table names
      "/({L#)([0-9]+)(})/e", // escapes long id
      "/(\:)(uid|session|file|access|mode|comment|desc|size|start|end)/e",
      "/(<uid>|<session>|<file>|<access>|<mode>|<comment>|<desc>|<size>" . ($ddl ? '' : '|<date>') . ")/e",
      '/([\(\.\s,\=])(uid|session|file|access|mode|comment|desc|size' . ($ddl ? '' : '|date') . ')([,\s\=)])/e',
      '/([\(\.\s,])(uid|session|file|access|mode|comment|desc|size' . ($ddl ? '' : '|date') . ')$/e',
    );

	  $replace = array(
      "'\"\\1'.strtoupper('\\2').'\\3\"'",
      "'\"\\1'.strtoupper('\\2').'\\3\"'",
      "'\\1'.'db_'.'\\2'.'\\3'",
      "strtoupper('\"\\1\"')",
      "'\\1'.strtoupper('\"\\2\"').'\\3'",
      "'\\1'.strtoupper('\"\\2\"')",
    );

    return preg_replace($search, $replace, $query);
  }

  public function removeFromCachedStatements($query) {
    if (is_object($query)) {
      $query = $query->getQueryString();
    }
    $iquery = md5($this->prefixTables($query, TRUE));
    if (isset($this->preparedStatements[$iquery])) {
      unset($this->preparedStatements[$iquery]);
    }
  }

  private function escapeCompatibility($query) {
    if (is_object($query)) {
      $query = $query->getQueryString();
    }
		$search = array(
      "''||", // remove empty concatenations leaved by concatenate_bind_variables
      "||''",
      "IN ()", // translate 'IN ()' to '= NULL' they do not match anything anyway (always false)
      "IN  ()",
      '(FALSE)',
      'POW(',
      ") AS count_alias", // ugly hacks here
      '"{URL_ALIAS}" GROUP BY path',
      "ESCAPE '\\\\'",
    );

		$replace = array(
      "",
      "",
      "= NULL",
      "= NULL",
      "(1=0)",
      "POWER(",
      ") count_alias",// ugly hacks replace strings here
      '"{URL_ALIAS}" GROUP BY SUBSTRING_INDEX(source, \'/\', 1)',
      "ESCAPE '\\'",
    );

		return str_replace($search, $replace, $query);
  }

  public function makeSequenceName($table, $field) {
    return $this->schema()->makeSequenceName($table, $field);
  }

  public function cleanupArgValue($value) {
  	if (is_string($value)) {
      if ($value == '') {
        return ORACLE_EMPTY_STRING_REPLACER;
      }
      elseif (strlen($value) > $this->max_varchar2_bind_size) {
        return $this->writeBlob($value);
      }
      else {
        return $value;
      }
  	}
    else {
      return $value;
    }
  }

  public function cleanupArgs($args) {
    if ($this->external) {
      return $args;
    }

  	$ret = array();
  	if (Connection::isAssoc($args)) {
  	  foreach ($args as $key => $value) {
        $key = Connection::escapeReserved($key); // bind variables cannot have reserved names
        $key = $this->lih->escapeLongIdentifiers($key);
        $ret[$key] = $this->cleanupArgValue($value);
  	  }
    }
    else { // indexed array
      foreach ($args as $key => $value) {
        $ret[$key] = $this->cleanupArgValue($value);
      }
    }

  	return $ret;
  }

  public function writeBlob($value) {
  	$hash = md5($value);
    $file = "/tmp/blob/{$hash}";

    if (!file_exists($file)) {
      file_put_contents($file, $value);
    }
    return ORACLE_BLOB_PREFIX . $hash;
  }

  public function readBlob($handle) {
    $hash = (string) substr($handle, strlen(ORACLE_BLOB_PREFIX));
    $file = "/tmp/blob/{$hash}";
    return file_get_contents($file);
  }

  public function cleanupFetchedValue($value) {
    if (is_string($value)) {
      if ($value == ORACLE_EMPTY_STRING_REPLACER) {
        return '';
      }
      elseif ($this->isBlob($value)) {
        return $this->readBlob($value);
      }
      else {
        return $value;
      }
    }
    else {
      return $value;
    }
  }

  public function resetLongIdentifiers() {
  	if (!$this->external) {
      $this->lih->resetLongIdentifiers();
  	}
  }

  public static function isLongIdentifier($key) {
  	return (substr(strtoupper($key), 0, strlen(ORACLE_LONG_IDENTIFIER_PREFIX)) == ORACLE_LONG_IDENTIFIER_PREFIX);
  }

  public static function isBlob($value) {
  	return (substr($value, 0, strlen(ORACLE_BLOB_PREFIX)) == ORACLE_BLOB_PREFIX);
  }

  private static function stringToStream($value) {
    $stream = fopen('php://memory', 'a');
    fwrite($stream, $value);
    rewind($stream);
    return $stream;
  }

  /**
   * Overridden to work around issues to Oracle not supporting transactional DDL.
   */
  protected function popCommittableTransactions() {
    // Commit all the committable layers.
    foreach (array_reverse($this->transactionLayers) as $name => $active) {
      // Stop once we found an active transaction.
      if ($active) {
        break;
      }

      // If there are no more layers left then we should commit.
      unset($this->transactionLayers[$name]);
      if (empty($this->transactionLayers)) {
        if (!$this->connection->commit()) {
          throw new TransactionCommitFailedException();
        }
      }
      else {
        // Attempt to release this savepoint in the standard way.
        try {
          $this->query('RELEASE SAVEPOINT ' . $name);
        }
        catch (\Exception $e) {
          throw $e;
        }
      }
    }
  }
}

/**
 * MAGIC CLASS O_o
 *
 * @todo WHAT THE FUCK IS IT???
 */
class DatabaseLongIdentifierHandlerOracle {
  // Holds search reg exp pattern to match known long identifiers.
  private $searchLongIdentifiers = array();

  // Holds replacement string to replace known long identifiers.
  private $replaceLongIdentifiers = array();

  // Holds long identifier hashmap.
  private $hashLongIdentifiers = array();

  // The parent connection.
  private $connection;

  public function __construct($connection) {
    $this->connection = $connection;

    // Load long identifiers for the first time in this connection.
    $this->resetLongIdentifiers();
  }

  public function escapeLongIdentifiers($query) {
    $ret = "";

    // Do not replace things in literals.
    $literals = array();
    preg_match_all("/'.*?'/", $query, $literals);
    $literals    = $literals[0];
    $replaceable = preg_split("/'.*?'/", $query);
    $lidx        = 0;

    // Assume that a query cannot start with a literal and that.
    foreach ($replaceable as $toescape) {
      $ret .= $this->removeLongIdentifiers($toescape) . (isset($literals[$lidx]) ? $literals[$lidx++] : "");
    }
    return $ret;
  }

  public function removeLongIdentifiers($query_part) {
    if (count($this->searchLongIdentifiers)) {
      return preg_replace($this->searchLongIdentifiers, $this->replaceLongIdentifiers, $query_part);
    }
    else {
      return $query_part;
    }
  }

  // TODO: would be wonderfull to enble a memcached switch here
  public function resetLongIdentifiers() {
    try  {
      $result = $this->connection->oracleQuery("select id, identifier from long_identifiers where substr(identifier,1,3) not in ('IDX','TRG','PK_','UK_') order by length(identifier) desc");

      while ($row = $result->fetchObject()) {
        $this->searchLongIdentifiers[] = '/\b' . $row->identifier . '\b/i';
        $this->replaceLongIdentifiers[] = ORACLE_LONG_IDENTIFIER_PREFIX . $row->id;
        $this->hashLongIdentifiers[ORACLE_LONG_IDENTIFIER_PREFIX . $row->id] = strtolower($row->identifier);
      }
    }
    catch (Exception $e) {
    	// Ignore until long_identifiers table is not created.
    }
  }

  public function findAndRecordLongIdentifiers($query_part) {
  	preg_match_all("/\w+/", $query_part, $words);
  	$words = $words[0];
  	foreach ($words as $word) {
  	  if (strlen($word) > ORACLE_IDENTIFIER_MAX_LENGTH) {
        $this->connection->schema()->oid($word);
  	  }
    }
  }

  public function findAndRemoveLongIdentifiers($query) {
    $this->connection->removeFromCachedStatements($query);

    // Do not replace things in literals.
    $literals = array();
    $replaceable = preg_split("/'.*?'/", $query);
    $lidx = 0;

    // Assume that a query cannot start with a literal and that.
    foreach ($replaceable as $toescape) {
      $this->findAndRecordLongIdentifiers($toescape);
    }
    $this->resetLongIdentifiers();
  }

  public function longIdentifierKey($key) {
  	return $this->hashLongIdentifiers[strtoupper($key)];
  }
}
