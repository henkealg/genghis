<?php

class Genghis_Models_Server implements ArrayAccess, Genghis_JsonEncodable
{
    public $dsn;
    public $name;
    public $options;
    public $default;
    public $db;
    public $error;
    public $defaultOptions = array();

    private $connection;
    private $databases = array();

    public function __construct($dsn, $default = false)
    {
        $this->default = $default;

        if (version_compare(phpversion("mongo"), '1.3.4', '>=')) {
            $this->defaultOptions['connectTimeoutMS'] = 1000;
        } else {
            $this->defaultOptions['timeout'] = 1000;
        }

        try {
            $config = self::parseDsn($dsn);
            $this->name    = $config['name'];
            $this->dsn     = $config['dsn'];
            $this->options = $config['options'];

            if (isset($config['db'])) {
                $this->db = $config['db'];
            }
        } catch (Genghis_HttpException $e) {
            $this->name  = $dsn;
            $this->dsn   = $dsn;
            $this->error = $e->getMessage();
        }
    }

    public function offsetExists($name)
    {
        $list = $this->listDBs();
        foreach ($list as $db) {
            if ($db->getName() === $name) {
                return true;
            }
        }

        return false;
    }

    public function offsetGet($name)
    {
        if (!isset($this[$name])) {
            throw new Genghis_HttpException(404, sprintf("Database '%s' not found on '%s'", $name, $this->name));
        }

        if (!isset($this->databases[$name])) {
            $this->databases[$name] = new Genghis_Models_Database($this, $name);
        }

        return $this->databases[$name];
    }

    public function getConnection()
    {
        if (!isset($this->connection)) {
            $this->connection = new \MongoDB\Client($this->dsn, array_merge($this->defaultOptions, $this->options));
        }

        return $this->connection;
    }

    public function createDatabase($name)
    {
        if (isset($this[$name])) {
            throw new Genghis_HttpException(400, sprintf("Database '%s' already exists on '%s'", $name, $this->name));
        }

        try {
            $db = $this->connection->selectDatabase($name);
        } catch (Exception $e) {
            if (strpos($e->getMessage(), 'invalid name') !== false) {
                throw new Genghis_HttpException(400, 'Invalid database name');
            }
            throw $e;
        }
        // create a collection and then drop it again to create the actual database
        $db->createCollection('defaultCollection');

        // collection was dropped initially to create an empty database
        // with the current php driver this is no longer possible
        // defaultCollection stays

        return $this[$name];
    }

    public function listDatabases()
    {
        $dbs = array();
        $list = $this->listDBs();
        foreach ($list as $db) {
            $dbs[] = $this[$db->getName()];
        }

        return $dbs;
    }

    public function getDatabaseNames()
    {
        $names = array();
        $list = $this->listDBs();
        foreach ($list as $db) {
            $names[] = $db->getName();
        }

        return $names;
    }

    public function offsetSet($name, $value)
    {
        throw new Exception;
    }

    public function offsetUnset($name)
    {
        $this[$name]->drop();
    }

    public function asJson()
    {
        $server = array(
            'id'       => $this->name,
            'name'     => $this->name,
            'editable' => !$this->default,
        );

        if (isset($this->error)) {
            $server['error'] = $this->error;

            return $server;
        }

        try {
            $res = $this->listDBs();
            if (is_array($res) && isset($res['errmsg'])) {
                $server['error'] = sprintf("Unable to connect to Mongo server at '%s': %s", $this->name, $res['errmsg']);

                return $server;
            }

            $dbs = $this->getDatabaseNames();
            return array_merge($server, array(
                'size'      => 0, // todo: size set to 0 as of now. where do we get this value?
                'count'     => count($dbs),
                'databases' => $dbs,
            ));
        } catch (Exception $e) {
            $server['error'] = $e->getMessage();

            return $server;
        }
    }

    const DSN_PATTERN = "~^(?:mongodb://)?(?:(?P<username>[^:@]+):(?P<password>[^@]+)@)?(?P<host>[^,/@:]+)(?::(?P<port>\d+))?(?:/(?P<database>[^\?]+)?(?:\?(?P<options>.*))?)?$~";

    public static function parseDsn($dsn)
    {
        $chunks = array();
        if (!preg_match(self::DSN_PATTERN, $dsn, $chunks)) {
            throw new Genghis_HttpException(400, 'Malformed server DSN');
        }

        if (strpos($dsn, 'mongodb://') !== 0) {
            $dsn = 'mongodb://'.$dsn;
        }

        $dsnOpts = array();
        $options = array();
        if (isset($chunks['options'])) {
            parse_str(str_replace(';', '&', $chunks['options']), $dsnOpts);
            foreach ($dsnOpts as $name => $value) {
                switch ($name) {
                    case 'replicaSet':
                        $options[$name] = (string) $value;
                        break;

                    case 'connectTimeoutMS':
                    case 'socketTimeoutMS':
                        $options[$name] = intval($value);
                        break;

                    case 'slaveOk':
                    case 'safe':
                    case 'w':
                    case 'wtimeoutMS':
                    case 'fsync':
                    case 'journal':
                        throw new Genghis_HttpException(400, 'Unsupported connection option - ' . $name);

                    default:
                        throw new Genghis_HttpException(400, 'Malformed server DSN: Unknown connection option - ' . $name);
                }
            }
        }

        $name = $chunks['host'];
        if (isset($chunks['username']) && !empty($chunks['username'])) {
            $name = $chunks['username'].'@'.$name;
        }
        if (isset($chunks['port']) && !empty($chunks['port'])) {
            $port = intval($chunks['port']);
            if ($port !== 27017) {
                $name .= ':'.$port;
            }
        }
        if (isset($chunks['database']) && !empty($chunks['database']) && $chunks['database'] != 'admin') {
            $db   = $chunks['database'];
            $name .= '/'.$db;
        }

        $ret = compact('name', 'dsn', 'options');

        if (isset($db)) {
            $ret['db'] = $db;
        }

        return $ret;
    }

    private function listDBs()
    {
        // Fake it if we've got a single-db connection.
        // todo: stats returns a mongo cursor that needs to be handled
        // todo: when is this part triggered?
        if (isset($this->db)) {
            $stats = $this->getConnection()
                ->selectDatabase($this->db)
                ->command(array('dbStats'));
            return array(
                'totalSize' => $stats['fileSize'],
                'databases' => array(array('name' => $this->db)),
            );
        }

        return $this->getConnection()->listDatabases();
    }
}
