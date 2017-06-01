<?php

class Genghis_Models_Collection implements ArrayAccess, Genghis_JsonEncodable
{
    public $database;
    public $collection;
    public $collCon;

    public function __construct(Genghis_Models_Database $database, MongoDB\Model\CollectionInfo $collection, MongoDB\Collection $collCon)
    {
        $this->database   = $database;
        $this->collection = $collection;
        $this->collCon = $collCon;
    }

    public function offsetExists($id)
    {
        try {
            $this->findDocument($id);
        } catch (Genghis_HttpException $e) {
            if ($e->getStatus() == 404) {
                return false;
            } else {
                // catch and release
                throw $e;
            }
        }

        return true;
    }

    public function offsetGet($id)
    {
        return $this->findDocument($id);
    }

    public function offsetSet($id, $doc)
    {
        $this->findDocument($id);

        $query = array('_id' => $this->thunkMongoId($id));

        try {
            // updates no longer uses safe mode. prior self::safe()
            $result = $this->collCon->updateOne($query, array('$set' => $doc));
        } catch (MongoCursorException $e) {
            throw new Genghis_HttpException(400, ucfirst($e->doc['err']));
        }

        $result = $result->isAcknowledged();
        if (empty($result) || !$result) {
            throw new Genghis_HttpException;
        }
    }

    public function offsetUnset($id)
    {
        $this->findDocument($id);

        $query  = array('_id' => $this->thunkMongoId($id));
        $result = $this->collCon->deleteOne($query);

        $result = $result->isAcknowledged();
        if (empty($result) || !$result) {
            throw new Genghis_HttpException;
        }
    }

    public function getFile($id)
    {
        $mongoId = $this->thunkMongoId($id);
        if (!$mongoId instanceof MongoDB\BSON\ObjectID) {
            // for some reason this only works with MongoIds?
            throw new Genghis_HttpException(404, sprintf("GridFS file '%s' not found", $id));
        }

        $file = $this->getGrid()->openDownloadStream($mongoId);
        if (!$file) {
            throw new Genghis_HttpException(404, sprintf("GridFS file '%s' not found", $id));
        }

        // make response into an array to include the filename
        $doc = $this->getGrid()->findOne(array('_id' => $mongoId));
        $file = array('name' => $doc['filename'], 'stream' => $file);

        return $file;
    }

    public function putFile($doc)
    {
        $grid = $this->getGrid();

        if (!property_exists($doc, 'file')) {
            throw new Genghis_HttpException(400, 'Missing file');
        }
        $file = $doc->file;
        unset($doc->file);

        $extra = array();
        foreach ($doc as $key => $val) {
            if (!in_array($key, array('_id', 'filename', 'contentType', 'metadata'))) {
                throw new Genghis_HttpException(400, sprintf("Unexpected property: '%s'", $key));
            }

            if ($key === 'metadata') {
                $encoded = json_encode($val);
                if ($encoded == '{}' || $encoded == '[]') {
                    continue;
                }
            }

            // why the eff doesn't this accept an object like everything else? ugh.
            $extra[$key] = $val;
        }

        // create a stream resource from the file string
        $fileStream = fopen('data://text/plain;base64,' . base64_encode($file),'r');

        $id = $grid->uploadFromStream($doc->filename, $fileStream , $extra);

        return $this->findDocument( (string)$id );
    }

    public function deleteFile($id)
    {
        $mongoId = $this->thunkMongoId($id);
        if (!$mongoId instanceof MongoDB\BSON\ObjectID) {
            throw new Genghis_HttpException(404, sprintf("GridFS file '%s' not found", $id));
        }

        $grid = $this->getGrid();

        $file = $grid->findOne(array('_id' => $mongoId));
        if (empty($file)) {
            throw new Genghis_HttpException(404, sprintf("GridFS file '%s' not found", $id));
        }

        $result = $grid->delete($mongoId);
        if ($result instanceof MongoDB\GridFS\Exception\FileNotFoundException) {
            throw new Genghis_HttpException;
        }
    }

    public function findDocuments($query = null, $page = 1)
    {
        try {
            $query = Genghis_Json::decode($query);
        } catch (Genghis_JsonException $e) {
            throw new Genghis_HttpException(400, 'Malformed document');
        }

        $offset = Genghis_Api::PAGE_LIMIT * ($page - 1);

        // need to make an exclusive query to get the count as stuff like count($cursor->toArray()) is not working
        $count = $this->collCon
            ->count($query ? $query : array());

        $cursor = $this->collCon
            ->find($query ? $query : array(),
                array('limit' => Genghis_Api::PAGE_LIMIT, 'skip' => $offset));

        if (is_array($count) && isset($count['errmsg'])) {
            throw new Genghis_HttpException(400, $count['errmsg']);
        }

        return array(
            'count'     => $count,
            'page'      => $page,
            'pages'     => max(1, ceil($count / Genghis_Api::PAGE_LIMIT)),
            'per_page'  => Genghis_Api::PAGE_LIMIT,
            'offset'    => $offset,
            'documents' => iterator_to_array($cursor, false), // Mongo doesn't use sane keys.
        );
    }

    public function insert($data)
    {
        // todo: improved error handling
        try {
            $result = $this->collCon->insertOne($data);
        } catch (MongoCursorException $e) {
            throw new Genghis_HttpException(400, ucfirst($e->doc['err']));
        }

        $results = $result->isAcknowledged();
        if (empty($results) || !$results) {
            throw new Genghis_HttpException;
        }

        // add the document id to the results data
        $data->_id = $result->getInsertedId();

        return $data;
    }

    public function drop()
    {
        $this->collCon->drop();
    }

    // todo: count, indexes and stats are missing
    public function asJson()
    {
        $name  = $this->collection->getName();
        $colls = $this->database->database->listCollections();
        foreach ($colls as $coll) {
            if ($coll->getName() == $name) {
                return array(
                    'id'      => $coll->getName(),
                    'name'    => $coll->getName(),
                    'count'   => 0, //$coll->count(),
                    'indexes' => 0, // $coll->getIndexInfo(),
                    'stats'   => $this->stats(),
                );
            }
        }

        throw new Genghis_HttpException(404, sprintf("Collection '%s' not found in '%s'", $name, $this->database->name));
    }

    private function thunkMongoId($id)
    {

        if ($id[0] == '~') {
            return Genghis_Json::decode(base64_decode(substr($id, 1)));
        }

        return preg_match('/^[a-f0-9]{24}$/i', $id) ? new \MongoDB\BSON\ObjectID($id) : $id;
    }

    private function findDocument($id)
    {
        $doc = $this->collCon->findOne(array('_id' => $this->thunkMongoId($id)));
        if (!$doc) {
            throw new Genghis_HttpException(404, sprintf("Document '%s' not found in '%s'", $id, $this->collection->getName()));
        }

        return $doc;
    }

    private function isGridCollection()
    {
        return preg_match('/\.files$/', $this->collection->getName());
    }

    private function getGrid()
    {
        if (!($this->isGridCollection())) {
            $msg = sprintf("GridFS collection '%s' not found in '%s'", $this->collection->getName(), $this->database->name);
            throw new Genghis_HttpException(404, $msg);
        }

        if (!isset($this->grid)) {
            $prefix = preg_replace('/\.files$/', '', $this->collection->getName());
            $this->grid = $this->database->database->selectGridFSBucket( array('bucketName' => $prefix) );
        }

        return $this->grid;
    }

    private function decodeFile($data)
    {
        $count = 0;
        $data  = preg_replace('/^data:[^;]+;base64,/', '', $data, 1, $count);
        if ($count !== 1) {
            throw new Genghis_HttpException(400, 'File must be a base64 encoded data: URI');
        }

        return base64_decode(str_replace(' ', '+', $data));
    }

    private function stats()
    {
        return $this->database->database->command(array('collStats' => $this->collection->getName()));
    }

    private static function safe()
    {
        if (version_compare(Mongo::VERSION, '1.3.0', '>=')) {
            return array('w' => 1);
        } else {
            return array('safe' => true);
        }
    }
}
