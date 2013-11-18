<?php

namespace Gaufrette\Adapter;

use Gaufrette\Adapter;
use Gaufrette\Util;
use OpenCloud\ObjectStore\Resource\Container as RackspaceContainer;

/**
 * Rackspace cloudfiles adapter
 *
 * @package Gaufrette
 * @author  Antoine Hérault <antoine.herault@gmail.com>
 */
class RackspaceCloudfiles implements Adapter,
                                     ChecksumCalculator
{
    protected $container;

    /**
     * Constructor
     *
     * @param RackspaceContainer $container A CF_Container instance
     */
    public function __construct(RackspaceContainer $container)
    {
        $this->container = $container;
    }

    /**
     * {@inheritDoc}
     *
     * @throws \InvalidResponseException
     */
    public function read($key)
    {
         $object = $this->container->getObject($key);
		return $object->getContent()->__toString();
    }

/**
 * Get a temporary url to download the $key param from CloudFiles
 * @param type $key
 * @param type $timeInSeconds
 * @param type $method
 * @return type
 */
	public function getTemporaryUrl($key, $timeInSeconds = 60, $method = 'GET') {
		$file = $this->container->getObject($key);
		return $file->getTemporaryUrl($timeInSeconds, $method);
	}

    /**
     * {@inheritDoc}
     */
    public function rename($key, $new)
    {
       $this->write($new, $this->read($key));
       $this->delete($key);

       return true;
    }

    /**
     * {@inheritDoc}
     */
    public function write($key, $content, array $metadata = array())
    {
		$dataObject = $this->container->uploadObject($key, $content, $metadata);
		if ($dataObject->getContentLength() === 0) {
			return false;
		}
        return Util\Size::fromContent($content);
    }

    /**
     * {@inheritDoc}
     */
    public function exists($key)
    {
        return false !== $this->tryGetObject($key);
    }

    /**
     * {@inheritDoc}
     */
    public function keys()
    {
        $keys = $this->container->list_objects(0, null, null);
        sort($keys);

        return $keys;
    }

    /**
     * {@inheritDoc}
     */
    public function mtime($key)
    {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    public function checksum($key)
    {
        $object = $this->container->getObject($key);

        return $object->getETag();
    }

    /**
     * {@inheritDoc}
     *
     * @throws InvalidResponseException
     * @throws SyntaxException
     */
    public function delete($key)
    {
        try {
            $this->container->delete_object($key);
        } catch (\NoSuchObjectException $e) {
            return false;
        }

        return true;
    }

    /**
     * {@inheritDoc}
     */
    public function isDirectory($key)
    {
        return false;
    }

    /**
     * Tries to get the object for the specified key or return false
     *
     * @param string $key The key of the object
     *
     * @return CF_Object or FALSE if the object does not exist
     */
    protected function tryGetObject($key)
    {
        try {
            return $this->container->getObject($key);
        } catch (\Guzzle\Http\Exception\BadResponseException $e) {
            // the NoSuchObjectException is thrown by the CF_Object during it's
            // creation if the object doesn't exist
			if ($e->getResponse()->getStatusCode() === 404) {
				return false;
			}
            throw $e;
        }
    }
}
