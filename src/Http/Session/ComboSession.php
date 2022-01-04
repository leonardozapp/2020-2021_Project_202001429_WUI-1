<?php

namespace App\Http\Session;

use Cake\Cache\Cache;
use Cake\Core\Configure;
use Cake\Http\Session\DatabaseSession;

class ComboSession extends DatabaseSession {//implements \SessionHandlerInterface {

	//protected $cacheKey;

	public function __construct() {
		//$this->cacheKey = Configure::read('Session.handler.cache');
		parent::__construct();
	}

// Read data from the session.
	public function read($id): string {
		//$result = Cache::read($id, $this->cacheKey);
		//if ($result) {
		//	return $result;
		//}
		return parent::read($id);
	}

// Write data into the session.
	public function write($id, $data): bool {
		//Cache::write($id, $data, $this->cacheKey);
		return parent::write($id, $data);
	}

// Destroy a session.
	public function destroy($id): bool {
		//Cache::delete($id, $this->cacheKey);
		return parent::destroy($id);
	}

// Removes expired sessions.
	public function gc($expires = null): bool {
		//return Cache::clear(true, $this->cacheKey) && parent::gc($expires);
		return parent::gc($expires);
	}

}
?>