import json

from cachetools.lru import LRUCache
from redis import Redis
from redisbloom.client import Client as RedisBloom

from utils.dgraph import get_client


class SimpleCache(object):
    def __init__(self, objname, localsize):
        super(SimpleCache, self).__init__()

        self.objname = objname
        self.local_cache = LRUCache(maxsize=localsize)
        self.redis = Redis("localhost")
        self.set_timeout = False

    def _key(self, key):
        return f"{self.objname}-{key}"

    def __setitem__(self, key, value):
        self.local_cache[self._key(key)] = value

        if self.set_timeout:
            timeout = 300
            self.redis.setex(self._key(key), timeout, value)
        else:
            self.redis.set(self._key(key), value)

    def __contains__(self, key):
        local_result = self.local_cache.get(self._key(key), None)
        if local_result is not None:
            return True

        redis_result = self.redis.get(self._key(key))
        if redis_result is not None:
            self[key] = redis_result.decode()
            return True

        return False

    def __getitem__(self, key):
        local_result = self.local_cache.get(self._key(key), None)
        if local_result is not None:
            return local_result

        redis_result = self.redis.get(self._key(key))
        if redis_result is not None:
            self[key] = redis_result.decode()
            return redis_result.decode()

        return False

    def close(self):
        self.redis.close()


class SpicyCache(SimpleCache):
    def __init__(self, objname, localsize):
        super(SpicyCache, self).__init__(objname, localsize)

        self.set_timeout = True
        self.bloom = RedisBloom(port=6378)
        self.dgraph, self.stub = get_client()
        self.txn = self.dgraph.txn()

    def __contains__(self, key):
        if super(SpicyCache, self).__contains__(key):
            return True

        if self.bloom.bfExists(self.objname, self._key(key)) == 0:
            query = (
            """query all($a: string) { all(func: eq(%s, $a)) { uid } }"""
            % self.objname
            )
            dgraph_result = self.txn.query(query, variables={"$a": str(key)})
            thing = json.loads(dgraph_result.json)
            if len(thing["all"]) > 0:
                self[key] = thing["all"][0]["uid"]
                return True

        return False

    def __getitem__(self, key):
        item = super(SpicyCache, self).__getitem__(key)
        if item is not None:
            return item

        if self.bloom.bfExists(self.objname, self._key(key)) == 0:
            query = (
            """query all($a: string) { all(func: eq(%s, $a)) { uid } }"""
            % self.objname
            )
            dgraph_result = self.txn.query(query, variables={"$a": str(key)})
            thing = json.loads(dgraph_result.json)
            if len(thing["all"]) > 0:
                self[key] = thing["all"][0]["uid"]
                return thing["all"][0]["uid"]

        return None

    def close(self):
        super(SpicyCache, self).close()
        self.stub.close()
