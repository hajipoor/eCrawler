import redis
import json


class redis_set(object):

    def __init__(self, name, namespace='set', **redis_kwargs):
        #        """The default connection parameters are: host='localhost', port=6379, db=0"""

        self.__db = redis.Redis(host='127.0.0.1', decode_responses=True, **redis_kwargs)
        self.key = '%s:%s' % (namespace, name)

    def size(self):
        """Return size of the set."""
        return self.__db.scard(self.key)


    def add(self, item):
        """Add item into the set."""
        self.__db.sadd(self.key, json.dumps(item))

    def add_items(self, items):
        """Add items into the set."""
        self.__db.sadd(self.key, *set([json.dumps(item) for item in items]))

    def get_items(self, count=100):
        """Remove and return several items from the set.
        """

        items = self.__db.spop(self.key, count=count)

        return [json.loads(item) for item in items]

