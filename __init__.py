from time import time
from collections import Mapping

import redis


class RoundRobin(object):

    redis_round_format = 'redrobin:{name}:round'

    def __init__(self, items=None, default_throttle=0, connection=None, name='default'):
        if connection is None:
            connection = redis.StrictRedis()
        self._connection = connection
        self._name = name
        self._default_throttle = default_throttle
        self._key = self.redis_round_format.format(name=self._name)
        if items:
            self.update(items)

    def add(self, item, throttle=0):
        self._connection.zadd(self._key, time() + throttle, item)

    def update(self, items):
        now = time()
        if isinstance(items, Mapping):
            items = {item: now + throttle for item, throttle in items.iteritems()}
        else:
            items = dict.fromkeys(items, now + self._default_throttle)
        self._connection.zadd(self._key, **items)

    def remove(self, *items):
        self._connection.zrem(self._key, *items)

    def clear(self):
        self._connection.delete(self._key)

    def next(self):
        raise NotImplementedError

    def __iter__(self):
        return self
