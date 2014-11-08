from time import time
from collections import Mapping

import redis


class RoundRobin(object):

    # sorted set of items sorte by availability time
    redis_items_format = 'redrobin:items:{name}'
    # hash of {item: throttle}
    redis_throttles_format = 'redrobin:throttles:{name}'

    def __init__(self, connection=None, default_throttle=0, name='default'):
        if default_throttle < 0:
            raise ValueError("default_throttle must be a positive number")
        if connection is None:
            connection = redis.StrictRedis()
        self._connection = connection
        self._default_throttle = default_throttle
        self._items_key = self.redis_items_format.format(name=name)
        self._throttles_key = self.redis_throttles_format.format(name=name)

    def update_one(self, item, throttle=None):
        if throttle is not None:
            # add or update item throttle
            added = self._connection.hset(self._throttles_key, item, throttle)
            # only insert in the queue, don't update the deadline
            if added:
                self._connection.zadd(self._items_key, time() + throttle, item)
        elif not self._connection.hexists(self._throttles_key, item):
            # new item: add throttle (or default) and push to the queue
            throttle = self._default_throttle
            self._connection.hset(self._throttles_key, item, throttle)
            self._connection.zadd(self._items_key, time() + throttle, item)
        # else item exists and throttle is None; no-op

    def update_many(self, throttled_items):
        if not isinstance(throttled_items, Mapping):
            throttled_items = dict.fromkeys(throttled_items)
        # split items into new and existing
        throttled_to_add, throttled_to_update = {}, {}
        current_items = set(self._connection.hkeys(self._throttles_key))
        for item, throttle in throttled_items.iteritems():
            if item not in current_items:
                throttled_to_add[item] = throttle if throttle is not None else self._default_throttle
            elif throttle is not None:  # don't update unless explicit throttle is passed
                throttled_to_update[item] = throttle
        # push the new items to the queue
        if throttled_to_add:
            now = time()
            self._connection.zadd(self._items_key,
                                  **{item: now + throttle
                                   for item, throttle in throttled_to_add.iteritems()})
        # update the throttles of both existing and new items
        throttled_to_update.update(throttled_to_add)
        if throttled_to_update:
            self._connection.hmset(self._throttles_key, throttled_to_update)

    def remove(self, *items):
        self._connection.hdel(self._throttles_key, *items)
        self._connection.zrem(self._items_key, *items)

    def clear(self):
        self._connection.delete(self._throttles_key, self._items_key)

    def next(self):
        raise NotImplementedError

    def __iter__(self):
        return self
