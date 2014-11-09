from time import time
from collections import Mapping

import redis


class RoundRobin(object):

    # sorted set of items sorte by availability time
    redis_items_key_format = 'redrobin:items:{name}'
    # hash of {item: throttle}
    redis_throttles_key_format = 'redrobin:throttles:{name}'

    def __init__(self, connection=None, default_throttle=0, name='default'):
        if default_throttle < 0:
            raise ValueError("default_throttle must be a positive number")
        if connection is None:
            connection = redis.StrictRedis()
        self._connection = connection
        self._default_throttle = default_throttle
        self._items_key = self.redis_items_key_format.format(name=name)
        self._throttles_key = self.redis_throttles_key_format.format(name=name)

    def update_one(self, item, throttle=None):
        def update(pipe, throttle=throttle):
            item_exists = pipe.hexists(self._throttles_key, item)
            if throttle is not None or not item_exists:
                if throttle is None:
                    throttle = self._default_throttle
                pipe.multi()
                pipe.hset(self._throttles_key, item, throttle)
                # don't update the current deadline of existing items
                if not item_exists:
                    pipe.zadd(self._items_key, time(), item)
        self._connection.transaction(update, self._throttles_key)

    def update_many(self, throttled_items):
        if not isinstance(throttled_items, Mapping):
            throttled_items = dict.fromkeys(throttled_items)

        def update(pipe):
            # split items into new and existing
            throttled_to_add, throttled_to_update = {}, {}
            current_items = set(pipe.hkeys(self._throttles_key))
            for item, throttle in throttled_items.iteritems():
                if item not in current_items:
                    throttled_to_add[item] = throttle if throttle is not None else self._default_throttle
                elif throttle is not None:  # don't update unless explicit throttle is passed
                    throttled_to_update[item] = throttle

            # update the throttles of both existing and new items
            throttled_to_update.update(throttled_to_add)
            if throttled_to_update:
                pipe.multi()
                pipe.hmset(self._throttles_key, throttled_to_update)
                # don't update the current deadline of existing items
                if throttled_to_add:
                    now = time()
                    items = {item: now for item in throttled_to_add.iterkeys()}
                    pipe.zadd(self._items_key, **items)

        self._connection.transaction(update, self._throttles_key)

    def remove(self, *items):
        pipe = self._connection.pipeline()
        pipe.hdel(self._throttles_key, *items)
        pipe.zrem(self._items_key, *items)
        pipe.execute()

    def clear(self):
        self._connection.delete(self._throttles_key, self._items_key)

    def next(self):
        raise NotImplementedError

    def __iter__(self):
        return self
