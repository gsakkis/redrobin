from collections import Mapping
from contextlib import contextmanager
from time import time, sleep

import redis


@apply
def monkeypatch_transaction():
    # Monkeypatch StrictRedis.transaction to allow it as context manager
    #TODO: Make pull request to redis-py

    @contextmanager
    def transaction_context(self, *watches, **kwargs):
        shard_hint = kwargs.pop('shard_hint', None)
        with self.pipeline(True, shard_hint) as pipe:
            while 1:
                try:
                    if watches:
                        pipe.watch(*watches)
                    yield pipe
                    pipe.execute()
                    break
                except redis.WatchError:
                    pass

    orig_transaction = redis.StrictRedis.transaction
    def transaction(self, *args, **kwargs):
        method = orig_transaction if args and callable(args[0]) else transaction_context
        return method(self, *args, **kwargs)
    redis.StrictRedis.transaction = transaction


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
        with self._connection.transaction(self._throttles_key) as pipe:
            item_exists = pipe.hexists(self._throttles_key, item)
            if throttle is not None or not item_exists:
                if throttle is None:
                    throttle = self._default_throttle
                pipe.multi()
                pipe.hset(self._throttles_key, item, throttle)
                # don't update the current deadline of existing items
                if not item_exists:
                    pipe.zadd(self._items_key, time(), item)

    def update_many(self, throttled_items):
        if not isinstance(throttled_items, Mapping):
            throttled_items = dict.fromkeys(throttled_items)

        with self._connection.transaction(self._throttles_key) as pipe:
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

    def remove(self, *items):
        pipe = self._connection.pipeline()
        pipe.hdel(self._throttles_key, *items)
        pipe.zrem(self._items_key, *items)
        pipe.execute()

    def clear(self):
        self._connection.delete(self._throttles_key, self._items_key)

    def next(self, wait=True):
        with self._connection.transaction(self._items_key) as pipe:
            # get the first (i.e. earliest available) item
            throttled_items = pipe.zrange(self._items_key, 0, 0, withscores=True)
            if not throttled_items:
                raise StopIteration
            item, throttled_until = throttled_items[0]
            # if it's throttled, sleep until it becomes unthrottled or return
            # if not waiting
            now = time()
            if now < throttled_until:
                if not wait:
                    return
                sleep(throttled_until - now)
            # update the item's score to the new time it will stay throttled
            throttle = float(pipe.hget(self._throttles_key, item))
            pipe.multi()
            pipe.zadd(self._items_key, now + throttle, item)
            return item

    def __iter__(self):
        return self
