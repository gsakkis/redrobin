from collections import Mapping
import time

import redis


class RoundRobin(object):

    # sorted set of items sorte by availability time
    redis_items_key_format = 'redrobin:items:{name}'
    # hash of {item: throttle}
    redis_throttles_key_format = 'redrobin:throttles:{name}'

    def __init__(self, connection=None, default_throttle=0.0, name='default'):
        if default_throttle < 0:
            raise ValueError("default_throttle must be a positive number")
        if connection is None:
            connection = redis.StrictRedis()
        self._connection = connection
        self.default_throttle = default_throttle
        self._items_key = self.redis_items_key_format.format(name=name)
        self._throttles_key = self.redis_throttles_key_format.format(name=name)

    def update_one(self, item, throttle=None):
        def update(pipe, throttle=throttle):
            item_exists = pipe.hexists(self._throttles_key, item)
            if throttle is not None or not item_exists:
                if throttle is None:
                    throttle = self.default_throttle
                pipe.multi()
                pipe.hset(self._throttles_key, item, throttle)
                # don't update the current deadline of existing items
                if not item_exists:
                    pipe.zadd(self._items_key, time.time(), item)
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
                    throttled_to_add[item] = throttle if throttle is not None else self.default_throttle
                elif throttle is not None:  # don't update unless explicit throttle is passed
                    throttled_to_update[item] = throttle

            # update the throttles of both existing and new items
            throttled_to_update.update(throttled_to_add)
            if throttled_to_update:
                pipe.multi()
                pipe.hmset(self._throttles_key, throttled_to_update)
                # don't update the current deadline of existing items
                if throttled_to_add:
                    now = time.time()
                    items = {item: now for item in throttled_to_add.iterkeys()}
                    pipe.zadd(self._items_key, **items)

        self._connection.transaction(update, self._throttles_key)

    def update_throttles(self, throttle, set_default=True):
        if throttle < 0:
            raise ValueError("throttle must be a positive number")
        def update(pipe):
            items = pipe.hkeys(self._throttles_key)
            pipe.multi()
            pipe.hmset(self._throttles_key, dict.fromkeys(items, throttle))
        self._connection.transaction(update, self._throttles_key)
        if set_default:
            self.default_throttle = throttle

    def remove(self, *items):
        pipe = self._connection.pipeline()
        pipe.hdel(self._throttles_key, *items)
        pipe.zrem(self._items_key, *items)
        pipe.execute()

    def clear(self):
        self._connection.delete(self._throttles_key, self._items_key)

    def items(self):
        return self._connection.hkeys(self._throttles_key)

    def item_throttles(self):
        return {item: float(throttle) for item, throttle in
                self._connection.hgetall(self._throttles_key).iteritems()}

    def is_throttled(self):
        return self.throttled_until() is not None

    def throttled_until(self):
        # get the first (i.e. earliest available) item
        throttled_items = self._connection.zrange(self._items_key, 0, 0, withscores=True)
        if throttled_items:
            throttled_until = throttled_items[0][1]
            if time.time() < throttled_until:
                return throttled_until

    def next(self, wait=True):
        def get_next(pipe):
            # get the first (i.e. earliest available) item
            throttled_items = pipe.zrange(self._items_key, 0, 0, withscores=True)
            if not throttled_items:
                raise StopIteration
            item, throttled_until = throttled_items[0]
            # if it's throttled, sleep until it becomes unthrottled or return
            # if not waiting
            now = time.time()
            if now < throttled_until:
                if not wait:
                    return
                time.sleep(throttled_until - now)
            # update the item's score to the new time it will stay throttled
            throttle = float(pipe.hget(self._throttles_key, item))
            pipe.multi()
            pipe.zadd(self._items_key, time.time() + throttle, item)
            return item

        return self._connection.transaction(get_next, self._items_key, value_from_callable=True)

    def __iter__(self):
        return self
