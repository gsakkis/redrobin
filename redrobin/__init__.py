import numbers
import time

import redis


class MultiThrottleBalancer(object):

    # set of keys sorted by availability time
    redis_keys_format = 'redrobin:keys:{name}'
    # hash of {key: throttle}
    redis_throttles_format = 'redrobin:throttles:{name}'

    def __init__(self, connection=None, name='default'):
        if connection is None:
            connection = redis.StrictRedis()
        self._connection = connection
        self._keys_key = self.redis_keys_format.format(name=name)
        self._throttles_key = self.redis_throttles_format.format(name=name)

    def add(self, key, throttle):
        self._validate_throttle(throttle)

        def update(pipe, throttle=throttle):
            key_exists = pipe.hexists(self._throttles_key, key)
            pipe.multi()
            pipe.hset(self._throttles_key, key, throttle)
            # don't update the current deadline of existing keys
            if not key_exists:
                pipe.zadd(self._keys_key, time.time(), key)

        self._connection.transaction(update, self._throttles_key)

    def update(self, throttled_keys):
        if not throttled_keys:
            return
        for throttle in throttled_keys.itervalues():
            self._validate_throttle(throttle)

        def update(pipe):
            current_keys = set(pipe.hkeys(self._throttles_key))
            throttled_to_add = {key: throttle
                                for key, throttle in throttled_keys.iteritems()
                                if key not in current_keys}
            pipe.multi()
            pipe.hmset(self._throttles_key, throttled_keys)
            # don't update the current deadline of existing keys
            if throttled_to_add:
                now = time.time()
                keys = {key: now for key in throttled_to_add.iterkeys()}
                pipe.zadd(self._keys_key, **keys)

        self._connection.transaction(update, self._throttles_key)

    def remove(self, *keys):
        pipe = self._connection.pipeline()
        pipe.hdel(self._throttles_key, *keys)
        pipe.zrem(self._keys_key, *keys)
        pipe.execute()

    def clear(self):
        self._connection.delete(self._throttles_key, self._keys_key)

    def keys(self):
        return self._connection.hkeys(self._throttles_key)

    def key_throttles(self):
        return {key: float(throttle) for key, throttle in
                self._connection.hgetall(self._throttles_key).iteritems()}

    def throttled_until(self):
        # get the first (i.e. earliest available) key
        throttled_keys = self._connection.zrange(self._keys_key, 0, 0, withscores=True)
        if throttled_keys:
            throttled_until = throttled_keys[0][1]
            if time.time() < throttled_until:
                return throttled_until

    def next(self, wait=True):
        def get_next(pipe):
            # get the first (i.e. earliest available) key
            throttled_keys = pipe.zrange(self._keys_key, 0, 0, withscores=True)
            if not throttled_keys:
                raise StopIteration
            key, throttled_until = throttled_keys[0]
            # if it's throttled, sleep until it becomes unthrottled or return
            # if not waiting
            now = time.time()
            if now < throttled_until:
                if not wait:
                    return
                time.sleep(throttled_until - now)
            # update the key's score to the new time it will stay throttled
            throttle = float(pipe.hget(self._throttles_key, key))
            pipe.multi()
            pipe.zadd(self._keys_key, time.time() + throttle, key)
            return key

        return self._connection.transaction(get_next, self._keys_key, value_from_callable=True)

    def __iter__(self):
        return self

    @staticmethod
    def _validate_throttle(throttle):
        if not (isinstance(throttle, numbers.Number) and throttle >= 0):
            raise ValueError("throttle must be a positive number ({!r} given)"
                             .format(throttle))
