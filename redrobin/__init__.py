import marshal
import numbers
import time

import redis
import redis_collections

from .zaddnx import zaddnx

# monkeypatch StrictRedis with a zaddnx method
redis.StrictRedis.zaddnx = zaddnx


class MultiThrottleBalancer(redis_collections.Dict):

    # set of keys sorted by availability time
    redis_keys_format = 'redrobin:keys:{name}'
    # hash of {key: throttle}
    redis_throttles_format = 'redrobin:throttles:{name}'

    def __init__(self, throttled_keys=None, connection=None, name='default'):
        self.queue_key = self.redis_keys_format.format(name=name)
        throttles_key = self.redis_throttles_format.format(name=name)
        if throttled_keys:
            for throttle in throttled_keys.itervalues():
                self._validate_throttle(throttle)
        super(MultiThrottleBalancer, self).__init__(data=throttled_keys,
                                                    redis=connection,
                                                    key=throttles_key,
                                                    pickler=marshal)

    def __setitem__(self, key, throttle):
        self._validate_throttle(throttle)
        with self.redis.pipeline() as pipe:
            pipe.hset(self.key, key, self._pickle(throttle))
            # don't update the deadline if the key exists
            pipe.zaddnx(self.queue_key, time.time(), key)
            pipe.execute()

    def setdefault(self, key, throttle=None):
        self._validate_throttle(throttle)
        with self.redis.pipeline() as pipe:
            pipe.hsetnx(self.key, key, self._pickle(throttle))
            pipe.zaddnx(self.queue_key, time.time(), key)
            pipe.hget(self.key, key)
            _, _, value = pipe.execute()
            return self._unpickle(value)

    def update(self, *args, **kwargs):
        throttled_keys = dict(*args, **kwargs)
        if throttled_keys:
            for throttle in throttled_keys.itervalues():
                self._validate_throttle(throttle)
            with self.redis.pipeline() as pipe:
                self._update(throttled_keys, pipe)
                pipe.execute()

    def __delitem__(self, key):
        with self.redis.pipeline() as pipe:
            pipe.hexists(self.key, key)
            pipe.hdel(self.key, key)
            pipe.zrem(self.queue_key, key)
            exists, _, _ = pipe.execute()
            if not exists:
                raise KeyError(key)

    def pop(self, key, default=redis_collections.Dict._Dict__marker):
        with self.redis.pipeline() as pipe:
            pipe.hget(self.key, key)
            pipe.hdel(self.key, key)
            pipe.zrem(self.queue_key, key)
            value, existed, _ = pipe.execute()
            if not existed:
                if default is redis_collections.Dict._Dict__marker:
                    raise KeyError(key)
                return default
            return self._unpickle(value)

    def popitem(self):
        def popitem_trans(pipe):
            try:
                key = pipe.hkeys(self.key)[0]
            except IndexError:
                raise KeyError
            value = pipe.hget(self.key, key)
            pipe.multi()
            pipe.hdel(self.key, key)
            pipe.zrem(self.queue_key, key)
            return key, self._unpickle(value)

        return self.redis.transaction(popitem_trans, self.key, value_from_callable=True)

    # TODO
    # @classmethod
    # def fromkeys(cls, seq, value=None, **kwargs):

    def discard(self, *keys):
        with self.redis.pipeline() as pipe:
            pipe.hdel(self.key, *keys)
            pipe.zrem(self.queue_key, *keys)
            pipe.execute()

    def throttled_until(self):
        # get the first (i.e. earliest available) key
        throttled_keys = self.redis.zrange(self.queue_key, 0, 0, withscores=True)
        if throttled_keys:
            throttled_until = throttled_keys[0][1]
            if time.time() < throttled_until:
                return throttled_until

    def next(self, wait=True):
        def next_trans(pipe):
            # get the first (i.e. earliest available) key
            throttled_keys = pipe.zrange(self.queue_key, 0, 0, withscores=True)
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
            throttle = self._unpickle(pipe.hget(self.key, key))
            pipe.multi()
            pipe.zadd(self.queue_key, time.time() + throttle, key)
            return key

        return self.redis.transaction(next_trans, self.queue_key, value_from_callable=True)

    def _clear(self, pipe=None):
        pipe = pipe if pipe is not None else self.redis
        pipe.delete(self.key, self.queue_key)

    def _update(self, throttled_keys, pipe=None):
        pipe = pipe if pipe is not None else self.redis
        super(MultiThrottleBalancer, self)._update(throttled_keys, pipe)
        now = time.time()
        items = {key: now for key in throttled_keys.iterkeys()}
        # don't update the deadlines of existing keys
        pipe.zaddnx(self.queue_key, **items)

    @staticmethod
    def _validate_throttle(throttle):
        if not (isinstance(throttle, numbers.Number) and throttle >= 0):
            raise ValueError("throttle must be a positive number ({!r} given)"
                             .format(throttle))
