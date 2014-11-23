import collections
import logging
import json
import time

import redis_collections
from .utils import validate_throttle

logger = logging.getLogger(__name__)


class ThrottlingScheduler(redis_collections.Dict):

    # set of keys sorted by availability time
    redis_queue_format = 'redrobin:{name}:throttled_keys'
    # hash of {key: throttle}
    redis_throttles_format = 'redrobin:{name}:throttles'

    def __init__(self, throttled_keys=None, connection=None, name='default'):
        if throttled_keys is not None:
            if not isinstance(throttled_keys, collections.Mapping):
                throttled_keys = dict(throttled_keys)
            for throttle in throttled_keys.itervalues():
                validate_throttle(throttle)
        throttles_key = self.redis_throttles_format.format(name=name)
        self.queue_key = self.redis_queue_format.format(name=name)
        super(ThrottlingScheduler, self).__init__(data=throttled_keys,
                                                  redis=connection,
                                                  key=throttles_key,
                                                  pickler=json)

    def __setitem__(self, key, throttle):
        validate_throttle(throttle)
        with self.redis.pipeline() as pipe:
            pipe.hset(self.key, key, self._pickle(throttle))
            # don't update the deadline if the key exists
            pipe.zaddnx(self.queue_key, time.time(), key)
            pipe.execute()

    def setdefault(self, key, throttle=None):
        validate_throttle(throttle)
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
                validate_throttle(throttle)
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

    def discard(self, *keys):
        with self.redis.pipeline() as pipe:
            pipe.hdel(self.key, *keys)
            pipe.zrem(self.queue_key, *keys)
            return pipe.execute()[0]

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

            # if it's not throttled or we're waiting, update the queue with the
            # throttled until timestamp
            wait_time = throttled_until - time.time()
            if wait_time <= 0 or wait:
                throttle = self._unpickle(pipe.hget(self.key, key))
                throttled_until += throttle
                pipe.multi()
                pipe.zadd(self.queue_key, throttled_until, key)
            return key, wait_time

        item, wait_time = self.redis.transaction(next_trans, self.queue_key,
                                                 value_from_callable=True)
        if wait_time > 0:
            if wait:
                logger.debug("Waiting %s for %.2fs", item, wait_time)
                time.sleep(wait_time)
            else:
                logger.debug("Not waiting %s for %.2fs", item, wait_time)
                item = None

        return item


    def _clear(self, pipe=None):
        pipe = pipe if pipe is not None else self.redis
        pipe.delete(self.key, self.queue_key)

    def _update(self, throttled_keys, pipe=None):
        pipe = pipe if pipe is not None else self.redis
        super(ThrottlingScheduler, self)._update(throttled_keys, pipe)
        now = time.time()
        items = {key: now for key in throttled_keys.iterkeys()}
        # don't update the deadlines of existing keys
        pipe.zaddnx(self.queue_key, **items)
