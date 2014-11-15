import marshal
import numbers
import time

import redis_collections


class MultiThrottleBalancer(redis_collections.Dict):

    # set of keys sorted by availability time
    redis_keys_format = 'redrobin:keys:{name}'
    # hash of {key: throttle}
    redis_throttles_format = 'redrobin:throttles:{name}'

    # TODO: initial data
    def __init__(self, connection=None, name='default'):
        self.queue_key = self.redis_keys_format.format(name=name)
        throttles_key = self.redis_throttles_format.format(name=name)
        super(MultiThrottleBalancer, self).__init__(redis=connection,
                                                    key=throttles_key,
                                                    pickler=marshal)

    def __setitem__(self, key, throttle):
        self._validate_throttle(throttle)

        def setitem_trans(pipe):
            key_exists = pipe.hexists(self.key, key)
            pipe.multi()
            pipe.hset(self.key, key, self._pickle(throttle))
            # don't update the current deadline of existing keys
            if not key_exists:
                pipe.zadd(self.queue_key, time.time(), key)

        self.redis.transaction(setitem_trans, self.key)

    def update(self, *args, **kwargs):
        throttled_keys = dict(*args, **kwargs)
        if not throttled_keys:
            return

        for throttle in throttled_keys.itervalues():
            self._validate_throttle(throttle)

        def update_trans(pipe):
            current_keys = set(pipe.hkeys(self.key))
            to_add = {key: throttle
                      for key, throttle in throttled_keys.iteritems()
                      if key not in current_keys}

            pipe.multi()
            self._update(throttled_keys, pipe)

            # don't update the current deadline of existing keys
            if to_add:
                now = time.time()
                items = {key: now for key in to_add.iterkeys()}
                pipe.zadd(self.queue_key, **items)

        self.redis.transaction(update_trans, self.key)

    # TODO
    # def __delitem__(self, key):
    # def pop(self, key, default=__marker):
    #def popitem(self):
    #def setdefault(self, key, default=None):
    # @classmethod
    # def fromkeys(cls, seq, value=None, **kwargs):

    def discard(self, *keys):
        pipe = self.redis.pipeline()
        pipe.hdel(self.key, *keys)
        pipe.zrem(self.queue_key, *keys)
        pipe.execute()

    def clear(self):
        self.redis.delete(self.key, self.queue_key)

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

    @staticmethod
    def _validate_throttle(throttle):
        if not (isinstance(throttle, numbers.Number) and throttle >= 0):
            raise ValueError("throttle must be a positive number ({!r} given)"
                             .format(throttle))
