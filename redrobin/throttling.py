import json
import time

import redis_collections
from .utils import validate_throttle


class ThrottlingBalancer(redis_collections.RedisCollection):

    # list of keys sorted by availability time
    redis_queue_format = 'redrobin:{name}:throttled_items'

    def __init__(self, throttle, keys=None, connection=None, name='default'):
        self._throttle = None
        self.throttle = throttle
        queue_key = self.redis_queue_format.format(name=name)
        super(ThrottlingBalancer, self).__init__(data=keys, redis=connection,
                                                 key=queue_key, pickler=json)

    @property
    def throttle(self):
        return self._throttle

    @throttle.setter
    def throttle(self, value):
        validate_throttle(value)
        self._throttle = value

    def __len__(self):
        return self.redis.llen(self.key)

    def __iter__(self):
        return self._data()

    def __contains__(self, item):
        return any(it == item for it in self._data())

    def add(self, *items):
        self.redis.rpush(self.key, *map(self._pickle, items))

    def discard(self, item, count=0):
        def discard_trans(pipe):
            pickled_throttled_items = pipe.lrange(self.key, 0, -1)
            indexes = [i for i, pickled in enumerate(pickled_throttled_items)
                       if self._unpickle(pickled)[0] == item]
            if count > 0:
                del indexes[count:]
            elif count < 0:
                del indexes[:count]
            if indexes:
                pipe.multi()
                for i in indexes:
                    pipe.lrem(self.key, 1, pickled_throttled_items[i])

        return sum(self.redis.transaction(discard_trans, self.key))

    def remove(self, item, count=0):
        removed_count = self.discard(item, count)
        if not removed_count:
            raise KeyError(item)

    def pop(self):
        value = self.redis.lpop(self.key)
        if value is None:
            raise KeyError
        return self._unpickle(value)[0]

    # def throttled_until(self):
    #     # get the first (i.e. earliest available) item
    #     throttled_items = self.redis.zrange(self.key, 0, 0, withscores=True)
    #     if throttled_items:
    #         throttled_until = throttled_items[0][1]
    #         if time.time() < throttled_until:
    #             return throttled_until

    def next(self, wait=True):
        def next_trans(pipe):
            # get the first (i.e. earliest available) item
            throttled_items = pipe.lrange(self.key, 0, 0)
            if not throttled_items:
                raise StopIteration

            item, throttled_until = self._unpickle(throttled_items[0])
            # if it's throttled, sleep until it becomes unthrottled or return
            # if not waiting
            now = time.time()
            if now < throttled_until:
                if not wait:
                    return
                time.sleep(throttled_until - now)
            # update the item's score to the new time it will stay throttled
            pipe.multi()
            pipe.lpop(self.key)
            pipe.rpush(self.key, self._pickle(item, throttle=True))
            return item

        return self.redis.transaction(next_trans, self.key, value_from_callable=True)

    def _data(self, pipe=None):
        pipe = pipe if pipe is not None else self.redis
        return (self._unpickle(v)[0] for v in pipe.lrange(self.key, 0, -1))

    def _update(self, data, pipe=None):
        super(ThrottlingBalancer, self)._update(data, pipe)
        pipe = pipe if pipe is not None else self.redis
        pipe.rpush(self.key, *map(self._pickle, data))

    def _pickle(self, data, throttle=False):
        throttled_until = time.time()
        if throttle:
            throttled_until += self.throttle
        return super(ThrottlingBalancer, self)._pickle((data, throttled_until))
