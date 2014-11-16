import collections
import marshal
import time

import redis_collections
from .utils import validate_throttle


class ThrottlingBalancer(redis_collections.RedisCollection, collections.MutableSet):

    # list of keys sorted by availability time
    redis_queue_format = 'redrobin:{name}:elements'

    def __init__(self, throttle, keys=None, connection=None, name='default'):
        self._throttle = None
        self.throttle = throttle
        queue_key = self.redis_queue_format.format(name=name)
        super(ThrottlingBalancer, self).__init__(data=keys, redis=connection,
                                                 key=queue_key, pickler=marshal)

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

    def __contains__(self, elem):
        raise NotImplementedError
        #return self.redis.lm(self.key, self._pickle(elem))

    def add(self, elem):
        raise NotImplementedError
        #return bool(self.redis.sadd(self.key, self._pickle(elem)))

    def discard(self, elem):
        raise NotImplementedError
        #self.redis.srem(self.key, self._pickle(elem))

    def _data(self, pipe=None):
        pipe = pipe if pipe is not None else self.redis
        return (self._unpickle(v) for v in pipe.lrange(self.key, 0, -1))

    def _update(self, data, pipe=None):
        super(ThrottlingBalancer, self)._update(data, pipe)
        pipe = pipe if pipe is not None else self.redis
        pipe.rpush(self.key, *map(self._pickle, data))

    # def update(self, *items):
    #     if not items:
    #         return
    #     if len(items) == 1:
    #         update = partial(self._update_one, items[0])
    #     else:
    #         update = partial(self._update_many, items)
    #
    #     self.redis.transaction(update, self.key)
    #
    # def _update_one(self, item, pipe):
    #     # add it if it doesn't exist
    #     if pipe.zscore(self.key, item) is not None:
    #         pipe.multi()
    #         pipe.zadd(self.key, time.time(), item)
    #
    # def _update_many(self, items, pipe):
    #     # find and add the non existing items
    #     current_items = set(pipe.zrange(self.key, 0, -1))
    #     new_items = [item for item in items if item not in current_items]
    #     if new_items:
    #         pipe.multi()
    #         now = time.time()
    #         pipe.zadd(self.key, **{item: now for item in new_items})
    #
    # def remove(self, *items):
    #     self.redis.zrem(self.key, *items)
    #
    # def clear(self):
    #     self.redis.delete(self.key)
    #
    # def throttled_until(self):
    #     # get the first (i.e. earliest available) item
    #     throttled_items = self.redis.zrange(self.key, 0, 0, withscores=True)
    #     if throttled_items:
    #         throttled_until = throttled_items[0][1]
    #         if time.time() < throttled_until:
    #             return throttled_until
    #
    # def next(self, wait=True):
    #     return self.redis.transaction(lambda pipe: self._next(wait, pipe),
    #                                   self.key, value_from_callable=True)
    #
    # def _next(self, wait, pipe):
    #     # get the first (i.e. earliest available) item
    #     throttled_items = pipe.zrange(self.key, 0, 0, withscores=True)
    #     if not throttled_items:
    #         raise StopIteration
    #     item, throttled_until = throttled_items[0]
    #     # if it's throttled, sleep until it becomes unthrottled or return
    #     # if not waiting
    #     now = time.time()
    #     if now < throttled_until:
    #         if not wait:
    #             return
    #         time.sleep(throttled_until - now)
    #     # update the item's score to the new time it will stay throttled
    #     pipe.multi()
    #     pipe.zadd(self.key, time.time() + self.throttle, item)
    #     return item
