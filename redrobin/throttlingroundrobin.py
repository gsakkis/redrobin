import itertools as it
import logging
import time

from . import RoundRobinScheduler
from .utils import validate_throttle

logger = logging.getLogger(__name__)


class ThrottlingRoundRobinScheduler(RoundRobinScheduler):

    # queue of (item, throttled_until) pairs for unclaimed items. Elements are
    # pushed to the left and popped from the right so the rightmost element is
    # the earliest available
    redis_queue_format = 'redrobin:{name}:unclaimed_items'

    # queue with same format as above for claimed throttled items waiting for the
    # throttle period to expire until they are released.
    redis_claimed_queue_format = 'redrobin:{name}:claimed_items'

    def __init__(self, throttle, keys=None, connection=None, name='default'):
        self._throttle = None
        self.throttle = throttle
        self.claimed_key = self.redis_claimed_queue_format.format(name=name)
        super(ThrottlingRoundRobinScheduler, self).__init__(keys=keys, name=name,
                                                            connection=connection)

    @property
    def throttle(self):
        return self._throttle

    @throttle.setter
    def throttle(self, value):
        validate_throttle(value)
        self._throttle = value

    def __len__(self):
        # get the length of both queues atomically
        with self.redis.pipeline() as pipe:
            pipe.llen(self.key)
            pipe.llen(self.claimed_key)
            return sum(pipe.execute())

    def __contains__(self, item):
        return any(it == item for it in self._data())

    def discard(self, item, count=0):
        # negate count because list is stored in reverse
        count = -count

        def discard_trans(pipe):
            pickled_throttled_items = pipe.lrange(self.key, 0, -1)
            indexes = [i for i, pickled in enumerate(pickled_throttled_items)
                       if self._unpickle(pickled)[0] == item]
            if not indexes:
                return

            if count > 0:
                del indexes[count:]
            elif count < 0:
                del indexes[:count]

            pipe.multi()
            for i in indexes:
                pipe.lrem(self.key, 1, pickled_throttled_items[i])

        return sum(self.redis.transaction(discard_trans, self.key))

    def pop(self):
        return super(ThrottlingRoundRobinScheduler, self).pop()[0]

    def throttled_until(self):
        # get the last (i.e. earliest available) item
        throttled_items = self.redis.lrange(self.key, -1, -1)
        if throttled_items:
            throttled_until = self._unpickle(throttled_items[0])[1]
            if time.time() < throttled_until:
                return throttled_until

    def next(self, wait=True):
        if wait:
            return self._next_wait()
        else:
            return self._next_nowait()

    def _next_wait(self):
        # don't wait if there are no items
        if not len(self):
            raise StopIteration

        # pop the last (i.e. earliest available) item and push it to the
        # claimed queue
        pickled_throttled_item = self.redis.brpoplpush(self.key, self.claimed_key)
        item, throttled_until = self._unpickle(pickled_throttled_item)

        # if it's throttled sleep until it becomes unthrottled again
        now = time.time()
        wait_time = throttled_until - now
        if wait_time > 0:
            logger.debug("Waiting %s for %.2fs", item, wait_time)
            time.sleep(wait_time)

        # put the item back with the new timestamp
        with self.redis.pipeline() as pipe:
            pipe.lrem(self.claimed_key, -1, pickled_throttled_item)
            pipe.lpush(self.key, self._pickle(item, throttle=True))
            pipe.execute()

        return item

    def _next_nowait(self):
        def next_trans(pipe):
            # check the last (i.e. earliest available) item
            throttled_items = pipe.lrange(self.key, -1, -1)
            if not throttled_items:
                raise StopIteration
            item, throttled_until = self._unpickle(throttled_items[0])

            # if it's throttled return
            now = time.time()
            wait_time = throttled_until - now
            if wait_time > 0:
                logger.debug("Not waiting %s for %.2fs", item, wait_time)
                return

            # otherwise put the item back with the new timestamp
            pipe.multi()
            pipe.rpop(self.key)
            pipe.lpush(self.key, self._pickle(item, throttle=True))
            return item

        return self.redis.transaction(next_trans, self.key, value_from_callable=True)

    def _data(self, pipe=None):
        # get the items from both queues atomically
        if pipe is None:
            with self.redis.pipeline() as pipe:
                pipe.lrange(self.claimed_key, 0, -1)
                pipe.lrange(self.key, 0, -1)
                d1, d2 = pipe.execute()
        else:
            d1 = pipe.lrange(self.claimed_key, 0, -1)
            d2 = pipe.lrange(self.key, 0, -1)

        # reverse each queue, iterate over the claimed throttled items followed
        # by the unclaimed, unpickle them and yield the items
        return (self._unpickle(throttled_item)[0]
                for throttled_item in it.chain(reversed(d1), reversed(d2)))

    def _pickle(self, data, throttle=False):
        throttled_until = time.time()
        if throttle:
            throttled_until += self.throttle
        return super(ThrottlingRoundRobinScheduler, self)._pickle((data, throttled_until))
