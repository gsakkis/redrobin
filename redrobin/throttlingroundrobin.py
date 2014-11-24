import logging
import time

from . import RoundRobinScheduler
from .utils import validate_throttle, transactional


logger = logging.getLogger(__name__)


class ThrottlingRoundRobinScheduler(RoundRobinScheduler):

    # queue of (item, throttled_until) pairs. Elements are pushed to the left
    # and popped from the right so the rightmost element is the earliest available
    redis_queue_format = 'redrobin:{name}:throttled_items'

    def __init__(self, throttle, keys=None, connection=None, name='default'):
        self._throttle = None
        self.throttle = throttle
        super(ThrottlingRoundRobinScheduler, self).__init__(keys=keys, name=name,
                                                            connection=connection)

    @property
    def throttle(self):
        return self._throttle

    @throttle.setter
    def throttle(self, value):
        validate_throttle(value)
        self._throttle = value

    def __contains__(self, item):
        return any(it == item for it in self._data())

    def discard(self, item, count=0):
        @transactional(self.key)
        def discard_trans(pipe, item, count):
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

        # negate count because list is stored in reverse
        return sum(discard_trans(self.redis, item, -count))

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
        @transactional(self.key, value_from_callable=True)
        def next_trans(pipe, wait):
            # check the last (i.e. earliest available) item
            throttled_items = pipe.lrange(self.key, -1, -1)
            if not throttled_items:
                raise StopIteration
            item, throttled_until = self._unpickle(throttled_items[0])

            # if it's not throttled or we're waiting, update the throttled until
            # timestamp and push it back
            wait_time = throttled_until - time.time()
            if wait_time <= 0 or wait:
                throttled_until = max(throttled_until, time.time()) + self.throttle
                pipe.multi()
                pipe.rpop(self.key)
                pipe.lpush(self.key, self._pickle(item, throttled_until))
            return item, wait_time

        item, wait_time = next_trans(self.redis, wait)
        if wait_time > 0:
            if wait:
                logger.debug("Waiting %s for %.2fs", item, wait_time)
                time.sleep(wait_time)
            else:
                logger.debug("Not waiting %s for %.2fs", item, wait_time)
                item = None

        return item

    def _data(self, pipe=None):
        return (it[0] for it in super(ThrottlingRoundRobinScheduler, self)._data(pipe))

    def _pickle(self, data, throttled_until=None):
        if throttled_until is None:
            throttled_until = time.time()
        return super(ThrottlingRoundRobinScheduler, self)._pickle((data, throttled_until))
