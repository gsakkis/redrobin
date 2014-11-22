import logging
import time

from . import RoundRobinScheduler
from .utils import validate_throttle

logger = logging.getLogger(__name__)


class ThrottlingRoundRobinScheduler(RoundRobinScheduler):

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
        def next_trans(pipe):
            # get the last (i.e. earliest available) item
            throttled_items = pipe.lrange(self.key, -1, -1)
            if not throttled_items:
                raise StopIteration

            item, throttled_until = self._unpickle(throttled_items[0])
            # if it's throttled, sleep until it becomes unthrottled or return
            # if not waiting
            now = time.time()
            wait_time = throttled_until - now
            if wait_time > 0:
                if not wait:
                    return
                logger.debug("Waiting %s for %.2fs", item, wait_time)
                time.sleep(wait_time)
            # update the item's score to the new time it will stay throttled
            pipe.multi()
            pipe.rpop(self.key)
            pipe.lpush(self.key, self._pickle(item, throttle=True))
            return item

        return self.redis.transaction(next_trans, self.key, value_from_callable=True)

    def _data(self, pipe=None):
        return (it[0] for it in super(ThrottlingRoundRobinScheduler, self)._data(pipe))

    def _pickle(self, data, throttle=False):
        throttled_until = time.time()
        if throttle:
            throttled_until += self.throttle
        return super(ThrottlingRoundRobinScheduler, self)._pickle((data, throttled_until))
