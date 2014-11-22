import json
import itertools as it
import redis_collections


class RoundRobinScheduler(redis_collections.RedisCollection):

    # queue is stored in reverse element order, i.e. items are added with lpush
    # and removed with rpop
    redis_queue_format = 'redrobin:{name}:items'

    def __init__(self, keys=None, connection=None, name='default'):
        queue_key = self.redis_queue_format.format(name=name)
        super(RoundRobinScheduler, self).__init__(data=keys, redis=connection,
                                                  key=queue_key, pickler=json)

    def __len__(self):
        return self.redis.llen(self.key)

    def __iter__(self):
        return self._data()

    def __contains__(self, elem):
        return self.redis.lismember(self.key, self._pickle(elem))

    def add(self, *items):
        self.redis.lpush(self.key, *map(self._pickle, items))

    def remove(self, item, count=0):
        removed_count = self.discard(item, count)
        if not removed_count:
            raise KeyError(item)
        return removed_count

    def discard(self, item, count=0):
        # negate count because list is stored in reverse
        return self.redis.lrem(self.key, -count, self._pickle(item))

    def pop(self):
        value = self.redis.rpop(self.key)
        if value is None:
            raise KeyError
        return self._unpickle(value)

    def next(self):
        item = self.redis.rpoplpush(self.key, self.key)
        if item is None:
            raise StopIteration
        return self._unpickle(item)

    def _data(self, pipe=None):
        pipe = pipe if pipe is not None else self.redis
        # reverse and unpickle list items
        return it.imap(self._unpickle, reversed(pipe.lrange(self.key, 0, -1)))

    def _update(self, data, pipe=None):
        super(RoundRobinScheduler, self)._update(data, pipe)
        pipe = pipe if pipe is not None else self.redis
        pipe.lpush(self.key, *map(self._pickle, data))
