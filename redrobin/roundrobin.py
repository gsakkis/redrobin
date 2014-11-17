import json
import redis_collections


class RoundRobinBalancer(redis_collections.RedisCollection):

    redis_queue_format = 'redrobin:{name}:items'

    def __init__(self, keys=None, connection=None, name='default'):
        queue_key = self.redis_queue_format.format(name=name)
        super(RoundRobinBalancer, self).__init__(data=keys, redis=connection,
                                                 key=queue_key, pickler=json)

    def __len__(self):
        return self.redis.llen(self.key)

    def __iter__(self):
        return self._data()

    def __contains__(self, elem):
        return self.redis.lismember(self.key, self._pickle(elem))

    def add(self, *items):
        self.redis.rpush(self.key, *map(self._pickle, items))

    def remove(self, item, count=0):
        removed_count = self.discard(item, count)
        if not removed_count:
            raise KeyError(item)

    def discard(self, item, count=0):
        return self.redis.lrem(self.key, count, self._pickle(item))

    def pop(self):
        value = self.redis.lpop(self.key)
        if value is None:
            raise KeyError
        return self._unpickle(value)

    def next(self):
        def next_trans(pipe):
            # get the first (i.e. earliest available) item
            items = pipe.lrange(self.key, 0, 0)
            if not items:
                raise StopIteration

            item = self._unpickle(items[0])
            pipe.multi()
            pipe.lpop(self.key)
            pipe.rpush(self.key, items[0])
            return item

        return self.redis.transaction(next_trans, self.key, value_from_callable=True)

    def _data(self, pipe=None):
        pipe = pipe if pipe is not None else self.redis
        return (self._unpickle(v) for v in pipe.lrange(self.key, 0, -1))

    def _update(self, data, pipe=None):
        super(RoundRobinBalancer, self)._update(data, pipe)
        pipe = pipe if pipe is not None else self.redis
        pipe.rpush(self.key, *map(self._pickle, data))
