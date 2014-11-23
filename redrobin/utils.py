import functools
import numbers

from redis import RedisError, WatchError
from redis.client import Script
from redis._compat import iteritems


def transactional(*watches, **trans_kwargs):
    shard_hint = trans_kwargs.pop('shard_hint', None)
    value_from_callable = trans_kwargs.pop('value_from_callable', False)
    def decorator(func):
        @functools.wraps(func)
        def wrapper(redis, *args, **kwargs):
            with redis.pipeline(True, shard_hint) as pipe:
                while 1:
                    try:
                        if watches:
                            pipe.watch(*watches)
                        func_value = func(pipe, *args, **kwargs)
                        exec_value = pipe.execute()
                        return func_value if value_from_callable else exec_value
                    except WatchError:
                        continue
        return wrapper
    return decorator


class RedisMixin:

    def zaddnx(self, name, *args, **kwargs):
        """Like zadd but don't update the score of existing elements,

        Set any number of score, element-name pairs to the key ``name``. Pairs
        can be specified in two ways:

        As *args, in the form of: score1, name1, score2, name2, ...
        or as **kwargs, in the form of: name1=score1, name2=score2, ...

        The following example would add four values to the 'my-key' key:
        redis.zaddnx('my-key', 1.1, 'name1', 2.2, 'name2', name3=3.3, name4=4.4)
        """
        pieces = []
        if args:
            if len(args) % 2 != 0:
                raise RedisError("ZADDNX requires an equal number of "
                                 "values and scores")
            pieces.extend(args)
        for pair in iteritems(kwargs):
            pieces.append(pair[1])
            pieces.append(pair[0])

        return ZADDNX(keys=[name], args=pieces, client=self)

    def lismember(self, name, value):
        """Return a boolean indicating if ``value`` is a member of list ``name``"""
        return bool(LISMEMBER(keys=[name], args=[value], client=self))


ZADDNX = Script(None, """
local missing = {}
for i = 1, #ARGV, 2 do
    local item = ARGV[i+1]
    if redis.call("ZSCORE", KEYS[1], item) == false then
        local score = ARGV[i]
        table.insert(missing, score)
        table.insert(missing, item)
    end
end

if #missing == 0 then
    return 0
end

return redis.call("ZADD", KEYS[1], unpack(missing))
""")

LISMEMBER = Script(None, """
local value = ARGV[1]
local items = redis.call("LRANGE", KEYS[1], 0, -1)
for i,item in ipairs(items) do
    if item == value then
        return i
    end
end
return 0
""")


def validate_throttle(throttle):
    if not (isinstance(throttle, numbers.Number) and throttle > 0):
        raise ValueError("throttle must be a positive number ({!r} given)"
                         .format(throttle))
