# monkeypatch StrictRedis with RedisMixin
import redis
from .mixin import RedisMixin
redis.StrictRedis.__bases__ += (RedisMixin,)

from .throttling import ThrottlingBalancer
from .multithrottling import MultiThrottleBalancer
