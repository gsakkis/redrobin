# monkeypatch StrictRedis with RedisMixin
import redis
from .mixin import RedisMixin
redis.StrictRedis.__bases__ += (RedisMixin,)

from .multithrottling import MultiThrottleBalancer
