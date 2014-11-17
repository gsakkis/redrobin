# monkeypatch StrictRedis with RedisMixin
import redis
from .utils import RedisMixin
redis.StrictRedis.__bases__ += (RedisMixin,)

from .roundrobin import RoundRobinBalancer
from .throttling import ThrottlingBalancer
from .multithrottling import MultiThrottleBalancer
