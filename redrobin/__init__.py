# monkeypatch StrictRedis with RedisMixin
import redis
from .utils import RedisMixin
redis.StrictRedis.__bases__ += (RedisMixin,)

from .roundrobin import RoundRobinScheduler
from .throttlingroundrobin import ThrottlingRoundRobinScheduler
from .throttling import ThrottlingScheduler
