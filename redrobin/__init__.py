# monkeypatch StrictRedis with a zaddnx method
import redis
from .zaddnx import zaddnx
redis.StrictRedis.zaddnx = zaddnx

from .multithrottling import MultiThrottleBalancer
