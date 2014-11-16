from contextlib import contextmanager
import time
import unittest

import mock
import redis


MOCK_TIME = True
TIME_TICK = 1e-3


class MockTime(object):

    @classmethod
    def patch(cls, now=0, tick=TIME_TICK):
        if not MOCK_TIME:
            return lambda f: f
        self = cls(now, tick)
        return mock.patch.multiple('time', time=self.time, sleep=self.sleep)

    def __init__(self, now, tick):
        self.now = now
        self.tick = tick

    def time(self):
        # add some minimum (virtual) delay between calls to time()
        self.sleep(self.tick)
        return self.now

    def sleep(self, n):
        self.now += n


class BaseTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        for dbnum in range(16):
            test_conn = redis.StrictRedis(db=dbnum)
            cursor, results = test_conn.scan(0)
            if cursor == 0 and not results:  # empty
                cls.test_conn = test_conn
                return
        assert False, 'No empty Redis database found to run tests in'

    @classmethod
    def tearDownClass(cls):
        cls.test_conn.flushdb()

    def setUp(self):
        self.test_conn.flushdb()

    def tearDown(self):
        self.test_conn.flushdb()

    @contextmanager
    def assertAlmostBefore(self, deadline):
        try:
            yield
        finally:
            self.assertAlmostEqual(time.time(), deadline)

    @contextmanager
    def assertAlmostInstant(self):
        start = time.time()
        try:
            yield
        finally:
            end = time.time()
            actual_duration = end - start - TIME_TICK
            self.assertAlmostEqual(actual_duration, 2 * TIME_TICK)

    def assertAlmostEqual(self, first, second, delta=2 * TIME_TICK):
        super(BaseTestCase, self).assertAlmostEqual(first, second, delta=delta)
