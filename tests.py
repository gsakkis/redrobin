from contextlib import contextmanager
from itertools import cycle, islice
import time
import unittest

import mock
import redis
import redrobin


MOCK_TIME = True
TIME_DELTA = 1e-3


class MockTime(object):

    @classmethod
    def patch(cls, now=0, tick=TIME_DELTA):
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


class RedisTestCase(unittest.TestCase):

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


class RedRobinTestCase(RedisTestCase):

    def RoundRobin(self, throttled_items=None, default_throttle=0.0):
        rr = redrobin.RoundRobin(default_throttle=default_throttle, name='test',
                                 connection=self.test_conn)
        if throttled_items:
            rr.update_many(throttled_items)
        return rr

    def assertAlmostEqualTime(self, t1, t2):
        # give an order of magnitude slack compared to TIME_DELTA
        self.assertAlmostEqual(t1, t2, delta=10 * TIME_DELTA)

    @contextmanager
    def assertAlmostEqualDuration(self, duration):
        start = time.time()
        try:
            yield
        finally:
            end = time.time()
            self.assertAlmostEqualTime(duration, end - start)

    def assertQueuesThrottles(self, round_robin, expected_queues, expected_throttled_items):
        queue = self.test_conn.zrange(round_robin._items_key, 0, -1)
        throttled_items = self.test_conn.hgetall(round_robin._throttles_key)
        for item, throttle in throttled_items.iteritems():
            throttled_items[item] = float(throttle)
        self.assertEqual(set(queue), set(throttled_items))
        self.assertEqual(queue, expected_queues)
        self.assertEqual(throttled_items, expected_throttled_items)

    def test_init(self):
        rr = self.RoundRobin()
        self.assertQueuesThrottles(rr, [], {})
        self.assertRaises(ValueError, self.RoundRobin, default_throttle=None)

    def test_update_one(self):
        rr = self.RoundRobin()
        rr.update_one('foo', 5)
        self.assertQueuesThrottles(rr, ['foo'], {'foo': 5})

        rr.update_one('bar', 4)
        self.assertQueuesThrottles(rr, ['foo', 'bar'], {'foo': 5, 'bar': 4})

        # neither queue nor throttle of foo is updated
        rr.update_one('foo')
        self.assertQueuesThrottles(rr, ['foo', 'bar'], {'foo': 5, 'bar': 4})

        # queue not updated but throttle of foo is
        rr.update_one('foo', 3)
        self.assertQueuesThrottles(rr, ['foo', 'bar'], {'foo': 3, 'bar': 4})

        # new item with default throttle(=0) added
        rr.update_one('xyz')
        self.assertQueuesThrottles(rr, ['foo', 'bar', 'xyz'], {'foo': 3, 'bar': 4, 'xyz': 0})

    def test_update_many(self):
        rr = self.RoundRobin()
        rr.update_many(dict.fromkeys(['foo', 'bar'], 5))
        self.assertQueuesThrottles(rr, ['bar', 'foo'], {'foo': 5, 'bar': 5})

        # baz and xyz are pushed, foo updates its throttle but stays in the same position
        rr.update_many(dict.fromkeys(['baz', 'foo', 'xyz'], 2))
        self.assertQueuesThrottles(rr, ['bar', 'foo', 'baz', 'xyz'],
                                   {'foo': 2, 'bar': 5, 'baz': 2, 'xyz': 2})

        rr.update_many({'uvw': 3, 'foo': 4})
        self.assertQueuesThrottles(rr, ['bar', 'foo', 'baz', 'xyz', 'uvw'],
                                   {'foo': 4, 'bar': 5, 'baz': 2, 'xyz': 2, 'uvw': 3})

        # default throttle(=0) used only when adding new items, not updating existing ones
        rr.update_many(['uvw', 'qa', 'bar'])
        self.assertQueuesThrottles(rr, ['bar', 'foo', 'baz', 'xyz', 'uvw', 'qa'],
                                   {'foo': 4, 'bar': 5, 'baz': 2, 'xyz': 2, 'uvw': 3, 'qa': 0})

    def test_remove_existing(self):
        rr = self.RoundRobin({'foo': 3, 'bar': 4, 'baz': 2})
        rr.remove('foo')
        self.assertQueuesThrottles(rr, ['bar', 'baz'], {'bar': 4, 'baz': 2})

    def test_remove_missing(self):
        rr = self.RoundRobin({'foo': 3, 'bar': 4, 'baz': 2})
        rr.remove('xyz')
        self.assertQueuesThrottles(rr, ['bar', 'baz', 'foo'], {'foo': 3, 'bar': 4, 'baz': 2})

    def test_remove_multiple(self):
        rr = self.RoundRobin({'foo': 3, 'bar': 4, 'baz': 2})
        rr.remove('baz', 'xyz', 'bar')
        self.assertQueuesThrottles(rr, ['foo'], {'foo': 3})

    def test_clear(self):
        rr = self.RoundRobin({'foo': 3, 'bar': 4, 'baz': 2})
        rr.clear()
        self.assertQueuesThrottles(rr, [], {})

    def test_next_unthrottled(self):
        rr = self.RoundRobin(['foo', 'bar', 'baz'])
        for item in islice(cycle(['bar', 'baz', 'foo']), 100):
            self.assertEqual(rr.next(), item)

    @MockTime.patch()
    def test_next_throttled(self):
        throttle = 1
        rr = self.RoundRobin(['foo', 'bar', 'baz'], default_throttle=throttle)

        # unthrottled
        first_throttled_until = None
        for item in 'bar', 'baz', 'foo':
            with self.assertAlmostEqualDuration(TIME_DELTA):
                self.assertEqual(rr.next(), item)
                if first_throttled_until is None:
                    first_throttled_until = time.time() + throttle

        # throttled
        with self.assertAlmostEqualDuration(first_throttled_until - time.time()):
            self.assertEqual(rr.next(), 'bar')

        # unthrottled
        for item in 'baz', 'foo':
            with self.assertAlmostEqualDuration(TIME_DELTA):
                self.assertEqual(rr.next(), item)

    @MockTime.patch()
    def test_next_throttled_no_wait(self):
        throttle = 1
        rr = self.RoundRobin(['foo', 'bar', 'baz'], default_throttle=throttle)

        # unthrottled
        for item in 'bar', 'baz', 'foo':
            with self.assertAlmostEqualDuration(TIME_DELTA):
                self.assertEqual(rr.next(wait=False), item)

        # throttled
        for _ in xrange(10):
            with self.assertAlmostEqualDuration(TIME_DELTA):
                self.assertIsNone(rr.next(wait=False))

        time.sleep(throttle)
        # unthrottled
        for item in 'bar', 'baz', 'foo':
            with self.assertAlmostEqualDuration(TIME_DELTA):
                self.assertEqual(rr.next(wait=False), item)

    @MockTime.patch()
    def test_is_throttled(self):
        throttle = 1
        items = ['foo', 'bar', 'baz']
        rr = self.RoundRobin(items, default_throttle=throttle)

        # unthrottled
        for _ in items:
            self.assertFalse(rr.is_throttled())
            rr.next()

        # throttled
        for _ in xrange(10):
            self.assertTrue(rr.is_throttled())

        time.sleep(throttle)
        # unthrottled
        for _ in items:
            self.assertFalse(rr.is_throttled())
            rr.next()

    @MockTime.patch()
    def test_throttled_until(self):
        throttle = 1
        items = ['foo', 'bar', 'baz']
        rr = self.RoundRobin(items, default_throttle=throttle)

        # unthrottled
        first_throttled_until = None
        for _ in items:
            self.assertIsNone(rr.throttled_until())
            rr.next()
            if first_throttled_until is None:
                first_throttled_until = time.time() + throttle

        # throttled
        for _ in xrange(10):
            self.assertAlmostEqualTime(rr.throttled_until(), first_throttled_until)

        time.sleep(throttle)
        # unthrottled
        for _ in items:
            self.assertIsNone(rr.throttled_until())
            rr.next()

    @MockTime.patch()
    def test_iter(self):
        rr = self.RoundRobin(['foo', 'bar', 'baz'], default_throttle=1)
        self.assertEqual(list(islice(rr, 5)), ['bar', 'baz', 'foo', 'bar', 'baz'])

    def test_empty_next_iter(self):
        rr = self.RoundRobin()
        self.assertRaises(StopIteration, rr.next)
        self.assertEqual(list(rr), [])
