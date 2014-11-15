from contextlib import contextmanager
from itertools import cycle, islice
import time
import redrobin

from . import RedisTestCase, MockTime, TIME_DELTA


class RedRobinTestCase(RedisTestCase):

    def get_balancer(self, throttled_keys=None, name='test'):
        rr = redrobin.MultiThrottleBalancer(name=name, connection=self.test_conn)
        if throttled_keys:
            rr.update(throttled_keys)
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

    def assertQueuesThrottles(self, round_robin, expected_queues, expected_throttled_keys):
        queue = self.test_conn.zrange(round_robin.queue_key, 0, -1)
        throttled_keys = dict(round_robin.iteritems())
        self.assertItemsEqual(queue, throttled_keys.keys())
        self.assertEqual(queue, expected_queues)
        self.assertEqual(throttled_keys, expected_throttled_keys)

    def test_init(self):
        rr = self.get_balancer()
        self.assertQueuesThrottles(rr, [], {})

    def test_setitem(self):
        rr = self.get_balancer()
        rr['foo'] = 5
        self.assertQueuesThrottles(rr, ['foo'], {'foo': 5})

        rr['bar'] = 4
        self.assertQueuesThrottles(rr, ['foo', 'bar'], {'foo': 5, 'bar': 4})

        # queue not updated but throttle of foo is
        rr['foo'] = 3
        self.assertQueuesThrottles(rr, ['foo', 'bar'], {'foo': 3, 'bar': 4})

        # invalid throttle
        for throttle in -1, '1', None:
            with self.assertRaises(ValueError):
                rr['foo'] = throttle

    def test_update(self):
        rr = self.get_balancer()
        rr.update(dict.fromkeys(['foo', 'bar'], 5))
        self.assertQueuesThrottles(rr, ['bar', 'foo'], {'foo': 5, 'bar': 5})

        # baz and xyz are pushed, foo updates its throttle but stays in the same position
        rr.update(dict.fromkeys(['baz', 'foo', 'xyz'], 2))
        self.assertQueuesThrottles(rr, ['bar', 'foo', 'baz', 'xyz'],
                                   {'foo': 2, 'bar': 5, 'baz': 2, 'xyz': 2})

        # invalid throttle
        for throttle in -1, '1', None:
            self.assertRaises(ValueError, rr.update, {'foo': throttle})

    def test_discard_existing(self):
        rr = self.get_balancer({'foo': 3, 'bar': 4, 'baz': 2})
        rr.discard('foo')
        self.assertQueuesThrottles(rr, ['bar', 'baz'], {'bar': 4, 'baz': 2})

    def test_discard_missing(self):
        rr = self.get_balancer({'foo': 3, 'bar': 4, 'baz': 2})
        rr.discard('xyz')
        self.assertQueuesThrottles(rr, ['bar', 'baz', 'foo'], {'foo': 3, 'bar': 4, 'baz': 2})

    def test_discard_multiple(self):
        rr = self.get_balancer({'foo': 3, 'bar': 4, 'baz': 2})
        rr.discard('baz', 'xyz', 'bar')
        self.assertQueuesThrottles(rr, ['foo'], {'foo': 3})

    def test_clear(self):
        rr = self.get_balancer({'foo': 3, 'bar': 4, 'baz': 2})
        rr.clear()
        self.assertQueuesThrottles(rr, [], {})

    def test_keys(self):
        rr = self.get_balancer({'x': 3, 'y': 4, 'z': 2}, name='diff_throttles')
        self.assertItemsEqual(rr.keys(), ['x', 'y', 'z'])

    def test_iter(self):
        keys = ['foo', 'bar', 'baz']
        rr = self.get_balancer(dict.fromkeys(keys, 1))
        self.assertItemsEqual(list(rr), keys)

    def test_key_iteritems(self):
        rr = self.get_balancer({'x': 3, 'y': 4, 'z': 2}, name='diff_throttles')
        self.assertEqual(dict(rr.iteritems()), {'x': 3, 'y': 4, 'z': 2})

    def test_empty(self):
        rr = self.get_balancer()
        self.assertRaises(StopIteration, rr.next)
        self.assertEqual(list(rr), [])


    def test_next_unthrottled(self):
        rr = self.get_balancer(dict.fromkeys(['foo', 'bar', 'baz'], 0))
        for key in islice(cycle(['bar', 'baz', 'foo']), 100):
            self.assertEqual(rr.next(), key)

    @MockTime.patch()
    def test_next_throttled(self):
        throttle = 1
        rr = self.get_balancer(dict.fromkeys(['foo', 'bar', 'baz'], throttle))

        # unthrottled
        first_throttled_until = None
        for key in 'bar', 'baz', 'foo':
            with self.assertAlmostEqualDuration(TIME_DELTA):
                self.assertEqual(rr.next(), key)
                if first_throttled_until is None:
                    first_throttled_until = time.time() + throttle

        # throttled
        with self.assertAlmostEqualDuration(first_throttled_until - time.time()):
            self.assertEqual(rr.next(), 'bar')

        # unthrottled
        for key in 'baz', 'foo':
            with self.assertAlmostEqualDuration(TIME_DELTA):
                self.assertEqual(rr.next(), key)

    @MockTime.patch()
    def test_next_throttled_no_wait(self):
        throttle = 1
        rr = self.get_balancer(dict.fromkeys(['foo', 'bar', 'baz'], throttle))

        # unthrottled
        for key in 'bar', 'baz', 'foo':
            with self.assertAlmostEqualDuration(TIME_DELTA):
                self.assertEqual(rr.next(wait=False), key)

        # throttled
        for _ in xrange(10):
            with self.assertAlmostEqualDuration(TIME_DELTA):
                self.assertIsNone(rr.next(wait=False))

        time.sleep(throttle)
        # unthrottled
        for key in 'bar', 'baz', 'foo':
            with self.assertAlmostEqualDuration(TIME_DELTA):
                self.assertEqual(rr.next(wait=False), key)

    @MockTime.patch()
    def test_throttled_until(self):
        throttle = 1
        keys = ['foo', 'bar', 'baz']
        rr = self.get_balancer(dict.fromkeys(keys, throttle))

        # unthrottled
        first_throttled_until = None
        for _ in keys:
            self.assertIsNone(rr.throttled_until())
            rr.next()
            if first_throttled_until is None:
                first_throttled_until = time.time() + throttle

        # throttled
        for _ in xrange(10):
            self.assertAlmostEqualTime(rr.throttled_until(), first_throttled_until)

        time.sleep(throttle)
        # unthrottled
        for _ in keys:
            self.assertIsNone(rr.throttled_until())
            rr.next()
