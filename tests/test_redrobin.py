from contextlib import contextmanager
from itertools import cycle, islice
import time
import redrobin

from . import RedisTestCase, MockTime, TIME_DELTA


class RedRobinTestCase(RedisTestCase):

    def RoundRobin(self, throttled_items=None, default_throttle=0.0, name='test'):
        rr = redrobin.RoundRobin(default_throttle=default_throttle, name=name,
                                 connection=self.test_conn)
        if throttled_items:
            rr.update(throttled_items)
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
        self.assertItemsEqual(queue, throttled_items.keys())
        self.assertEqual(queue, expected_queues)
        self.assertEqual(throttled_items, expected_throttled_items)

    def test_init(self):
        rr = self.RoundRobin()
        self.assertQueuesThrottles(rr, [], {})
        self.assertRaises(ValueError, self.RoundRobin, default_throttle=None)

    def test_add(self):
        rr = self.RoundRobin()
        rr.add('foo', 5)
        self.assertQueuesThrottles(rr, ['foo'], {'foo': 5})

        rr.add('bar', 4)
        self.assertQueuesThrottles(rr, ['foo', 'bar'], {'foo': 5, 'bar': 4})

        # neither queue nor throttle of foo is updated
        rr.add('foo')
        self.assertQueuesThrottles(rr, ['foo', 'bar'], {'foo': 5, 'bar': 4})

        # queue not updated but throttle of foo is
        rr.add('foo', 3)
        self.assertQueuesThrottles(rr, ['foo', 'bar'], {'foo': 3, 'bar': 4})

        # new item with default throttle(=0) added
        rr.add('xyz')
        self.assertQueuesThrottles(rr, ['foo', 'bar', 'xyz'], {'foo': 3, 'bar': 4, 'xyz': 0})

    def test_update(self):
        rr = self.RoundRobin()
        rr.update(dict.fromkeys(['foo', 'bar'], 5))
        self.assertQueuesThrottles(rr, ['bar', 'foo'], {'foo': 5, 'bar': 5})

        # baz and xyz are pushed, foo updates its throttle but stays in the same position
        rr.update(dict.fromkeys(['baz', 'foo', 'xyz'], 2))
        self.assertQueuesThrottles(rr, ['bar', 'foo', 'baz', 'xyz'],
                                   {'foo': 2, 'bar': 5, 'baz': 2, 'xyz': 2})

        rr.update({'uvw': 3, 'foo': 4})
        self.assertQueuesThrottles(rr, ['bar', 'foo', 'baz', 'xyz', 'uvw'],
                                   {'foo': 4, 'bar': 5, 'baz': 2, 'xyz': 2, 'uvw': 3})

        # default throttle(=0) used only when adding new items, not updating existing ones
        rr.update(['uvw', 'qa', 'bar'])
        self.assertQueuesThrottles(rr, ['bar', 'foo', 'baz', 'xyz', 'uvw', 'qa'],
                                   {'foo': 4, 'bar': 5, 'baz': 2, 'xyz': 2, 'uvw': 3, 'qa': 0})

    def test_update_throttles(self):
        rr = self.RoundRobin({'foo': 3, 'bar': 4, 'baz': 2})
        rr.update_throttles(2.5)
        self.assertEqual(rr.item_throttles(), {'foo': 2.5, 'bar': 2.5, 'baz': 2.5})
        self.assertEqual(rr.default_throttle, 2.5)

        rr.update_throttles(1.5, set_default=False)
        self.assertEqual(rr.item_throttles(), {'foo': 1.5, 'bar': 1.5, 'baz': 1.5})
        self.assertEqual(rr.default_throttle, 2.5)

        self.assertRaises(ValueError, rr.update_throttles, None)

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

    def test_items(self):
        rr = self.RoundRobin(['a', 'b', 'c'], name='no_throttle')
        self.assertItemsEqual(rr.items(), ['a', 'b', 'c'])

        rr = self.RoundRobin(['p', 'q', 'r'], name='same_throttle', default_throttle=1)
        self.assertItemsEqual(rr.items(), ['p', 'q', 'r'])

        rr = self.RoundRobin({'x': 3, 'y': 4, 'z': 2}, name='diff_throttles')
        self.assertItemsEqual(rr.items(), ['x', 'y', 'z'])

    def test_item_throttles(self):
        rr = self.RoundRobin(['a', 'b', 'c'], name='no_throttle')
        self.assertEqual(rr.item_throttles(), {'a': 0, 'b': 0, 'c': 0})

        rr = self.RoundRobin(['p', 'q', 'r'], name='same_throttle', default_throttle=1)
        self.assertEqual(rr.item_throttles(), {'p': 1, 'q': 1, 'r': 1})

        rr = self.RoundRobin({'x': 3, 'y': 4, 'z': 2}, name='diff_throttles')
        self.assertEqual(rr.item_throttles(), {'x': 3, 'y': 4, 'z': 2})

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
