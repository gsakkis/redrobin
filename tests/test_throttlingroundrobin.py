from itertools import cycle, islice
import time
import redrobin

from . import BaseTestCase, MockTime


class ThrottlingRoundRobinSchedulerTestCase(BaseTestCase):

    def get_scheduler(self, throttle, keys=None, name='test'):
        return redrobin.ThrottlingRoundRobinScheduler(throttle, keys=keys, name=name,
                                                      connection=self.test_conn)

    def assertQueue(self, round_robin, expected_queues):
        queue = [round_robin._unpickle(v)[0]
                 # list is stored in reverse item order
                 for v in reversed(self.test_conn.lrange(round_robin.key, 0, -1))]
        self.assertEqual(queue, expected_queues)

    def test_init(self):
        rr = self.get_scheduler(1)
        self.assertQueue(rr, [])

        keys = ['foo', 'bar', 'foo', 'baz']
        rr = self.get_scheduler(1, keys)
        self.assertQueue(rr, keys)

        # invalid throttle
        for throttle in 0, -1, '1', None:
            self.assertRaises(ValueError, self.get_scheduler, throttle)

    def test_len(self):
        keys = ['foo', 'bar', 'foo', 'baz']
        rr = self.get_scheduler(1, keys)
        self.assertEqual(len(rr), 4)

    def test_iter(self):
        keys = ['foo', 'bar', 'foo', 'baz']
        rr = self.get_scheduler(1, keys)
        self.assertEqual(list(rr), keys)

    def test_contains(self):
        keys = ['foo', 'bar', 'foo', 'baz']
        rr = self.get_scheduler(1, keys)

        for key in 'foo', 'bar', 'baz':
            self.assertIn(key, rr)

        for key in 'fooz', 'barz', None:
            self.assertNotIn(key, rr)

    def test_add(self):
        rr = self.get_scheduler(1)
        rr.add('foo')
        self.assertQueue(rr, ['foo'])

        rr.add('bar')
        self.assertQueue(rr, ['foo', 'bar'])

        rr.add('foo')
        self.assertQueue(rr, ['foo', 'bar', 'foo'])

        rr.add('foo', 'baz', 'bar')
        self.assertQueue(rr, ['foo', 'bar', 'foo', 'foo', 'baz', 'bar'])

    def test_pop(self):
        rr = self.get_scheduler(1, ['foo', 'bar', 'foo', 'baz'])

        self.assertEqual(rr.pop(), 'foo')
        self.assertQueue(rr, ['bar', 'foo', 'baz'])

        self.assertEqual(rr.pop(), 'bar')
        self.assertQueue(rr, ['foo', 'baz'])

        self.assertEqual(rr.pop(), 'foo')
        self.assertQueue(rr, ['baz'])

        self.assertEqual(rr.pop(), 'baz')
        self.assertQueue(rr, [])

        self.assertRaises(KeyError, rr.pop)

    def test_discard(self):
        rr = self.get_scheduler(1, ['foo', 'bar', 'bar', 'foo', 'baz', 'bar'])
        self.assertEqual(rr.discard('foo'), 2)
        self.assertQueue(rr, ['bar', 'bar', 'baz', 'bar'])

        self.assertEqual(rr.discard('xyz'), 0)
        self.assertQueue(rr, ['bar', 'bar', 'baz', 'bar'])

        self.assertEqual(rr.discard('bar', count=1), 1)
        self.assertQueue(rr, ['bar', 'baz', 'bar'])

        self.assertEqual(rr.discard('bar', count=-1), 1)
        self.assertQueue(rr, ['bar', 'baz'])

    def test_remove(self):
        rr = self.get_scheduler(1, ['foo', 'bar', 'bar', 'foo', 'baz', 'bar'])
        self.assertEqual(rr.remove('foo'), 2)
        self.assertQueue(rr, ['bar', 'bar', 'baz', 'bar'])

        with self.assertRaises(KeyError):
            rr.remove('xyz')
            self.assertQueue(rr, ['bar', 'bar', 'baz', 'bar'])

        self.assertEqual(rr.remove('bar', count=1), 1)
        self.assertQueue(rr, ['bar', 'baz', 'bar'])

        self.assertEqual(rr.remove('bar', count=-1), 1)
        self.assertQueue(rr, ['bar', 'baz'])

    def test_clear(self):
        rr = self.get_scheduler(1, ['foo', 'bar', 'foo', 'baz'])
        rr.clear()
        self.assertQueue(rr, [])

    def test_get_set_throttle(self):
        rr = self.get_scheduler(1, ['foo', 'bar', 'foo', 'baz'])
        self.assertEqual(rr.throttle, 1)
        rr.throttle = 1e-3
        self.assertEqual(rr.throttle, 1e-3)
        # invalid throttle
        for throttle in 0, -1, '1', None:
            with self.assertRaises(ValueError):
                rr.throttle = throttle

    def test_next_empty(self):
        rr = self.get_scheduler(1)
        self.assertRaises(StopIteration, rr.next)
        self.assertRaises(StopIteration, rr.next, wait=False)

    def test_next(self):
        keys = ['foo', 'bar', 'foo', 'baz']
        rr = self.get_scheduler(1e-3, keys)
        for key in islice(cycle(keys), 100):
            self.assertEqual(rr.next(), key)

    @MockTime.patch()
    def test_next_throttled_wait(self):
        throttle = 1
        keys = ['foo', 'bar', 'foo', 'baz']
        start = time.time()
        rr = self.get_scheduler(throttle, keys)

        # unthrottled
        first_throttled = None
        for key in keys:
            with self.assertAlmostInstant():
                self.assertEqual(rr.next(), key)
            if first_throttled is None:
                first_throttled = time.time()

        # throttled
        with self.assertTimeRange(start + throttle, first_throttled + throttle):
            self.assertEqual(rr.next(), keys[0])

        # unthrottled
        for key in keys[1:]:
            with self.assertAlmostInstant():
                self.assertEqual(rr.next(), key)

    @MockTime.patch()
    def test_next_throttled_no_wait(self):
        throttle = 1
        keys = ['foo', 'bar', 'foo', 'baz']
        rr = self.get_scheduler(throttle, keys)

        # unthrottled
        for key in keys:
            with self.assertAlmostInstant():
                self.assertEqual(rr.next(wait=False), key)

        # throttled
        for _ in xrange(10):
            with self.assertAlmostInstant():
                self.assertIsNone(rr.next(wait=False))

        time.sleep(throttle)
        # unthrottled
        for key in keys:
            with self.assertAlmostInstant():
                self.assertEqual(rr.next(wait=False), key)

    @MockTime.patch()
    def test_throttled_until(self):
        throttle = 1
        keys = ['foo', 'bar', 'foo', 'baz']
        start = time.time()
        rr = self.get_scheduler(throttle, keys)

        # unthrottled
        first_throttled = None
        for _ in keys:
            self.assertIsNone(rr.throttled_until())
            rr.next()
            if first_throttled is None:
                first_throttled = time.time()

        # throttled
        for _ in xrange(10):
            throttled_until = rr.throttled_until()
            self.assertGreater(throttled_until, start + throttle)
            self.assertLessEqual(throttled_until, first_throttled + throttle)

        time.sleep(throttle)
        # unthrottled
        for _ in keys:
            self.assertIsNone(rr.throttled_until())
            rr.next()
