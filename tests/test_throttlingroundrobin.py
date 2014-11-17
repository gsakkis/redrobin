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
                 for v in self.test_conn.lrange(round_robin.key, 0, -1)]
        self.assertEqual(queue, expected_queues)

    def test_init(self):
        rr = self.get_scheduler(1)
        self.assertQueue(rr, [])

        keys = ['foo', 'bar', 'foo', 'baz']
        rr = self.get_scheduler(1, keys)
        self.assertQueue(rr, keys)

        # invalid throttle
        for throttle in -1, '1', None:
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
        rr.discard('foo')
        self.assertQueue(rr, ['bar', 'bar', 'baz', 'bar'])

        rr.discard('xyz')
        self.assertQueue(rr, ['bar', 'bar', 'baz', 'bar'])

        rr.discard('bar', count=1)
        self.assertQueue(rr, ['bar', 'baz', 'bar'])

        rr.discard('bar', count=-1)
        self.assertQueue(rr, ['bar', 'baz'])

    def test_remove(self):
        rr = self.get_scheduler(1, ['foo', 'bar', 'bar', 'foo', 'baz', 'bar'])
        rr.remove('foo')
        self.assertQueue(rr, ['bar', 'bar', 'baz', 'bar'])

        with self.assertRaises(KeyError):
            rr.remove('xyz')
            self.assertQueue(rr, ['bar', 'bar', 'baz', 'bar'])

        rr.remove('bar', count=1)
        self.assertQueue(rr, ['bar', 'baz', 'bar'])

    def test_clear(self):
        rr = self.get_scheduler(1, ['foo', 'bar', 'foo', 'baz'])
        rr.clear()
        self.assertQueue(rr, [])

    def test_get_set_throttle(self):
        rr = self.get_scheduler(1, ['foo', 'bar', 'foo', 'baz'])
        self.assertEqual(rr.throttle, 1)
        rr.throttle = 0
        self.assertEqual(rr.throttle, 0)
        # invalid throttle
        for throttle in -1, '1', None:
            with self.assertRaises(ValueError):
                rr.throttle = throttle

    def test_next_empty(self):
        rr = self.get_scheduler(1)
        self.assertRaises(StopIteration, rr.next)

    def test_next_unthrottled(self):
        keys = ['foo', 'bar', 'foo', 'baz']
        rr = self.get_scheduler(0, keys)
        for key in islice(cycle(keys), 100):
            self.assertEqual(rr.next(), key)

    @MockTime.patch()
    def test_next_throttled(self):
        throttle = 1
        keys = ['foo', 'bar', 'foo', 'baz']
        rr = self.get_scheduler(throttle, keys)

        # unthrottled
        first_throttled_until = None
        for key in keys:
            with self.assertAlmostInstant():
                self.assertEqual(rr.next(), key)
                if first_throttled_until is None:
                    first_throttled_until = time.time() + throttle

        # throttled
        with self.assertAlmostBefore(first_throttled_until):
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
        rr = self.get_scheduler(throttle, keys)

        # unthrottled
        first_throttled_until = None
        for _ in keys:
            self.assertIsNone(rr.throttled_until())
            rr.next()
            if first_throttled_until is None:
                first_throttled_until = time.time() + throttle

        # throttled
        for _ in xrange(10):
            self.assertAlmostEqual(rr.throttled_until(), first_throttled_until)

        time.sleep(throttle)
        # unthrottled
        for _ in keys:
            self.assertIsNone(rr.throttled_until())
            rr.next()
