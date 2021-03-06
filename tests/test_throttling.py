from itertools import cycle, islice
import time
import redrobin

from . import BaseTestCase, MockTime


class ThrottlingSchedulerTestCase(BaseTestCase):

    def get_scheduler(self, throttled_keys=None, name='test'):
        return redrobin.ThrottlingScheduler(throttled_keys=throttled_keys,
                                              name=name, connection=self.test_conn)

    def assertQueueThrottles(self, round_robin, expected_queue, expected_throttled_keys):
        queue = self.test_conn.zrange(round_robin.queue_key, 0, -1)
        self.assertEqual(queue, expected_queue)
        throttled_keys = dict(round_robin.iteritems())
        self.assertItemsEqual(queue, throttled_keys.keys())
        self.assertEqual(throttled_keys, expected_throttled_keys)

    def test_init(self):
        rr = self.get_scheduler()
        self.assertQueueThrottles(rr, [], {})

        rr = self.get_scheduler({'foo': 3, 'bar': 4})
        self.assertQueueThrottles(rr, ['bar', 'foo'], {'foo': 3, 'bar': 4})

        rr = self.get_scheduler({'abc': 1, 'xyz': 2, 'foo': 4})
        self.assertQueueThrottles(rr, ['abc', 'foo', 'xyz'],
                                   {'abc': 1, 'xyz': 2, 'foo': 4})

    def test_fromkeys(self):
        rr = redrobin.ThrottlingScheduler.fromkeys([], name='test',
                                                    connection=self.test_conn)
        self.assertQueueThrottles(rr, [], {})

        rr = redrobin.ThrottlingScheduler.fromkeys(['foo', 'bar'], 1,
                                                      name='test',
                                                      connection=self.test_conn)
        self.assertQueueThrottles(rr, ['bar', 'foo'], {'foo': 1, 'bar': 1})

        rr = redrobin.ThrottlingScheduler.fromkeys(['abc', 'xyz', 'foo'], 1e-3,
                                                      name='test',
                                                      connection=self.test_conn)
        self.assertQueueThrottles(rr, ['abc', 'foo', 'xyz'],
                                   {'abc': 1e-3, 'xyz': 1e-3, 'foo': 1e-3})

    def test_setitem(self):
        rr = self.get_scheduler()
        rr['foo'] = 5
        self.assertQueueThrottles(rr, ['foo'], {'foo': 5})

        rr['bar'] = 4
        self.assertQueueThrottles(rr, ['foo', 'bar'], {'foo': 5, 'bar': 4})

        # queue not updated but throttle of foo is
        rr['foo'] = 3
        self.assertQueueThrottles(rr, ['foo', 'bar'], {'foo': 3, 'bar': 4})

        # invalid throttle
        for throttle in 0, -1, '1', None:
            with self.assertRaises(ValueError):
                rr['foo'] = throttle

    def test_setdefault(self):
        rr = self.get_scheduler()
        rr.setdefault('foo', 5)
        self.assertQueueThrottles(rr, ['foo'], {'foo': 5})

        rr.setdefault('bar', 4)
        self.assertQueueThrottles(rr, ['foo', 'bar'], {'foo': 5, 'bar': 4})

        rr.setdefault('foo', 3)
        self.assertQueueThrottles(rr, ['foo', 'bar'], {'foo': 5, 'bar': 4})

        # invalid throttle
        self.assertRaises(ValueError, rr.setdefault, 'foo')
        for throttle in 0, -1, '1':
            with self.assertRaises(ValueError):
                rr.setdefault('foo', throttle)

    def test_delitem(self):
        rr = self.get_scheduler({'foo': 3, 'bar': 4, 'baz': 2})
        del rr['foo']
        self.assertQueueThrottles(rr, ['bar', 'baz'], {'bar': 4, 'baz': 2})

        with self.assertRaises(KeyError):
            del rr['xyz']
            self.assertQueueThrottles(rr, ['bar', 'baz'], {'bar': 4, 'baz': 2})

    def test_pop(self):
        rr = self.get_scheduler({'foo': 3, 'bar': 4, 'baz': 2})
        self.assertEqual(rr.pop('foo'), 3)
        self.assertQueueThrottles(rr, ['bar', 'baz'], {'bar': 4, 'baz': 2})

        with self.assertRaises(KeyError):
            rr.pop('xyz')
            self.assertQueueThrottles(rr, ['bar', 'baz'], {'bar': 4, 'baz': 2})

        self.assertEqual(rr.pop('xyz', -1), -1)
        self.assertQueueThrottles(rr, ['bar', 'baz'], {'bar': 4, 'baz': 2})

    def test_popitem(self):
        rr = self.get_scheduler({'foo': 3, 'bar': 4, 'baz': 2})

        self.assertEqual(rr.popitem(), ('baz', 2))
        self.assertQueueThrottles(rr, ['bar', 'foo'], {'bar': 4, 'foo': 3})

        self.assertEqual(rr.popitem(), ('foo', 3))
        self.assertQueueThrottles(rr, ['bar'], {'bar': 4})

        self.assertEqual(rr.popitem(), ('bar', 4))
        self.assertQueueThrottles(rr, [], {})

        self.assertRaises(KeyError, rr.popitem)

    def test_update(self):
        rr = self.get_scheduler()
        rr.update(dict.fromkeys(['foo', 'bar'], 5))
        self.assertQueueThrottles(rr, ['bar', 'foo'], {'foo': 5, 'bar': 5})

        # baz and xyz are pushed, foo updates its throttle but stays in the same position
        rr.update(dict.fromkeys(['baz', 'foo', 'xyz'], 2))
        self.assertQueueThrottles(rr, ['bar', 'foo', 'baz', 'xyz'],
                                   {'foo': 2, 'bar': 5, 'baz': 2, 'xyz': 2})

        # update from kwargs
        rr.update(xyz=3, abc=4)
        self.assertQueueThrottles(rr, ['bar', 'foo', 'baz', 'xyz', 'abc'],
                                   {'foo': 2, 'bar': 5, 'baz': 2, 'xyz': 3, 'abc': 4})

        # update from both a dict and kwargs
        rr.update({'bar': 2, 'mno': 8}, foo=1, ghi=7)
        self.assertQueueThrottles(rr, ['bar', 'foo', 'baz', 'xyz', 'abc', 'ghi', 'mno'],
                                   {'foo': 1, 'bar': 2, 'baz': 2, 'xyz': 3,
                                    'abc': 4, 'mno': 8, 'ghi': 7})

        # invalid throttle
        for throttle in 0, -1, '1', None:
            self.assertRaises(ValueError, rr.update, {'foo': throttle})

    def test_discard(self):
        rr = self.get_scheduler({'foo': 3, 'bar': 4, 'baz': 2})
        self.assertEqual(rr.discard('foo'), 1)
        self.assertQueueThrottles(rr, ['bar', 'baz'], {'bar': 4, 'baz': 2})

        self.assertEqual(rr.discard('xyz'), 0)
        self.assertQueueThrottles(rr, ['bar', 'baz'], {'bar': 4, 'baz': 2})

        self.assertEqual(rr.discard('baz', 'xyz'), 1)
        self.assertQueueThrottles(rr, ['bar'], {'bar': 4})

    def test_clear(self):
        rr = self.get_scheduler({'foo': 3, 'bar': 4, 'baz': 2})
        rr.clear()
        self.assertQueueThrottles(rr, [], {})

    def test_keys(self):
        rr = self.get_scheduler({'x': 3, 'y': 4, 'z': 2}, name='diff_throttles')
        self.assertItemsEqual(rr.keys(), ['x', 'y', 'z'])

    def test_iter(self):
        keys = ['foo', 'bar', 'baz']
        rr = self.get_scheduler(dict.fromkeys(keys, 1))
        self.assertItemsEqual(list(rr), keys)

    def test_iteritems(self):
        rr = self.get_scheduler({'x': 3, 'y': 4, 'z': 2}, name='diff_throttles')
        self.assertEqual(dict(rr.iteritems()), {'x': 3, 'y': 4, 'z': 2})

    def test_next_empty(self):
        rr = self.get_scheduler()
        self.assertRaises(StopIteration, rr.next)

    def test_next(self):
        rr = self.get_scheduler(dict.fromkeys(['foo', 'bar', 'baz'], 1e-3))
        for key in islice(cycle(['bar', 'baz', 'foo']), 100):
            self.assertEqual(rr.next(), key)

    @MockTime.patch()
    def test_next_throttled_wait(self):
        throttle = 1
        start = time.time()
        rr = self.get_scheduler(dict.fromkeys(['foo', 'bar', 'baz'], throttle))

        # unthrottled
        first_throttled = None
        for key in 'bar', 'baz', 'foo':
            with self.assertAlmostInstant():
                self.assertEqual(rr.next(), key)
            if first_throttled is None:
                first_throttled = time.time()

        # throttled
        with self.assertTimeRange(start + throttle, first_throttled + throttle):
            self.assertEqual(rr.next(), 'bar')

        # unthrottled
        for key in 'baz', 'foo':
            with self.assertAlmostInstant():
                self.assertEqual(rr.next(), key)

    @MockTime.patch()
    def test_next_throttled_no_wait(self):
        throttle = 1
        rr = self.get_scheduler(dict.fromkeys(['foo', 'bar', 'baz'], throttle))

        # unthrottled
        for key in 'bar', 'baz', 'foo':
            with self.assertAlmostInstant():
                self.assertEqual(rr.next(wait=False), key)

        # throttled
        for _ in xrange(10):
            with self.assertAlmostInstant():
                self.assertIsNone(rr.next(wait=False))

        time.sleep(throttle)
        # unthrottled
        for key in 'bar', 'baz', 'foo':
            with self.assertAlmostInstant():
                self.assertEqual(rr.next(wait=False), key)

    @MockTime.patch()
    def test_throttled_until(self):
        throttle = 1
        keys = ['foo', 'bar', 'baz']
        start = time.time()
        rr = self.get_scheduler(dict.fromkeys(keys, throttle))

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
