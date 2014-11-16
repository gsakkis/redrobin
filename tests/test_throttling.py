from itertools import cycle, islice
import time
import redrobin

from . import BaseTestCase, MockTime, TIME_DELTA


class MultiThrottleBalancerTestCase(BaseTestCase):

    def get_balancer(self, throttle, keys=None, name='test'):
        return redrobin.ThrottlingBalancer(throttle, keys=keys, name=name,
                                           connection=self.test_conn)

    def assertQueue(self, round_robin, expected_queues):
        queue = map(round_robin._unpickle,
                    self.test_conn.lrange(round_robin.key, 0, -1))
        self.assertEqual(queue, expected_queues)

    def test_init(self):
        rr = self.get_balancer(1)
        self.assertQueue(rr, [])

        keys = ['foo', 'bar', 'foo', 'baz']
        rr = self.get_balancer(1, keys)
        self.assertQueue(rr, keys)

        # invalid throttle
        for throttle in -1, '1', None:
            self.assertRaises(ValueError, self.get_balancer, throttle)

    def test_len(self):
        keys = ['foo', 'bar', 'foo', 'baz']
        rr = self.get_balancer(1, keys)
        self.assertEqual(len(rr), 4)

    def test_iter(self):
        keys = ['foo', 'bar', 'foo', 'baz']
        rr = self.get_balancer(1, keys)
        self.assertEqual(list(rr), keys)

    def test_contains(self):
        keys = ['foo', 'bar', 'foo', 'baz']
        rr = self.get_balancer(1, keys)

        for key in 'foo', 'bar', 'baz':
            self.assertIn(key, rr)

        for key in 'fooz', 'barz', None:
            self.assertNotIn(key, rr)

    def test_add(self):
        rr = self.get_balancer(1)
        rr.add('foo')
        self.assertQueue(rr, ['foo'])

        rr.add('bar')
        self.assertQueue(rr, ['foo', 'bar'])

        rr.add('foo')
        self.assertQueue(rr, ['foo', 'bar', 'foo'])

        rr.add('foo', 'baz', 'bar')
        self.assertQueue(rr, ['foo', 'bar', 'foo', 'foo', 'baz', 'bar'])

    def test_pop(self):
        rr = self.get_balancer(1, ['foo', 'bar', 'foo', 'baz'])

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
        rr = self.get_balancer(1, ['foo', 'bar', 'bar', 'foo', 'baz', 'bar'])
        rr.discard('foo')
        self.assertQueue(rr, ['bar', 'bar', 'baz', 'bar'])

        rr.discard('xyz')
        self.assertQueue(rr, ['bar', 'bar', 'baz', 'bar'])

        rr.discard('bar', count=1)
        self.assertQueue(rr, ['bar', 'baz', 'bar'])

        rr.discard('bar', count=-1)
        self.assertQueue(rr, ['bar', 'baz'])

    def test_remove(self):
        rr = self.get_balancer(1, ['foo', 'bar', 'bar', 'foo', 'baz', 'bar'])
        rr.remove('foo')
        self.assertQueue(rr, ['bar', 'bar', 'baz', 'bar'])

        with self.assertRaises(KeyError):
            rr.remove('xyz')
            self.assertQueue(rr, ['bar', 'bar', 'baz', 'bar'])

        rr.remove('bar', count=1)
        self.assertQueue(rr, ['bar', 'baz', 'bar'])

    def test_clear(self):
        rr = self.get_balancer(1, ['foo', 'bar', 'foo', 'baz'])
        rr.clear()
        self.assertQueue(rr, [])

    # def test_next_empty(self):
    #     rr = self.get_balancer()
    #     self.assertRaises(StopIteration, rr.next)
    #
    # def test_next_unthrottled(self):
    #     rr = self.get_balancer(dict.fromkeys(['foo', 'bar', 'baz'], 0))
    #     for key in islice(cycle(['bar', 'baz', 'foo']), 100):
    #         self.assertEqual(rr.next(), key)
    #
    # @MockTime.patch()
    # def test_next_throttled(self):
    #     throttle = 1
    #     rr = self.get_balancer(dict.fromkeys(['foo', 'bar', 'baz'], throttle))
    #
    #     # unthrottled
    #     first_throttled_until = None
    #     for key in 'bar', 'baz', 'foo':
    #         with self.assertAlmostEqualDuration(TIME_DELTA):
    #             self.assertEqual(rr.next(), key)
    #             if first_throttled_until is None:
    #                 first_throttled_until = time.time() + throttle
    #
    #     # throttled
    #     with self.assertAlmostEqualDuration(first_throttled_until - time.time()):
    #         self.assertEqual(rr.next(), 'bar')
    #
    #     # unthrottled
    #     for key in 'baz', 'foo':
    #         with self.assertAlmostEqualDuration(TIME_DELTA):
    #             self.assertEqual(rr.next(), key)
    #
    # @MockTime.patch()
    # def test_next_throttled_no_wait(self):
    #     throttle = 1
    #     rr = self.get_balancer(dict.fromkeys(['foo', 'bar', 'baz'], throttle))
    #
    #     # unthrottled
    #     for key in 'bar', 'baz', 'foo':
    #         with self.assertAlmostEqualDuration(TIME_DELTA):
    #             self.assertEqual(rr.next(wait=False), key)
    #
    #     # throttled
    #     for _ in xrange(10):
    #         with self.assertAlmostEqualDuration(TIME_DELTA):
    #             self.assertIsNone(rr.next(wait=False))
    #
    #     time.sleep(throttle)
    #     # unthrottled
    #     for key in 'bar', 'baz', 'foo':
    #         with self.assertAlmostEqualDuration(TIME_DELTA):
    #             self.assertEqual(rr.next(wait=False), key)
    #
    # @MockTime.patch()
    # def test_throttled_until(self):
    #     throttle = 1
    #     keys = ['foo', 'bar', 'baz']
    #     rr = self.get_balancer(dict.fromkeys(keys, throttle))
    #
    #     # unthrottled
    #     first_throttled_until = None
    #     for _ in keys:
    #         self.assertIsNone(rr.throttled_until())
    #         rr.next()
    #         if first_throttled_until is None:
    #             first_throttled_until = time.time() + throttle
    #
    #     # throttled
    #     for _ in xrange(10):
    #         self.assertAlmostEqualTime(rr.throttled_until(), first_throttled_until)
    #
    #     time.sleep(throttle)
    #     # unthrottled
    #     for _ in keys:
    #         self.assertIsNone(rr.throttled_until())
    #         rr.next()
