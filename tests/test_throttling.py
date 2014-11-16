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

    # def test_delitem(self):
    #     rr = self.get_balancer({'foo': 3, 'bar': 4, 'baz': 2})
    #     del rr['foo']
    #     self.assertQueue(rr, ['bar', 'baz'], {'bar': 4, 'baz': 2})
    #
    #     with self.assertRaises(KeyError):
    #         del rr['xyz']
    #         self.assertQueue(rr, ['bar', 'baz'], {'bar': 4, 'baz': 2})
    #
    # def test_pop(self):
    #     rr = self.get_balancer({'foo': 3, 'bar': 4, 'baz': 2})
    #     self.assertEqual(rr.pop('foo'), 3)
    #     self.assertQueue(rr, ['bar', 'baz'], {'bar': 4, 'baz': 2})
    #
    #     with self.assertRaises(KeyError):
    #         rr.pop('xyz')
    #         self.assertQueue(rr, ['bar', 'baz'], {'bar': 4, 'baz': 2})
    #
    #     self.assertEqual(rr.pop('xyz', -1), -1)
    #     self.assertQueue(rr, ['bar', 'baz'], {'bar': 4, 'baz': 2})
    #
    # def test_update(self):
    #     rr = self.get_balancer()
    #     rr.update(dict.fromkeys(['foo', 'bar'], 5))
    #     self.assertQueue(rr, ['bar', 'foo'], {'foo': 5, 'bar': 5})
    #
    #     # baz and xyz are pushed, foo updates its throttle but stays in the same position
    #     rr.update(dict.fromkeys(['baz', 'foo', 'xyz'], 2))
    #     self.assertQueue(rr, ['bar', 'foo', 'baz', 'xyz'],
    #                                {'foo': 2, 'bar': 5, 'baz': 2, 'xyz': 2})
    #
    #     # update from kwargs
    #     rr.update(xyz=3, abc=4)
    #     self.assertQueue(rr, ['bar', 'foo', 'baz', 'xyz', 'abc'],
    #                                {'foo': 2, 'bar': 5, 'baz': 2, 'xyz': 3, 'abc': 4})
    #
    #     # update from both a dict and kwargs
    #     rr.update({'bar': 2, 'mno': 8}, foo=1, ghi=7)
    #     self.assertQueue(rr, ['bar', 'foo', 'baz', 'xyz', 'abc', 'ghi', 'mno'],
    #                                {'foo': 1, 'bar': 2, 'baz': 2, 'xyz': 3,
    #                                 'abc': 4, 'mno': 8, 'ghi': 7})
    #
    #     # invalid throttle
    #     for throttle in -1, '1', None:
    #         self.assertRaises(ValueError, rr.update, {'foo': throttle})
    #
    # def test_discard(self):
    #     rr = self.get_balancer({'foo': 3, 'bar': 4, 'baz': 2})
    #     rr.discard('foo')
    #     self.assertQueue(rr, ['bar', 'baz'], {'bar': 4, 'baz': 2})
    #
    #     rr.discard('xyz')
    #     self.assertQueue(rr, ['bar', 'baz'], {'bar': 4, 'baz': 2})
    #
    #     rr.discard('baz', 'xyz')
    #     self.assertQueue(rr, ['bar'], {'bar': 4})
    #
    # def test_clear(self):
    #     rr = self.get_balancer({'foo': 3, 'bar': 4, 'baz': 2})
    #     rr.clear()
    #     self.assertQueue(rr, [], {})
    #
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
