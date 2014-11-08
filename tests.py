from itertools import islice
import unittest

import redis
import redrobin


class RedisTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # try to connect to a random Redis database (starting from 4) and
        # use/connect it when no keys are in there.
        for dbnum in range(16):
            test_conn = redis.StrictRedis(db=dbnum)
            cursor, results = test_conn.scan(0)
            if cursor == 0 and not results: # empty
                cls.test_conn = test_conn
                return
        assert False, 'No empty Redis database found to run tests in'

    def setUp(self):
        self.test_conn.flushdb()

    def tearDown(self):
        self.test_conn.flushdb()


class RedRobinTestCase(RedisTestCase):

    def RoundRobin(self, throttled_items=None, default_throttle=0):
        rr = redrobin.RoundRobin(default_throttle=default_throttle, name='test',
                                 connection=self.test_conn)
        if throttled_items:
            rr.update_many(throttled_items)
        return rr

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
        self.assertQueuesThrottles(rr, ['bar', 'foo'], {'foo': 5, 'bar': 4})

        # neither queue nor throttle of foo is updated
        rr.update_one('foo')
        self.assertQueuesThrottles(rr, ['bar', 'foo'], {'foo': 5, 'bar': 4})

        # queue not updated but throttle of foo is
        rr.update_one('foo', 3)
        self.assertQueuesThrottles(rr, ['bar', 'foo'], {'foo': 3, 'bar': 4})

        # new item with default throttle(=0) added
        rr.update_one('xyz')
        self.assertQueuesThrottles(rr, ['xyz', 'bar', 'foo'], {'foo': 3, 'bar': 4, 'xyz': 0})

    def test_update_many(self):
        rr = self.RoundRobin()
        rr.update_many(dict.fromkeys(['foo', 'bar'], 5))
        self.assertQueuesThrottles(rr, ['bar', 'foo'], {'foo': 5, 'bar': 5})

        # baz and xyz are pushed, foo updates its throttle but stays in the same position
        rr.update_many(dict.fromkeys(['baz', 'foo', 'xyz'], 2))
        self.assertQueuesThrottles(rr, ['baz', 'xyz', 'bar', 'foo'],
                                   {'foo': 2, 'bar': 5, 'baz': 2, 'xyz': 2})

        rr.update_many({'uvw': 3, 'foo': 4})
        self.assertQueuesThrottles(rr, ['baz', 'xyz', 'uvw', 'bar', 'foo'],
                                   {'foo': 4, 'bar': 5, 'baz': 2, 'xyz': 2, 'uvw': 3})

        # default throttle(=0) used only when adding new items, not updating existing ones
        rr.update_many(['uvw', 'qa', 'bar'])
        self.assertQueuesThrottles(rr, ['qa', 'baz', 'xyz', 'uvw', 'bar', 'foo'],
                                   {'foo': 4, 'bar': 5, 'baz': 2, 'xyz': 2, 'uvw': 3, 'qa': 0})

    def test_remove_existing(self):
        rr = self.RoundRobin({'foo': 3, 'bar': 4, 'baz': 2})
        rr.remove('foo')
        self.assertQueuesThrottles(rr, ['baz', 'bar'], {'bar': 4, 'baz': 2})

    def test_remove_missing(self):
        rr = self.RoundRobin({'foo': 3, 'bar': 4, 'baz': 2})
        rr.remove('xyz')
        self.assertQueuesThrottles(rr, ['baz', 'foo', 'bar'], {'foo':3, 'bar': 4, 'baz': 2})

    def test_remove_multiple(self):
        rr = self.RoundRobin({'foo': 3, 'bar': 4, 'baz': 2})
        rr.remove('baz', 'xyz', 'bar')
        self.assertQueuesThrottles(rr, ['foo'], {'foo': 3})

    def test_clear(self):
        rr = self.RoundRobin({'foo': 3, 'bar': 4, 'baz': 2})
        rr.clear()
        self.assertQueuesThrottles(rr, [], {})
