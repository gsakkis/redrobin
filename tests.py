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

    def RoundRobin(self, items=None, default_throttle=0):
        return redrobin.RoundRobin(items=items, default_throttle=default_throttle,
                                   name='test', connection=self.test_conn)

    def get_queue(self, round_robin, withscores=False):
        return self.test_conn.zrange(round_robin._key, 0, -1, withscores=withscores)

    def test_init_empty(self):
        self.assertEqual(self.get_queue(self.RoundRobin()), [])

    def test_init_from_mapping(self):
        rr = self.RoundRobin({'foo': 3, 'bar': 4, 'baz': 2})
        self.assertEqual(self.get_queue(rr), ['baz', 'foo', 'bar'])

    def test_init_from_sequence(self):
        items = ['foo', 'bar', 'baz']
        for rr in self.RoundRobin(items), self.RoundRobin(items, default_throttle=2):
            q, scores = zip(*self.get_queue(rr, withscores=True))
            self.assertEqual(len(set(scores)), 1)
            self.assertEqual(q, ('bar', 'baz', 'foo'))

    def test_add(self):
        rr = self.RoundRobin()
        rr.add('foo', 5)
        rr.add('bar', 4)
        rr.add('foo', 3)
        rr.add('baz', 2)
        self.assertEqual(self.get_queue(rr), ['baz', 'foo', 'bar'])

    def test_remove_existing(self):
        rr = self.RoundRobin({'foo': 3, 'bar': 4, 'baz': 2})
        rr.remove('foo')
        self.assertEqual(self.get_queue(rr), ['baz', 'bar'])

    def test_remove_missing(self):
        rr = self.RoundRobin({'foo': 3, 'bar': 4, 'baz': 2})
        rr.remove('xyz')
        self.assertEqual(self.get_queue(rr), ['baz', 'foo', 'bar'])

    def test_remove_multiple(self):
        rr = self.RoundRobin({'foo': 3, 'bar': 4, 'baz': 2})
        rr.remove('baz', 'xyz', 'bar')
        self.assertEqual(self.get_queue(rr), ['foo'])

    def test_clear(self):
        rr = self.RoundRobin({'foo': 3, 'bar': 4, 'baz': 2})
        rr.clear()
        self.assertEqual(self.get_queue(rr), [])
