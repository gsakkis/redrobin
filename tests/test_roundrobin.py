from itertools import cycle, islice
import redrobin

from . import BaseTestCase


class RoundRobinSchedulerTestCase(BaseTestCase):

    def get_scheduler(self, keys=None, name='test'):
        return redrobin.RoundRobinScheduler(keys=keys, name=name,
                                           connection=self.test_conn)

    def assertQueue(self, round_robin, expected_queues):
        queue = map(round_robin._unpickle, self.test_conn.lrange(round_robin.key, 0, -1))
        self.assertEqual(queue, expected_queues)

    def test_init(self):
        rr = self.get_scheduler()
        self.assertQueue(rr, [])

        keys = ['foo', 'bar', 'foo', 'baz']
        rr = self.get_scheduler(keys)
        self.assertQueue(rr, keys)

    def test_len(self):
        keys = ['foo', 'bar', 'foo', 'baz']
        rr = self.get_scheduler(keys)
        self.assertEqual(len(rr), 4)

    def test_iter(self):
        keys = ['foo', 'bar', 'foo', 'baz']
        rr = self.get_scheduler(keys)
        self.assertEqual(list(rr), keys)

    def test_contains(self):
        keys = ['foo', 'bar', 'foo', 'baz']
        rr = self.get_scheduler(keys)

        for key in 'foo', 'bar', 'baz':
            self.assertIn(key, rr)

        for key in 'fooz', 'barz', None:
            self.assertNotIn(key, rr)

    def test_add(self):
        rr = self.get_scheduler()
        rr.add('foo')
        self.assertQueue(rr, ['foo'])

        rr.add('bar')
        self.assertQueue(rr, ['foo', 'bar'])

        rr.add('foo')
        self.assertQueue(rr, ['foo', 'bar', 'foo'])

        rr.add('foo', 'baz', 'bar')
        self.assertQueue(rr, ['foo', 'bar', 'foo', 'foo', 'baz', 'bar'])

    def test_pop(self):
        rr = self.get_scheduler(['foo', 'bar', 'foo', 'baz'])

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
        rr = self.get_scheduler(['foo', 'bar', 'bar', 'foo', 'baz', 'bar'])
        rr.discard('foo')
        self.assertQueue(rr, ['bar', 'bar', 'baz', 'bar'])

        rr.discard('xyz')
        self.assertQueue(rr, ['bar', 'bar', 'baz', 'bar'])

        rr.discard('bar', count=1)
        self.assertQueue(rr, ['bar', 'baz', 'bar'])

        rr.discard('bar', count=-1)
        self.assertQueue(rr, ['bar', 'baz'])

    def test_remove(self):
        rr = self.get_scheduler(['foo', 'bar', 'bar', 'foo', 'baz', 'bar'])
        rr.remove('foo')
        self.assertQueue(rr, ['bar', 'bar', 'baz', 'bar'])

        with self.assertRaises(KeyError):
            rr.remove('xyz')
            self.assertQueue(rr, ['bar', 'bar', 'baz', 'bar'])

        rr.remove('bar', count=1)
        self.assertQueue(rr, ['bar', 'baz', 'bar'])

    def test_clear(self):
        rr = self.get_scheduler(['foo', 'bar', 'foo', 'baz'])
        rr.clear()
        self.assertQueue(rr, [])

    def test_next_empty(self):
        rr = self.get_scheduler()
        self.assertRaises(StopIteration, rr.next)

    def test_next(self):
        keys = ['foo', 'bar', 'foo', 'baz']
        rr = self.get_scheduler(keys)
        for key in islice(cycle(keys), 100):
            self.assertEqual(rr.next(), key)
