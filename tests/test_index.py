import unittest
from graphite_influxdb.classes.tree import IndexTree
from graphite_influxdb.utils import Query
import datetime

class IndexTreeTestCase(unittest.TestCase):

    def setUp(self):
        # all_series = [
        #     'root.branch%s.branch%s.branch%s.branch%s.leaf%s' % (b1, b2, b3, b4, n,)
        #     for b1 in xrange(1,5)
        #     for b2 in xrange(1,5)
        #     for b3 in xrange(1,5)
        #     for b4 in xrange(1,5)
        #     for n in xrange(1,5)]
        all_series = ['b1.b1.b1.b1.leaf1',
                      'b1.b1.b1.b2.leaf1',
                      'b1.b1.b2.b2.leaf1',
                      'b1.b1.b1.b1.leaf2',
                      'b1.b1.b1.b2.leaf2',
                      'b1.b1.b2.b2.leaf2'
                      ]
        self.index = IndexTree(all_series, None)
    
    def test_root_wildcard(self):
        query = '*'
        result = list(self.index.search(query))
        print result
        import ipdb; ipdb.set_trace()
