import sys
import unittest

try:
    from influxgraph.ext.nodetrie import Node as NodeTreeIndex
except ImportError:
    NODE_TRIE = False
else:
    NODE_TRIE = True
from influxgraph.utils import Query


@unittest.skipUnless(NODE_TRIE, "NodeTrie extension not enabled")
class CIndexTreeTestCase(unittest.TestCase):

    def setUp(self):
        all_series = [u'b1.b1.b1.b1.leaf1',
                      u'b1.b1.b1.b2.leaf1',
                      u'b1.b1.b2.b2.leaf1',
                      u'b1.b1.b1.b1.leaf2',
                      u'b1.b1.b1.b2.leaf2',
                      u'b1.b1.b2.b2.leaf2'
                      ]
        self.index = NodeTreeIndex()
        for serie in all_series:
            self.index.insert(serie)

    def test_root_wildcard(self):
        result = list(self.index.query('*'))
        self.assertTrue(len(result) == 1)
        # Unicode query test
        result = list(self.index.query(u'*'))
        self.assertTrue(len(result) == 1)
        self.assertTrue(result[0][0] == 'b1')
        result = list(self.index.query('b1'))
        self.assertTrue(result[0][0] == 'b1')
        result = list(self.index.query('b1.*'))
        self.assertTrue(len(result) == 1)
        self.assertTrue(result[0][0] == 'b1.b1')
        result = list(self.index.query('b1.b1.*'))
        self.assertTrue(len(result) == 2)
        self.assertTrue(result[0][0] == 'b1.b1.b1')
        self.assertTrue(result[1][0] == 'b1.b1.b2')
        result = list(self.index.query('b1.b1.*.*'))
        self.assertTrue(len(result) == 3)
        self.assertTrue(result[0][0] == 'b1.b1.b1.b1')
        self.assertTrue(result[1][0] == 'b1.b1.b1.b2')
        self.assertTrue(result[2][0] == 'b1.b1.b2.b2')
        result = list(self.index.query('b1.b1.*.*.*'))
        self.assertTrue(len(result) == 6)
        self.assertTrue(result[0][0] == 'b1.b1.b1.b1.leaf1')
        self.assertTrue(result[1][0] == 'b1.b1.b1.b1.leaf2')
        self.assertTrue(result[2][0] == 'b1.b1.b1.b2.leaf1')
        self.assertTrue(result[3][0] == 'b1.b1.b1.b2.leaf2')
        self.assertTrue(result[4][0] == 'b1.b1.b2.b2.leaf1')
        self.assertTrue(result[5][0] == 'b1.b1.b2.b2.leaf2')
        result = list(self.index.query('b1.b1.*.*.{leaf1,leaf2}'))
        self.assertTrue(len(result) == 6)
        self.assertTrue(result[0][0] == 'b1.b1.b1.b1.leaf1')
        self.assertTrue(result[1][0] == 'b1.b1.b1.b1.leaf2')
        self.assertTrue(result[2][0] == 'b1.b1.b1.b2.leaf1')
        self.assertTrue(result[3][0] == 'b1.b1.b1.b2.leaf2')
        self.assertTrue(result[4][0] == 'b1.b1.b2.b2.leaf1')
        self.assertTrue(result[5][0] == 'b1.b1.b2.b2.leaf2')
        result = list(self.index.query('b1.b1.b1.b1.leaf1'))
        self.assertTrue(len(result) == 1)
        self.assertTrue(result[0][0] == 'b1.b1.b1.b1.leaf1')
        result = list(self.index.query('fakey*'))
        self.assertFalse(result)
