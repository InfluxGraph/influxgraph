import sys
import unittest

try:
    from influxgraph.classes.ext.tree import NodeTreeIndex
except ImportError:
    from influxgraph.classes.tree import NodeTreeIndex
from influxgraph.utils import Query

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
        self.assertTrue(result[0]['metric'] == 'b1')
        result = list(self.index.query('b1'))
        self.assertTrue(result[0]['metric'] == 'b1')
        result = list(self.index.query('b1.*'))
        self.assertTrue(len(result) == 1)
        self.assertTrue(result[0]['metric'] == 'b1.b1')
        result = list(self.index.query('b1.b1.*'))
        self.assertTrue(len(result) == 2)
        self.assertTrue(result[0]['metric'] == 'b1.b1.b1')
        self.assertTrue(result[1]['metric'] == 'b1.b1.b2')
        result = list(self.index.query('b1.b1.*.*'))
        self.assertTrue(len(result) == 3)
        self.assertTrue(result[0]['metric'] == 'b1.b1.b1.b1')
        self.assertTrue(result[1]['metric'] == 'b1.b1.b1.b2')
        self.assertTrue(result[2]['metric'] == 'b1.b1.b2.b2')
        result = list(self.index.query('b1.b1.*.*.*'))
        self.assertTrue(len(result) == 6)
        self.assertTrue(result[0]['metric'] == 'b1.b1.b1.b1.leaf1')
        self.assertTrue(result[1]['metric'] == 'b1.b1.b1.b1.leaf2')
        self.assertTrue(result[2]['metric'] == 'b1.b1.b1.b2.leaf1')
        self.assertTrue(result[3]['metric'] == 'b1.b1.b1.b2.leaf2')
        self.assertTrue(result[4]['metric'] == 'b1.b1.b2.b2.leaf1')
        self.assertTrue(result[5]['metric'] == 'b1.b1.b2.b2.leaf2')
        result = list(self.index.query('b1.b1.*.*.{leaf1,leaf2}'))
        self.assertTrue(len(result) == 6)
        self.assertTrue(result[0]['metric'] == 'b1.b1.b1.b1.leaf1')
        self.assertTrue(result[1]['metric'] == 'b1.b1.b1.b1.leaf2')
        self.assertTrue(result[2]['metric'] == 'b1.b1.b1.b2.leaf1')
        self.assertTrue(result[3]['metric'] == 'b1.b1.b1.b2.leaf2')
        self.assertTrue(result[4]['metric'] == 'b1.b1.b2.b2.leaf1')
        self.assertTrue(result[5]['metric'] == 'b1.b1.b2.b2.leaf2')
        result = list(self.index.query('b1.b1.b1.b1.leaf1'))
        self.assertTrue(len(result) == 1)
        self.assertTrue(result[0]['metric'] == 'b1.b1.b1.b1.leaf1')
        result = list(self.index.query('fakey*'))
        self.assertFalse(result)

    def test_to_from_array(self):
        index2 = NodeTreeIndex.from_array(self.index.to_array())
        self.assertEqual(index2.to_array(), self.index.to_array())
        self.assertEqual(list(self.index.query('*')), list(index2.query('*')))
