import unittest
from graphite_influxdb.classes.tree import NodeTreeIndex
from graphite_influxdb.utils import Query
import datetime

class IndexTreeTestCase(unittest.TestCase):

    def setUp(self):
        all_series = ['b1.b1.b1.b1.leaf1',
                      'b1.b1.b1.b2.leaf1',
                      'b1.b1.b2.b2.leaf1',
                      'b1.b1.b1.b1.leaf2',
                      'b1.b1.b1.b2.leaf2',
                      'b1.b1.b2.b2.leaf2'
                      ]
        self.index = NodeTreeIndex()
        for serie in all_series:
            self.index.insert(serie)
    
    def test_root_wildcard(self):
        result = list(self.index.query('*'))
        self.assertTrue(len(result) == 1)
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
        result = list(self.index.query('fakey*'))
        self.assertFalse(result)
