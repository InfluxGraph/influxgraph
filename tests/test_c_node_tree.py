import sys
import unittest
from string import ascii_letters
from random import randint, choice
import logging

from influxgraph.templates import parse_influxdb_graphite_templates
from influxgraph.utils import parse_series as parse_py_series
try:
    from influxgraph.ext.nodetrie import Node
    from influxgraph.ext.templates import parse_series
except ImportError:
    NODE_TRIE = False
else:
    NODE_TRIE = True


logger = logging.getLogger('influxgraph')
logger.setLevel(logging.DEBUG)
logging.basicConfig()


@unittest.skipUnless(NODE_TRIE, "NodeTrie extension not enabled")
class CNodeTreeTestCase(unittest.TestCase):

    def setUp(self):
        self.all_series = ['b1.b1.b1.b1.leaf1',
                           'b1.b1.b1.b2.leaf1',
                           'b1.b1.b2.b2.leaf1',
                           'b1.b1.b1.b1.leaf2',
                           'b1.b1.b1.b2.leaf2',
                           'b1.b1.b2.b2.leaf2'
        ]
        self.index = Node()
        for serie in self.all_series:
            split_path = serie.split('.')
            self.index.insert_split_path(split_path)

    def tearDown(self):
        del self.index

    def test_parse_series(self):
        all_series = [u'b1.b1.b1.b1.leaf1',
                      u'b1.b1.b1.b2.leaf1',
                      u'b1.b1.b2.b2.leaf1',
                      u'b1.b1.b1.b1.leaf2',
                      u'b1.b1.b1.b2.leaf2',
                      u'b1.b1.b2.b2.leaf2'
        ]
        index = parse_series(all_series, None, None)
        result = list(index.query(u'*'))
        self.assertTrue(len(result) > 0)
        self.assertEqual(result[0][0], 'b1')
        result = list(index.query('b1'))
        self.assertTrue(result[0][0] == 'b1')
        result = list(index.query('b1.*'))
        self.assertTrue(len(result) == 1)
        self.assertTrue(result[0][0] == 'b1.b1')
        result = list(index.query('b1.b1.*'))
        self.assertEqual(len(result), 2)
        self.assertTrue(result[0][0] == 'b1.b1.b1')
        self.assertTrue(result[1][0] == 'b1.b1.b2')
        result = list(index.query('b1.b1.*.*'))
        self.assertTrue(len(result) == 3)
        self.assertTrue(result[0][0] == 'b1.b1.b1.b1')
        self.assertTrue(result[1][0] == 'b1.b1.b1.b2')
        self.assertTrue(result[2][0] == 'b1.b1.b2.b2')
        result = list(index.query('b1.b1.*.*.*'))
        self.assertTrue(len(result) == 6)
        self.assertTrue(result[0][0] == 'b1.b1.b1.b1.leaf1')
        self.assertTrue(result[1][0] == 'b1.b1.b1.b1.leaf2')
        self.assertTrue(result[2][0] == 'b1.b1.b1.b2.leaf1')
        self.assertTrue(result[3][0] == 'b1.b1.b1.b2.leaf2')
        self.assertTrue(result[4][0] == 'b1.b1.b2.b2.leaf1')
        self.assertTrue(result[5][0] == 'b1.b1.b2.b2.leaf2')
        result = list(index.query('b1.b1.*.*.{leaf1,leaf2}'))
        self.assertTrue(len(result) == 6)
        self.assertTrue(result[0][0] == 'b1.b1.b1.b1.leaf1')
        self.assertTrue(result[1][0] == 'b1.b1.b1.b1.leaf2')
        self.assertTrue(result[2][0] == 'b1.b1.b1.b2.leaf1')
        self.assertTrue(result[3][0] == 'b1.b1.b1.b2.leaf2')
        self.assertTrue(result[4][0] == 'b1.b1.b2.b2.leaf1')
        self.assertTrue(result[5][0] == 'b1.b1.b2.b2.leaf2')
        result = list(index.query('b1.b1.b1.b1.leaf1'))
        self.assertTrue(len(result) == 1)
        self.assertTrue(result[0][0] == 'b1.b1.b1.b1.leaf1')
        result = list(index.query('fakey*'))
        self.assertFalse(result)

    def test_parse_series_templates(self):
        _templates = ["dc.env.host.measurement.field*"]
        templates = parse_influxdb_graphite_templates(_templates)
        all_series = [u'm1,host=b1,dc=dc1,env=a',
                      u'm1,host=b1,dc=dc1,env=a',
                      u'm2,host=b1,dc=dc1,env=a',
                      u'm2,host=b2,dc=dc1,env=a',
                      u'm2,host=b2,dc=dc2,env=a',
                      u'm2,host=b2,dc=dc2,env=b'
        ]
        fields = {u'm1': [u'leaf1', u'leaf2'],
                  u'm2': [u'leaf1']}
        index = parse_series(all_series, fields, templates)
        result = sorted(list(index.query(u'*')))
        self.assertTrue(len(result) > 0)
        self.assertEqual(result[0][0], 'dc1')

    def test_empty_tree(self):
        tree = Node()
        self.assertTrue(len(tree.children) == 0)
        self.assertTrue(tree.name is None)

    def test_index(self):
        self.assertEqual(self.index.children_size, 1)
        for serie in self.all_series:
            split_path = serie.split('.')
            i = 0
            parent = self.index
            while i < len(split_path):
                path = split_path[i]
                child = [c for c in parent.children if c.name == path]
                self.assertTrue(len(child) > 0)
                child = child[0]
                self.assertEqual(child.name, path)
                parent = child
                i += 1
                if i < len(split_path):
                    self.assertFalse(child.is_leaf())
                else:
                    self.assertTrue(child.is_leaf())

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
        # dumped_ar = self.index.to_array()
        # self.index = Node.from_array(dumped_ar)
        # import ipdb; ipdb.set_trace()

    def test_string_insert(self):
        del self.index
        self.index = Node()
        for serie in self.all_series:
            self.index.insert(serie)
        self.test_root_wildcard()
