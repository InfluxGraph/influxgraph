import sys
import unittest
from string import ascii_letters
from random import randint, choice

try:
    from influxgraph.ext.nodetrie import Node
except ImportError:
    NODE_TRIE = False
else:
    NODE_TRIE = True

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
        # import ipdb; ipdb.set_trace()
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
