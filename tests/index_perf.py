import unittest
from influxgraph.utils import Query
import datetime
from timeit import timeit
from pprint import pprint

class IndexTreePerfTestCase(unittest.TestCase):
    series = """series = [u'.'.join([u''.join([choice(ascii_letters) for _ in range(randint(1,255))])
                                     for _ in range(randint(1, 500))])
                          for _ in range(5,1000)]"""
    timeit_setup = """from string import ascii_letters
from random import randint, choice
%s
%s
index = NodeTreeIndex()
for serie in series:
    index.insert(serie)"""

    def test_python_index(self):
        index_import = """from influxgraph.classes.tree import NodeTreeIndex"""
        pprint("Python index load time is %s" % (
            timeit(stmt='for serie in series: index.insert(serie)',
                   setup=self.timeit_setup % (self.series, index_import), number=10),))

    def test_python_cindex(self):
        index_import = """from influxgraph.classes.ext.tree import NodeTreeIndex"""
        pprint("Cython index load time is %s" % (
            timeit(stmt='for serie in series: index.insert(serie)',
                   setup=self.timeit_setup % (self.series, index_import), number=10),))
