import unittest
from timeit import timeit
from pprint import pprint

class IndexTreePerfTestCase(unittest.TestCase):
    series = """series = [u'.'.join([u''.join([choice(ascii_letters) for _ in range(127)])
                                     for _ in range(500)])
                          for _ in range(5, 1000)]"""
    timeit_setup = """from string import ascii_letters
from random import randint, choice
%s
%s
index = NodeTreeIndex()"""
    timeit_insert_stmt = """
index.insert_series(series)"""
    timeit_query_stmt = """
for query in ['.'.join(['*' for _ in range(1,10)]) for _ in range(10,20)]:
    index.query(query)
    """

    def time_index(self, index_import):
        return timeit(
            stmt=self.timeit_insert_stmt,
            setup=self.timeit_setup % (self.series, index_import), number=10), \
            timeit(
                stmt=self.timeit_insert_stmt,
                setup="\n".join([self.timeit_setup % (self.series, index_import),
                                 self.timeit_query_stmt]),
                number=10), \
            timeit(stmt=self.timeit_query_stmt,
                   setup="\n".join([self.timeit_setup % (self.series, index_import),
                                    self.timeit_insert_stmt]),
                   number=10)

    def test_python_index(self):
        index_import = """from influxgraph.classes.tree import NodeTreeIndex"""
        load_time, reload_time, query_time = self.time_index(index_import)
        pprint("Python index load time is %s" % (load_time,))
        pprint("Python index re-load time is %s" % (reload_time,))
        pprint("Python index query time is %s" % (query_time,))

    def test_cython_index(self):
        index_import = """from influxgraph.ext.classes.tree import NodeTreeIndex"""
        load_time, reload_time, query_time = self.time_index(index_import)
        pprint("Cython index load time is %s" % (load_time,))
        pprint("Cython index re-load time is %s" % (reload_time,))
        pprint("Cython index query time is %s" % (query_time,))
