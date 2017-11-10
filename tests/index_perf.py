from __future__ import print_function
import unittest
from timeit import timeit
from pprint import pprint


class IndexTreePerfTestCase(unittest.TestCase):
    templates = """
_templates = ["dc.env.host.measurement.field*"]
templates = parse_influxdb_graphite_templates(_templates)
tags = [[u','.join(['='.join([tag, val])
                 for tag in ['host', 'dc', 'env']])
        for val in [u''.join([choice(ascii_letters)
                      for _ in range(4)])
            for _ in range(10)]]]
measurements = [u''.join([choice(ascii_letters) for _ in range(8)])
                for _ in range(200)]
all_series = [u','.join([m, t])
    for m in measurements
    for _t in tags
    for t in _t]

fields = {m: [u''.join([choice(ascii_letters) for _ in range(8)])
              for _ in range(10)]
    for m in measurements}
"""
    series = """
series = [u'.'.join([u''.join([choice(ascii_letters) for _ in range(8)])
                                     for _ in range(28)])
                          for _ in range(5, 500)]"""
    timeit_setup = """from string import ascii_letters
from random import randint, choice
from influxgraph.templates import parse_influxdb_graphite_templates
%s
%s
queries = ['.'.join(['*' for _ in range(1,10)]) for _ in range(10,20)]"""
    py_timeit_insert_stmt = """
index = Node()
split_series = [serie.split('.') for serie in series]
for split_path in split_series:
    index.insert_split_path(split_path)
    """
    py_timeit_tmpl_stmt = """
index = py_parse_series(all_series, fields, templates)
    """
    timeit_insert_stmt = """
index = parse_series(series, None, None)
    """
    timeit_tmpl_stmt = """
index = parse_series(all_series, fields, templates)
    """
    timeit_query_stmt = """
for query in queries:
    index.query(query)
    """

    def time_index(self, index_import, insert_stmt, tmpl_stmt):
        return timeit(
            stmt=insert_stmt,
            setup=self.timeit_setup % (self.series, index_import), number=10), \
            timeit(
                stmt=tmpl_stmt,
                setup="\n".join([self.timeit_setup % (self.templates, index_import)]),
                number=10), \
            timeit(stmt=self.timeit_query_stmt,
                   setup="\n".join([self.timeit_setup % (self.series, index_import),
                                    self.py_timeit_insert_stmt]),
                   number=10)

    def test_python_index(self):
        index_import = """
from influxgraph.classes.tree import NodeTreeIndex as Node
from influxgraph.utils import parse_series as py_parse_series"""
        load_time, reload_time, query_time = self.time_index(
            index_import, self.py_timeit_insert_stmt, self.py_timeit_tmpl_stmt)
        pprint("Python index load time is %s" % (load_time,))
        pprint("Python index template load time is %s" % (reload_time,))
        pprint("Python index query time is %s" % (query_time,))

    def test_c_index(self):
        c_node_import = """
from influxgraph.ext.templates import parse_series
from influxgraph.ext.nodetrie import Node"""
        load_time, reload_time, query_time = self.time_index(
            c_node_import, self.timeit_insert_stmt, self.timeit_tmpl_stmt)
        pprint("C index load time is %s" % (load_time,))
        pprint("C index template load time is %s" % (reload_time,))
        pprint("C index query time is %s" % (query_time,))
