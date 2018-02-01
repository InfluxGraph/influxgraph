# -*- coding: utf-8 -*-
import os
import unittest
import sys
import tempfile
import datetime
import time
from gzip import GzipFile
from random import randint
import logging
import fcntl
from time import sleep

from influxgraph.influxdb import InfluxDBClient as INFLClient
from influxdb.client import InfluxDBClient
import influxdb.exceptions
import influxgraph
import influxgraph.utils
from influxgraph.utils import Query, gen_memcache_key, get_aggregation_func, \
     Node
from influxgraph.constants import SERIES_LOADER_MUTEX_KEY, \
     MEMCACHE_SERIES_DEFAULT_TTL, LOADER_LIMIT, DEFAULT_AGGREGATIONS, \
     _INFLUXDB_CLIENT_PARAMS, FILE_LOCK
from influxgraph.classes.finder import logger as finder_logger, \
    _SERIES_LOADER_LOCK
import memcache

finder_logger.setLevel(logging.DEBUG)
logging.basicConfig()

os.environ['TZ'] = 'UTC'

class InfluxGraphIntegrationTestCase(unittest.TestCase):

    def setup_db(self):
        try:
            self.client.drop_database(self.db_name)
        except influxdb.exceptions.InfluxDBClientError:
            pass
        self.client.create_database(self.db_name)
        data = [{
            "measurement": series,
            "tags": {},
            "time": _time,
            "fields": {
                "value": self.series_values[i],
                }
            }
            for i, series in enumerate(self.series)
            for _time in [
                (self.end_time - datetime.timedelta(minutes=30)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                (self.end_time - datetime.timedelta(minutes=2)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                ]]
        self.assertTrue(self.client.write_points(data))

    def setUp(self):
        # Num datapoints is number of non-zero non-null datapoints
        self.step, self.num_datapoints, self.db_name = 60, 31, 'integration_test'
        self.all_datapoints_num = 61
        self.start_time, self.end_time = (datetime.datetime.utcnow() - datetime.timedelta(hours=1)), \
          datetime.datetime.utcnow()
        self.steps = int(round((int(self.end_time.strftime("%s")) - \
                                int(self.start_time.strftime("%s"))) * 1.0 / self.step)) + 1
        self.config = { 'influxdb': { 'db' : self.db_name,
                                      'log_level': 0,
                                      'fill': 'previous',
                                      },
                        'statsd': { 'host': 'localhost' },
                        'search_index': 'index',
                        }
        self.client = InfluxDBClient(database=self.db_name)
        self.metric_prefix = "integration_test"
        self.nodes = ["leaf_node1", "leaf_node2"]
        self.series1, self.series2 = ".".join([self.metric_prefix, self.nodes[0]]), \
          ".".join([self.metric_prefix, self.nodes[1]])
        self.default_nodes_limit = LOADER_LIMIT
        self.series = [self.series1, self.series2,
                       'integration_test.agg_path.min',
                       'integration_test.agg_path.max',
                       'integration_test.agg_path.last',
                       'integration_test.agg_path.sum',
                       ]
        self.series_values = [randint(1,100) for _ in self.series]
        self.setup_db()
        self.finder = influxgraph.InfluxDBFinder(self.config)

    def tearDown(self):
        self.client.drop_database(self.db_name)
        try:
            os.unlink('index')
        except OSError:
            pass

    def test_configured_deltas(self):
        del self.finder
        config = { 'influxdb': {
            'db' : self.db_name,
            # Set data interval to 1 second for queries
            # of one hour or less
            'deltas' : {3600: 1},
            'log_level': 0,
            'fill': 'previous',
            }}
        finder = influxgraph.InfluxDBFinder(config)
        self.assertTrue(finder.deltas)
        nodes = list(finder.find_nodes(Query(self.series1)))
        paths = [node.path for node in nodes]
        time_info, data = finder.fetch_multi(nodes,
                                             int(self.start_time.strftime("%s")),
                                             int(self.end_time.strftime("%s")))
        self.assertTrue(self.series1 in data,
                        msg="Did not get data for requested series %s - got data for %s" % (
                            self.series1, data.keys(),))
        self.assertTrue(len(data[self.series1]) == 3601,
                        msg="Expected exactly %s data points - got %s instead" % (
                            3601, len(data[self.series1])))

    def test_multi_series_data(self):
        nodes = [influxgraph.classes.leaf.InfluxDBLeafNode(
            path, self.finder.reader)
                 for path in reversed(self.series)]
        _, data = self.finder.fetch_multi(nodes,
                                          int(self.start_time.strftime("%s")),
                                          int(self.end_time.strftime("%s")))
        self.assertEqual(len(data), len(self.series))
        for i, path in enumerate(self.series):
            self.assertTrue(path in data)
            self.assertEqual(data[path][-1], self.series_values[i])

    def test_find_branch(self):
        """Test getting branch of metric path"""
        query = Query('fakeyfakeyfakefake')
        branches = list(self.finder.find_nodes(query))
        self.assertEqual(branches, [],
                         msg="Got branches list %s - wanted empty list" % (
                             branches,))
        query = Query('*')
        prefix = 'branch_test_prefix'
        written_branches = ['branch_node1', 'branch_node2']
        leaf_nodes = ['leaf_node1', 'leaf_node2']
        written_series = [".".join([prefix,
                                    branch, leaf_node,])
                                    for branch in written_branches
                                    for leaf_node in leaf_nodes]
        data = [{
            "measurement": series,
            "tags": {},
            "time": _time,
            "fields": {
                "value": 1,
                }
            }
            for series in written_series
            for _time in [
                (self.end_time - datetime.timedelta(minutes=30)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                (self.end_time - datetime.timedelta(minutes=2)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                ]]
        self.assertTrue(self.client.write_points(data))
        self.finder.build_index()
        query = Query(prefix + '.*')
        # Test getting leaf nodes with wildcard
        query = Query(prefix + '.branch_node*.*')
        _nodes = list(self.finder.find_nodes(query))
        nodes = sorted([n.path for n in _nodes])
        expected = sorted(written_series)
        self.assertEqual(nodes, expected,
                         msg="Got node list %s - wanted %s" % (nodes,
                                                               expected,))

    def test_get_all_series(self):
        """ """
        query = Query('*')
        series = self.finder.get_all_series(cache=True)
        self.assertTrue(len(series) == len(self.series),
                        msg="Got series list %s for root branch query - expected %s" % (
                            series, self.series,))

    def test_find_series(self):
        """Test finding a series by name"""
        nodes = [node.name for node in self.finder.find_nodes(Query(self.series1))
                 if node.is_leaf]
        expected = [self.nodes[0]]
        self.assertEqual(nodes, expected,
                        msg="Got node list %s - wanted %s" % (nodes, expected,))

    def test_find_series_wildcard(self):
        """Test finding metric prefix by wildcard"""
        nodes = [node.name for node in self.finder.find_nodes(Query('*'))]
        self.assertTrue(self.metric_prefix in nodes,
                        msg="Node list does not contain prefix '%s' - %s" % (
                            self.metric_prefix, nodes))

    def test_find_series_glob_expansion(self):
        """Test finding metric prefix by glob expansion"""
        query = Query('{%s}' % (self.metric_prefix))
        nodes = [node.name for node in self.finder.find_nodes(query)]
        self.assertTrue(self.metric_prefix in nodes,
                        msg="Node list does not contain prefix '%s' - %s" % (
                            self.metric_prefix, nodes))

    def test_find_leaf_nodes(self):
        """Test finding leaf nodes by wildcard"""
        prefix = 'branch_test_prefix'
        written_branches = ['branch_node1.sub_branch1.sub_branch2.sub_branch3',
                            'branch_node2.sub_branch11.sub_branch22.sub_branch33']
        leaf_nodes = ['leaf_node1', 'leaf_node2']
        written_series = [".".join([prefix,
                                    branch, leaf_node,])
                                    for branch in written_branches
                                    for leaf_node in leaf_nodes]
        data = [{
            "measurement": series,
            "tags": {},
            "time": _time,
            "fields": {
                "value": 1,
                }
            }
            for series in written_series
            for _time in [
                (self.end_time - datetime.timedelta(minutes=30)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                (self.end_time - datetime.timedelta(minutes=2)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                ]]
        self.assertTrue(self.client.write_points(data))
        self.finder.build_index()
        query = Query(".".join([prefix, "branch_node*",
                                "sub_branch*", "sub_branch*", "sub_branch*",
                                "leaf*"]))
        nodes = list(self.finder.find_nodes(query))
        expected = sorted(leaf_nodes + leaf_nodes)
        found_leaves = sorted([n.name for n in nodes])
        self.assertEqual(found_leaves, expected,
                         msg="Expected leaf node list '%s' - got %s" % (
                             expected, found_leaves))
        for node in nodes:
            self.assertTrue(node.is_leaf,
                            msg="Leaf node %s is not marked as leaf node" % (node))
        nodes = [node.name
                 for node in self.finder.find_nodes(Query("fakeyfakeyfakefake.*"))]
        self.assertEqual(nodes, [],
                         msg="Expected empty leaf node list - got %s" % (nodes,))

    def test_find_branch_nodes(self):
        """Test finding branch nodes by wildcard"""
        prefix = 'branch_test_prefix'
        written_branches = ['branch_node1.sub_branch1.sub_branch2.sub_branch3',
                            'branch_node2.sub_branch11.sub_branch22.sub_branch33']
        leaf_nodes = ['leaf_node1', 'leaf_node2']
        written_series = [".".join([prefix,
                                    branch, leaf_node,])
                                    for branch in written_branches
                                    for leaf_node in leaf_nodes]
        data = [{
            "measurement": series,
            "tags": {},
            "time": _time,
            "fields": {
                "value": 1,
                }
            }
            for series in written_series
            for _time in [
                (self.end_time - datetime.timedelta(minutes=30)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                (self.end_time - datetime.timedelta(minutes=2)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                ]]
        self.assertTrue(self.client.write_points(data))
        self.finder.build_index()
        query = Query(prefix + '.*')
        nodes = list(self.finder.find_nodes(query))
        expected = sorted([b.split('.')[0] for b in written_branches])
        branches = sorted([n.name for n in nodes])
        self.assertEqual(branches, expected,
                         msg="Got branches %s - wanted %s" % (
                             branches, expected,))
        query = Query(prefix + '.branch_node*.*')
        nodes = list(self.finder.find_nodes(query))
        expected = sorted([b.split('.')[1] for b in written_branches])
        branches = sorted([n.name for n in nodes])
        self.assertEqual(branches, expected,
                         msg="Got branches %s - wanted %s" % (
                             branches, expected,))
        query = Query(prefix + '.branch_node*.sub_branch*.*')
        nodes = list(self.finder.find_nodes(query))
        expected = sorted([b.split('.')[2] for b in written_branches])
        branches = sorted([n.name for n in nodes])
        self.assertEqual(branches, expected,
                         msg="Got branches list %s - wanted %s" % (
                             branches, expected,))

    def test_multi_fetch_data(self):
        """Test fetching data for a single series by name"""
        nodes = list(self.finder.find_nodes(Query(self.series1)))
        time_info, data = self.finder.fetch_multi(nodes,
                                                  int(self.start_time.strftime("%s")),
                                                  int(self.end_time.strftime("%s")))
        self.assertTrue(self.series1 in data,
                        msg="Did not get data for requested series %s - got data for %s" % (
                            self.series1, data.keys(),))
        self.assertEqual(time_info,
                         (int(self.start_time.strftime("%s")),
                          int(self.end_time.strftime("%s")),
                         self.step),
                         msg="Time info and step do not match our requested values")
        datapoints = [v for v in data[self.series1] if v]
        self.assertTrue(len(datapoints) == self.num_datapoints,
                        msg="Expected %s datapoints - got %s" % (
                            self.num_datapoints, len(datapoints),))
        self.assertTrue(datapoints[-1] == self.series_values[0])

    def test_single_fetch_data(self):
        """Test single fetch data for a series by name"""
        node = list(self.finder.find_nodes(Query(self.series1)))[0]
        time_info, data = node.reader.fetch(int(self.start_time.strftime("%s")),
                                            int(self.end_time.strftime("%s")))
        self.assertTrue(self.steps == len(data),
                        msg="Expected %s datapoints, got %s instead" % (
                            self.steps, len(data),))
        datapoints = [v for v in data if v]
        self.assertEqual(len(datapoints), self.num_datapoints,
                        msg="Expected %s datapoints - got %s" % (
                            self.num_datapoints, len(datapoints),))

    def test_single_fetch_memcache_integration(self):
        self.config['influxdb']['memcache'] = {'host': 'localhost'}
        del self.config['search_index']
        self.finder = influxgraph.InfluxDBFinder(self.config)
        node = list(self.finder.find_nodes(Query(self.series1)))[0]
        aggregation_func = get_aggregation_func(
            node.path, self.finder.aggregation_functions)
        memcache_key = gen_memcache_key(int(self.start_time.strftime("%s")),
                                        int(self.end_time.strftime("%s")),
                                        aggregation_func, [node.path])
        self.finder.memcache.delete(memcache_key)
        node.reader.fetch(int(self.start_time.strftime("%s")),
                          int(self.end_time.strftime("%s")))
        self.assertTrue(node.reader.memcache.get(memcache_key),
                        msg="Expected data for %s to be in memcache after a fetch" % (
                            node.path,))
        time_info, data = node.reader.fetch(int(self.start_time.strftime("%s")),
                                            int(self.end_time.strftime("%s")))
        datapoints = [v for v in data if v]
        self.assertTrue(self.steps == len(data),
                        msg="Expected %s datapoints, got %s instead" % (
                            self.steps, len(data),))

    def test_multi_fetch_data_multi_series(self):
        """Test fetching data for multiple series by name"""
        nodes = list(self.finder.find_nodes(Query(self.metric_prefix + ".leaf*")))
        time_info, data = self.finder.fetch_multi(nodes,
                                                  int(self.start_time.strftime("%s")),
                                                  int(self.end_time.strftime("%s")))
        self.assertTrue(self.series1 in data and self.series2 in data,
                        msg="Did not get data for requested series %s and %s - got data for %s" % (
                            self.series1, self.series2, data.keys(),))
        self.assertEqual(time_info,
                         (int(self.start_time.strftime("%s")),
                          int(self.end_time.strftime("%s")),
                         self.step),
                         msg="Time info and step do not match our requested values")
        for i, series in enumerate([self.series1, self.series2]):
            self.assertTrue(self.steps == len(data[series]),
                            msg="Expected %s datapoints, got %s instead" % (
                                self.steps, len(data[series]),))
            datapoints = [v for v in data[series] if v]
            self.assertTrue(len(datapoints) == self.num_datapoints,
                            msg="Expected %s datapoints for series %s - got %s" % (
                                self.num_datapoints, series, len(datapoints),))
            self.assertTrue(datapoints[-1] == self.series_values[i])

    def test_get_non_existant_series(self):
        """Test single fetch data for a series by name"""
        path = 'fake_path'
        reader = influxgraph.InfluxDBReader(INFLClient(
            'localhost', db=self.db_name), path)
        time_info, data = reader.fetch(
            int(self.start_time.strftime("%s")),
            int(self.end_time.strftime("%s")))
        self.assertFalse(
            data,
            msg="Expected no data for non-existant series %s - got %s" % (
                path, data,))

    def test_multi_fetch_non_existant_series(self):
        """Test single fetch data for a series by name"""
        path1, path2 = 'fake_path1', 'fake_path2'
        reader1 = influxgraph.InfluxDBReader(InfluxDBClient(
            database=self.db_name), path1)
        reader2 = influxgraph.InfluxDBReader(InfluxDBClient(
            database=self.db_name), path2)
        nodes = [influxgraph.classes.leaf.InfluxDBLeafNode(path1, reader1),
                 influxgraph.classes.leaf.InfluxDBLeafNode(path2, reader2)]
        time_info, data = self.finder.fetch_multi(nodes,
                                                  int(self.start_time.strftime("%s")),
                                                  int(self.end_time.strftime("%s")))
        for metric_name in data:
            self.assertFalse(data[metric_name],
                             msg="Expected no data for non-existant series %s - got %s" % (
                                 metric_name, data,))
        fake_nodes = list(self.finder.find_nodes(Query('fake_pathy_path')))
        time_info, data = self.finder.fetch_multi(fake_nodes,
                                                  int(self.start_time.strftime("%s")),
                                                  int(self.end_time.strftime("%s")))
        self.assertFalse(data)

    def test_multi_fetch_data_multi_series_configured_aggregation_functions(self):
        """Test fetching data for multiple series with aggregation functions configured"""
        nodes = list(self.finder.find_nodes(Query(self.metric_prefix + ".agg_path.*")))
        paths = [node.path for node in nodes]
        aggregation_funcs = sorted(list(set(influxgraph.utils.get_aggregation_func(
            path, self.finder.aggregation_functions) for path in paths)))
        expected = sorted(DEFAULT_AGGREGATIONS.values())
        self.assertEqual(expected, aggregation_funcs,
                         msg="Expected aggregation functions %s for paths %s - got %s" % (
                             expected, paths, aggregation_funcs))
        time_info, data = self.finder.fetch_multi(nodes,
                                                  int(self.start_time.strftime("%s")),
                                                  int(self.end_time.strftime("%s")))
        self.assertTrue(nodes[0].path in data,
                        msg="Did not get data for requested series %s - got data for %s" % (
                            nodes[0].path, data.keys(),))
        for i, suffix in enumerate(['min', 'max', 'last', 'sum']):
            series = self.metric_prefix + ".agg_path.%s" % (suffix,)
            nodes = list(self.finder.find_nodes(Query(series)))
            time_info, data = self.finder.fetch_multi(nodes,
                                                      int(self.start_time.strftime("%s")),
                                                      int(self.end_time.strftime("%s")))
            self.assertTrue(series in data,
                            msg="Did not get data for requested series %s - got data for %s" % (
                                series, data.keys(),))
            self.assertTrue(data[series][-1] == self.series_values[i-4])

    def test_memcache_configuration_off_by_default(self):
        self.assertFalse(self.finder.memcache)

    def test_series_loader(self):
        query = Query('*')
        loader_memcache_key = influxgraph.utils.gen_memcache_pattern_key("_".join([
            query.pattern, str(self.default_nodes_limit), str(0)]))
        del self.finder
        _loader_interval = 2
        config = { 'influxdb' : { 'host' : 'localhost',
                                  'port' : 8086,
                                  'user' : 'root',
                                  'pass' : 'root',
                                  'db' : self.db_name,
                                  'series_loader_interval': _loader_interval,
                                  'memcache' : { 'host': 'localhost',
                                                 'ttl' : 60,
                                                 'max_value': 20,
                                                 },
                                  'log_level': 0,
                                  'fill': 'previous',
                                  },}
        try:
            _memcache = memcache.Client([config['influxdb']['memcache']['host']])
            _memcache.delete(SERIES_LOADER_MUTEX_KEY)
        except NameError:
            pass
        finder = influxgraph.InfluxDBFinder(config)
        time.sleep(_loader_interval/2.0)
        # if finder.memcache:
        #     self.assertTrue(finder.memcache.get(SERIES_LOADER_MUTEX_KEY))
        self.assertTrue(finder.memcache)
        self.assertEqual(finder.memcache_ttl, 60,
                         msg="Configured TTL of %s sec, got %s sec TTL instead" % (
                             60, finder.memcache_ttl,))
        self.assertEqual(finder.memcache.server_max_value_length, 1024**2*20,
                         msg="Configured max value of %s MB, got %s instead" % (
                             1024**2*20, finder.memcache.server_max_value_length,))
        # Give series loader more than long enough to finish
        time.sleep(_loader_interval + 2)
        if finder.memcache:
            self.assertTrue(finder.memcache.get(loader_memcache_key))
        del finder
        config['influxdb']['loader_startup_block'] = False
        finder = influxgraph.InfluxDBFinder(config)
        try:
            self.assertTrue(_SERIES_LOADER_LOCK.acquire(block=False))
        except:
            pass
        else:
            _SERIES_LOADER_LOCK.release()

    def test_reindex(self):
        del self.finder
        _reindex_interval = 2
        config = { 'influxdb' : { 'host' : 'localhost',
                                  'port' : 8086,
                                  'user' : 'root',
                                  'pass' : 'root',
                                  'db' : self.db_name,
                                  'reindex_interval': _reindex_interval,
                                  'memcache' : { 'host': 'localhost',
                                                 'ttl' : 60,
                                                 'max_value': 20,
                                                 },
                                  'log_level': 0,
                                  'fill': 'previous',
                                  },}
        finder = influxgraph.InfluxDBFinder(config)
        time.sleep(_reindex_interval)
        self.assertTrue(finder.index)

    def test_get_series_pagination(self):
        query, limit = Query('*'), 5
        self.finder.loader_limit = limit
        series = self.finder.get_all_series(
            query)
        self.assertTrue(len(series) == len(self.series),
                        msg="Did not get data for all series with page limit %s" % (
                            limit,))
        query, limit = Query('*'), 10
        self.finder.loader_limit = limit
        series = self.finder.get_all_series(
            query)
        self.assertTrue(len(series) == len(self.series),
                        msg="Did not get data for all series with page limit %s" % (
                            limit,))

    def test_memcache_integration(self):
        del self.finder
        config = { 'influxdb' : { 'host' : 'localhost',
                                  'port' : 8086,
                                  'user' : 'root',
                                  'pass' : 'root',
                                  'db' : self.db_name,
                                  'memcache' : { 'host': 'localhost',
                                                 'ttl' : 60,
                                                 'max_value' : 20},
                                  'log_level': 0,
                                  'fill': 'previous',
                                  },}
        try:
            _memcache = memcache.Client([config['influxdb']['memcache']['host']])
        except NameError:
            # No memcache module - no memcache integration tests
            return
        query, limit = Query('*'), 1
        memcache_keys = [influxgraph.utils.gen_memcache_pattern_key("_".join([
            query.pattern, str(limit), str(offset)]))
                         for offset in range(len(self.series))]
        for _key in memcache_keys:
            _memcache.delete(_key)
        _memcache.delete(SERIES_LOADER_MUTEX_KEY)
        finder = influxgraph.InfluxDBFinder(config)
        self.assertTrue(finder.memcache)
        self.assertEqual(finder.memcache_ttl, 60,
                         msg="Configured TTL of %s sec, got %s sec TTL instead" % (
                             60, finder.memcache_ttl,))
        self.assertEqual(finder.memcache.server_max_value_length, 1024**2*20,
                         msg="Configured max value of %s MB, got %s instead" % (
                             1024**2*20, finder.memcache.server_max_value_length,))
        finder.loader_limit = limit
        node_names = list(finder.get_all_series())
        self.assertTrue(self.series[0] in node_names,
                        msg="Node list does not contain prefix '%s' - %s" % (
                            self.metric_prefix, node_names))
        if finder.memcache:
            for memcache_key in memcache_keys:
                self.assertTrue(finder.memcache.get(memcache_key),
                                msg="No memcache data for key %s" % (memcache_key,))
        limit = 1
        finder.loader_limit = limit
        nodes = sorted(list(finder.get_all_series()))
        expected = sorted(self.series)
        self.assertEqual(nodes, expected,
                         msg="Did not get correct series list - "
                         "wanted %s series, got %s" % (
                             len(expected), len(nodes),))
        nodes = list(finder.find_nodes(Query(self.series1)))
        paths = [node.path for node in nodes]
        self.assertEqual(paths, [self.series1],
                         msg="Did not get requested node %s, got %s" % (
                             self.series1, paths,))
        time_info, data = finder.fetch_multi(nodes,
                                             int(self.start_time.strftime("%s")),
                                             int(self.end_time.strftime("%s")))
        self.assertTrue(self.series1 in data,
                        msg="Did not get data for requested series %s - got data for %s" % (
                            self.series1, data.keys(),))
        time_info, data_cached = finder.fetch_multi(nodes,
                                                    int(self.start_time.strftime("%s")),
                                                    int(self.end_time.strftime("%s")))
        self.assertEqual(len(data[self.series1]), len(data_cached[self.series1]),
                         msg="Cached data does not match uncached data for series %s" % (
                             self.series1))
        aggregation_func = list(set(influxgraph.utils.get_aggregation_func(
            path, finder.aggregation_functions) for path in paths))[0]
        memcache_key = influxgraph.utils.gen_memcache_key(
            int(self.start_time.strftime("%s")), int(self.end_time.strftime("%s")),
            aggregation_func, paths)
        if finder.memcache:
            self.assertTrue(finder.memcache.get(memcache_key),
                            msg="Got no memcache data for query %s with key %s" % (
                                query, memcache_key,))
        time_info, reader_data = nodes[0].reader.fetch(int(self.start_time.strftime("%s")),
                                                       int(self.end_time.strftime("%s")))
        self.assertEqual(len(data[self.series1]), len(reader_data),
                         msg="Reader cached data does not match finder cached data"
                         " for series %s" % (self.series1,))
        fake_nodes = [influxgraph.classes.leaf.InfluxDBLeafNode('fake.node', finder.reader)]
        fake_nodes_memcache_key = influxgraph.utils.gen_memcache_key(
            int(self.start_time.strftime("%s")), int(self.end_time.strftime("%s")),
            aggregation_func, [n.path for n in fake_nodes])
        time_info, data = finder.fetch_multi(
            fake_nodes, int(self.start_time.strftime("%s")),
            int(self.end_time.strftime("%s")))
        self.assertTrue(fake_nodes[0].path in data)
        self.assertFalse(finder.memcache.get(fake_nodes_memcache_key))

    def test_reader_memcache_integration(self):
        reader = influxgraph.InfluxDBReader(
            InfluxDBClient(database=self.db_name),
            self.series1,
            memcache=influxgraph.utils.make_memcache_client('localhost'))
        self.assertTrue(reader.fetch(int(self.start_time.strftime("%s")),
                                     int(self.end_time.strftime("%s"))))

    def test_memcache_default_config_values(self):
        del self.finder
        config = { 'influxdb' : { 'host' : 'localhost',
                                  'port' : 8086,
                                  'user' : 'root',
                                  'pass' : 'root',
                                  'db' : self.db_name,
                                  'memcache' : { 'host': 'localhost'},
                                  'log_level': 0,
                                  'fill': 'previous',
                                  },}
        finder = influxgraph.InfluxDBFinder(config)
        self.assertTrue(finder.memcache)
        self.assertEqual(finder.memcache_ttl, MEMCACHE_SERIES_DEFAULT_TTL,
                         msg="Default TTL should be %s sec, got %s sec TTL instead" % (
                             MEMCACHE_SERIES_DEFAULT_TTL, finder.memcache_ttl,))
        self.assertEqual(finder.memcache.server_max_value_length, 1024**2*1,
                         msg="Default max value should be 1 MB, got %s instead" % (
                             1024**2*finder.memcache.server_max_value_length,))

    def test_named_branch_query(self):
        query = Query(self.metric_prefix)
        nodes = list(self.finder.find_nodes(query))
        node_names = [n.name for n in nodes]
        self.assertEqual(node_names, [self.metric_prefix],
                         msg="Expected node names %s, got %s" % (
                             [self.metric_prefix], node_names,))
        self.assertFalse(nodes[0].is_leaf,
                         msg="Root branch node incorrectly marked as leaf node")

    def test_parent_branch_series(self):
        prefix = 'branch_test_prefix'
        written_branches = ['branch_node1.sub_branch1.sub_branch2.sub_branch3',
                            'branch_node2.sub_branch11.sub_branch22.sub_branch33']
        leaf_nodes = ['leaf_node1', 'leaf_node2']
        written_series = [".".join([prefix,
                                    branch, leaf_node,])
                                    for branch in written_branches
                                    for leaf_node in leaf_nodes]
        data = [{
            "measurement": series,
            "tags": {},
            "time": _time,
            "fields": {
                "value": 1,
                }
            }
            for series in written_series
            for _time in [
                (self.end_time - datetime.timedelta(minutes=30)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                (self.end_time - datetime.timedelta(minutes=2)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                ]]
        self.assertTrue(self.client.write_points(data))
        del self.finder
        config = { 'influxdb' : { 'host' : 'localhost',
                                  'port' : 8086,
                                  'user' : 'root',
                                  'pass' : 'root',
                                  'db' : self.db_name,
                                  'memcache' : { 'host': 'localhost',
                                                 'ttl' : 60,
                                                 'max_value' : 20},
                                  'log_level': 0,
                                  'fill': 'previous',
                                  },}
        try:
            _memcache = memcache.Client([config['influxdb']['memcache']['host']])
            memcache_key = influxgraph.utils.gen_memcache_pattern_key("_".join([
                '*', str(self.default_nodes_limit), str(0)]))
            _memcache.delete(memcache_key)
            _memcache.delete(SERIES_LOADER_MUTEX_KEY)
        except NameError:
            pass
        finder = influxgraph.InfluxDBFinder(config)
        time.sleep(1)
        query = Query(prefix + '.*.*.*.*.' + leaf_nodes[0])
        nodes = list(finder.find_nodes(query))
        self.assertTrue(len(nodes)==len(leaf_nodes),
                        msg="Did not get all leaf nodes for wildcard query")
        for node in nodes:
            self.assertTrue(node.is_leaf,
                            msg="Leaf node %s from query %s not marked as leaf" % (
                                node.name, query.pattern,))
        query = Query(prefix)
        nodes = list(finder.find_nodes(query))
        expected = [prefix]
        branches = sorted([n.name for n in nodes])
        self.assertEqual(expected, branches,
                         msg="Expected branches %s, got %s" % (expected, branches,))
        self.assertFalse(nodes[0].is_leaf,
                         msg="Root branch node marked as leaf")
        query = Query(prefix + '.branch_node*.sub_branch*.*')
        nodes = list(finder.find_nodes(query))
        expected = sorted([b.split('.')[2] for b in written_branches])
        branches = sorted([n.name for n in nodes])
        self.assertEqual(branches, expected,
                         msg="Got branches list %s - wanted %s" % (
                             branches, expected,))
        query = Query(".".join([prefix, "branch_node*",
                                "sub_branch*", "sub_branch*", "sub_branch*",
                                "leaf*"]))
        nodes = list(finder.find_nodes(query))
        expected = sorted(leaf_nodes + leaf_nodes)
        found_leaves = sorted([n.name for n in nodes])
        self.assertEqual(found_leaves, expected,
                         msg="Expected leaf node list '%s' - got %s" % (
                             expected, found_leaves))
        for node in nodes:
            self.assertTrue(node.is_leaf,
                            msg="Leaf node %s is not marked as leaf node" % (node))
        query = Query(".".join([prefix, "branch_node*",
                                "sub_branch*", "sub_branch*", "sub_branch*",
                                "{%s}" % (",".join(leaf_nodes),)]))
        nodes = list(finder.find_nodes(query))
        expected = sorted(leaf_nodes + leaf_nodes)
        found_leaves = sorted([n.name for n in nodes])
        self.assertEqual(found_leaves, expected,
                         msg="Expected leaf node list '%s' - got %s" % (
                             expected, found_leaves))

    def test_retention_policies(self):
        del self.finder
        data_point_value = 5
        retention_policies = {60: 'default', 600: '10m', 1800: '30m'}
        config = { 'influxdb' : { 'host' : 'localhost',
                                  'port' : 8086,
                                  'user' : 'root',
                                  'pass' : 'root',
                                  'db' : self.db_name,
                                  'deltas' : {
                                      1800: 600,
                                      3600: 1800,
                                              },
                                  'retention_policies' : retention_policies,
                                  'log_level': 0,
                                  'fill': 'previous',
                                  }}
        self.client.create_retention_policy('10m', '1d', 1, database=self.db_name, default=False)
        self.client.create_retention_policy('30m', '1d', 1, database=self.db_name, default=False)
        write_data = [{
            "measurement": series,
            "tags": {},
            "time": _time,
            "fields": {
                "value": data_point_value,
                }
            }
            for series in self.series
            for _time in [
                (self.start_time - datetime.timedelta(minutes=60)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                (self.start_time - datetime.timedelta(minutes=50)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                (self.start_time - datetime.timedelta(minutes=40)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                (self.start_time - datetime.timedelta(minutes=30)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                (self.start_time - datetime.timedelta(minutes=20)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                (self.start_time - datetime.timedelta(minutes=10)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                (self.start_time).strftime("%Y-%m-%dT%H:%M:%SZ"),
                ]]
        self.assertTrue(self.client.write_points(write_data, retention_policy='10m'))
        self.assertTrue(self.client.write_points(write_data, retention_policy='30m'))
        finder = influxgraph.InfluxDBFinder(config)
        time.sleep(1)
        nodes = list(finder.find_nodes(Query(self.series1)))
        paths = [node.path for node in nodes]
        self.assertEqual(paths, [self.series1],
                         msg="Did not get requested node %s, got %s" % (
                             self.series1, paths,))
        time_info, data = finder.fetch_multi(nodes,
                                             int((self.start_time - datetime.timedelta(minutes=29)).strftime("%s")),
                                             int(self.start_time.strftime("%s")))
        self.assertTrue(self.series1 in data,
                        msg="Did not get data for requested series %s - got data for %s" % (
                            self.series1, data.keys(),))
        data_points = [v for v in data[self.series1] if v]
        self.assertTrue(len(data_points)==3,
                        msg="Three datapoints in interval in retention policy, got %s from query" % (
                            len(data_points)))
        self.assertTrue(data_points[0]==data_point_value)
        time_info, data = finder.fetch_multi(nodes,
                                             int((self.start_time - datetime.timedelta(minutes=31)).strftime("%s")),
                                             int(self.start_time.strftime("%s")))
        self.assertTrue(self.series1 in data,
                        msg="Did not get data for requested series %s - got data for %s" % (
                            self.series1, data.keys(),))
        data_points = [v for v in data[self.series1] if v]
        self.assertTrue(data_points[0]==data_point_value)
        self.assertTrue(len(data_points)==2,
                        msg="Two datapoints in interval in retention policy, got %s from query" % (
                            len(data_points)))

    def test_index_save_load_failure(self):
        del self.finder.index
        del self.finder
        bad_index_path = 'bad_index'
        try:
            os.unlink(bad_index_path)
        except OSError:
            pass
        finally:
            open(bad_index_path, 'wt').close()
        # Permission errors on all operations
        mask = 0000 if sys.version_info <= (2,) else 0o000
        os.chmod(bad_index_path, mask)
        config = { 'influxdb': { 'host' : 'localhost',
                                 'port' : 8086,
                                 'memcache' : {'host': 'localhost',},
                                 'user' : 'root',
                                 'pass' : 'root',
                                 'db' : self.db_name,
                                 'log_level': 0,
                                 'fill': 'previous',
                                 },
                        'statsd': {'host': 'localhost' },
                        'search_index': bad_index_path,
                        }
        finder = influxgraph.InfluxDBFinder(config)
        del finder
        mask = int('0600') if sys.version_info <= (2,) else 0o600
        os.chmod(bad_index_path, mask)
        # Corrupt data in index file
        with open(bad_index_path, 'wt') as index_fh:
            index_fh.write('fasdfa}\n')
        finder = influxgraph.InfluxDBFinder(config)
        self.assertTrue(finder.index)
        try:
            os.unlink(bad_index_path)
        except OSError:
            pass

    def test_loader_limit(self):
        del self.finder
        config = { 'influxdb' : { 'host' : 'localhost',
                                  'port' : 8086,
                                  'user' : 'root',
                                  'pass' : 'root',
                                  'db' : self.db_name,
                                  'loader_limit': 1,
                                  'series_loader_interval': 1,
                                  'log_level': 0,
                                  'memcache' : { 'host': 'localhost',
                                                 'ttl' : 60,
                                                 },
                                                 }}
        loader_memcache_key = influxgraph.utils.gen_memcache_pattern_key("_".join([
            '*', str(config['influxdb']['loader_limit']), str(0)]))
        try:
            _memcache = memcache.Client([config['influxdb']['memcache']['host']])
            _memcache.delete(loader_memcache_key)
            _memcache.delete(SERIES_LOADER_MUTEX_KEY)
        except NameError:
            pass
        self.finder = influxgraph.InfluxDBFinder(config)
        self.assertEqual(self.finder.loader_limit, config['influxdb']['loader_limit'])
        time.sleep(config['influxdb']['series_loader_interval'] * 2)
        if self.finder.memcache:
            self.assertTrue(self.finder.memcache.get(loader_memcache_key))
        config['influxdb']['loader_limit'] = 'bad_limit'
        self.assertRaises(Exception, influxgraph.InfluxDBFinder, config)

    def test_fill_param_config(self):
        self.config['influxdb']['fill'] = 0
        self.finder = influxgraph.InfluxDBFinder(self.config)
        self.assertEqual(self.finder.fill_param, self.config['influxdb']['fill'])
        nodes = list(self.finder.find_nodes(Query(self.series1)))
        time_info, data = self.finder.fetch_multi(nodes,
                                                  int(self.start_time.strftime("%s")),
                                                  int(self.end_time.strftime("%s")))
        self.assertTrue(self.series1 in data,
                        msg="Did not get data for requested series %s - got data for %s" % (
                            self.series1, data.keys(),))
        self.assertEqual(time_info,
                         (int(self.start_time.strftime("%s")),
                          int(self.end_time.strftime("%s")),
                         self.step),
                         msg="Time info and step do not match our requested values")
        self.assertTrue(len(data[self.series1]) == self.all_datapoints_num,
                        msg="Expected %s datapoints - got %s" % (
                            self.all_datapoints_num, len(data[self.series1]),))
        self.assertTrue(data[self.series1][-1] == self.config['influxdb']['fill'])

    def test_multi_finder_index_build(self):
        """Test index build lock with multiple finders"""
        num_finders = 50
        fh = open(FILE_LOCK, 'w')
        del self.finder
        self.config['influxdb']['reindex_interval'] = 0
        finders = []
        for _ in range(num_finders):
            _finder = influxgraph.InfluxDBFinder(self.config)
            time.sleep(.001)
            finders.append(_finder)
        try:
            self.assertRaises(IOError, fcntl.flock,
                              fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except AssertionError:
            i = 0
            while i < 50:
                i += 1
                fcntl.flock(fh, fcntl.LOCK_UN)
                try:
                    self.assertRaises(IOError, fcntl.flock,
                                      fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except AssertionError:
                    continue
        finally:
            fcntl.flock(fh, fcntl.LOCK_UN)
            fh.close()
            os.unlink(FILE_LOCK)

if __name__ == '__main__':
    unittest.main()
