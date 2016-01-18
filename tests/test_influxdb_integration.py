import os
import unittest
from influxdb import InfluxDBClient
import influxdb.exceptions
import graphite_influxdb
import graphite_influxdb.utils
from graphite_influxdb.utils import Query
import datetime
import time

os.environ['TZ'] = 'UTC'

class GraphiteInfluxdbIntegrationTestCase(unittest.TestCase):

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
                "value": 1,
                }
            }
            for series in self.series
            for _time in [
                (self.end_time - datetime.timedelta(minutes=30)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                (self.end_time - datetime.timedelta(minutes=2)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                ]]
        self.assertTrue(self.client.write_points(data))

    def setUp(self):
        self.step, self.num_datapoints, self.db_name = 60, 2, 'integration_test'
        self.start_time, self.end_time = (datetime.datetime.utcnow() - datetime.timedelta(hours=1)), \
          datetime.datetime.utcnow()
        self.steps = int(round((int(self.end_time.strftime("%s")) - \
                                int(self.start_time.strftime("%s"))) * 1.0 / self.step)) + 1
        self.client = InfluxDBClient(database=self.db_name)
        self.config = { 'influxdb' : { 'host' : 'localhost',
                                       'port' : 8086,
                                       'user' : 'root',
                                       'pass' : 'root',
                                       'db' : self.db_name,
                                       'log_level' : 'debug',
                                       },
                        'statsd' : { 'host': 'localhost' },
                        }
        self.finder = graphite_influxdb.InfluxdbFinder(self.config)
        self.metric_prefix = "integration_test"
        self.nodes = ["leaf_node1", "leaf_node2"]
        self.series1, self.series2 = ".".join([self.metric_prefix, self.nodes[0]]), \
          ".".join([self.metric_prefix, self.nodes[1]])
        self.series = [self.series1, self.series2,
                       'integration_test.agg_path.min',
                       'integration_test.agg_path.max',
                       'integration_test.agg_path.last',
                       'integration_test.agg_path.sum',
                       ]
        self.setup_db()

    def tearDown(self):
        self.client.drop_database(self.db_name)

    def test_configured_deltas(self):
        del self.finder
        config = { 'influxdb' : { 'host' : 'localhost',
                                  'port' : 8086,
                                  'user' : 'root',
                                  'pass' : 'root',
                                  'db' : self.db_name,
                                  'log_level' : 'debug',
                                  # Set data interval to 1 second for queries
                                  # of one hour or less
                                  'deltas' : { 3600:1 },
                                  },}
        finder = graphite_influxdb.InfluxdbFinder(config)
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

    def test_get_branch(self):
        """Test getting branch of metric path"""
        query = Query('fakeyfakeyfakefake')
        series = self.finder.get_series(query)
        branches = [b for b in [self.finder.get_branch(
            path, query, set())
            for path in series] if b]
        self.assertEqual(branches, [],
                         msg="Got branches list %s - wanted empty list" % (
                             branches,))
        query = Query('*')
        series = list(self.finder.get_series(query))
        seen_branches = set()
        # import ipdb; ipdb.set_trace()
        branches = [b for b in [self.finder.get_branch(path, query, seen_branches)
                                for path in series] if b]
        expected = [self.metric_prefix]
        self.assertEqual(branches, expected,
                         msg="Got branches list %s - wanted %s" % (branches,
                                                                   expected,))
        prefix = 'branch_test_prefix'
        written_branches = ['branch_node1', 'branch_node2']
        written_series = [".".join([prefix,
                            branch, 'leaf_node',])
                            for branch in written_branches]
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
        query = Query(prefix + '.*')
        series = list(self.finder.get_series(query))
        seen_branches = set()
        # import ipdb; ipdb.set_trace()
        branches = sorted([b for b in [self.finder.get_branch(
            path, query, seen_branches)
            for path in series] if b])
        expected = sorted([".".join([prefix, b]) for b in written_branches])
        self.assertEqual(branches, expected,
                         msg="Got branches list %s - wanted %s" % (branches,
                                                                  expected,))

    def test_get_all_series(self):
        """ """
        query = Query('*')
        series = self.finder.get_all_series(query, cache=True, limit=1)
        self.assertTrue(len(series) == len(self.series))

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
        # import ipdb; ipdb.set_trace()
        self.assertTrue(self.metric_prefix in nodes,
                        msg="Node list does not contain prefix '%s' - %s" % (
                            self.metric_prefix, nodes))

    def test_find_leaf_nodes(self):
        """Test finding leaf nodes by wildcard"""
        # import ipdb; ipdb.set_trace()
        nodes = [node.name
                 for node in self.finder.find_nodes(Query(self.metric_prefix + ".leaf*"))]
        expected = self.nodes
        self.assertEqual(nodes, expected,
                         msg="Expected leaf node list '%s' - got %s" % (expected, nodes))
        nodes = [node.name
                 for node in self.finder.find_nodes(Query("fakeyfakeyfakefake.*"))]
        self.assertEqual(nodes, [],
                         msg="Expected empty leaf node list - got %s" % (nodes,))

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

    def test_single_fetch_data(self):
        """Test single fetch data for a series by name"""
        node = list(self.finder.find_nodes(Query(self.series1)))[0]
        time_info, data = node.reader.fetch(int(self.start_time.strftime("%s")),
                                            int(self.end_time.strftime("%s")))
        self.assertTrue(self.steps == len(data),
                        msg="Expected %s datapoints, got %s instead" % (
                            self.steps, len(data),))
        datapoints = [v for v in data if v]
        self.assertTrue(len(datapoints) == self.num_datapoints,
                        msg="Expected %s datapoints - got %s" % (
                            self.num_datapoints, len(datapoints),))
        
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
        for series in [self.series1, self.series2]:
            self.assertTrue(self.steps == len(data[series]),
                            msg="Expected %s datapoints, got %s instead" % (
                            self.steps, len(data[series]),))
            datapoints = [v for v in data[series] if v]
            self.assertTrue(len(datapoints) == self.num_datapoints,
                            msg="Expected %s datapoints for series %s - got %s" % (
                                self.num_datapoints, series, len(datapoints),))

    def test_get_non_existant_series(self):
        """Test single fetch data for a series by name"""
        path = 'fake_path'
        reader = graphite_influxdb.InfluxdbReader(InfluxDBClient(
            database=self.db_name), path, graphite_influxdb.utils.NullStatsd())
        time_info, data = reader.fetch(int(self.start_time.strftime("%s")),
                                            int(self.end_time.strftime("%s")))
        self.assertFalse(data,
                         msg="Expected no data for non-existant series %s - got %s" % (
                             path, data,))

    def test_multi_fetch_non_existant_series(self):
        """Test single fetch data for a series by name"""
        path1, path2 = 'fake_path1', 'fake_path2'
        reader1 = graphite_influxdb.InfluxdbReader(InfluxDBClient(
            database=self.db_name), path1, graphite_influxdb.utils.NullStatsd())
        reader2 = graphite_influxdb.InfluxdbReader(InfluxDBClient(
            database=self.db_name), path2, graphite_influxdb.utils.NullStatsd())
        nodes = [reader1, reader2]
        time_info, data = self.finder.fetch_multi(nodes,
                                                  int(self.start_time.strftime("%s")),
                                                  int(self.end_time.strftime("%s")))
        for metric_name in data:
            self.assertFalse(data[metric_name],
                             msg="Expected no data for non-existant series %s - got %s" % (
                                 metric_name, data,))

    def test_multi_fetch_data_multi_series_configured_aggregation_functions(self):
        """Test fetching data for multiple series with aggregation functions configured"""
        nodes = list(self.finder.find_nodes(Query(self.metric_prefix + ".agg_path.*")))
        paths = [node.path for node in nodes]
        aggregation_funcs = sorted(list(set(graphite_influxdb.utils.get_aggregation_func(
            path, self.finder.aggregation_functions) for path in paths)))
        expected = sorted(graphite_influxdb.utils.DEFAULT_AGGREGATIONS.values())
        self.assertEqual(expected, aggregation_funcs,
                         msg="Expected aggregation functions %s for paths %s - got %s" % (
                             expected, paths, aggregation_funcs))
        time_info, data = self.finder.fetch_multi(nodes,
                                                  int(self.start_time.strftime("%s")),
                                                  int(self.end_time.strftime("%s")))
        self.assertTrue(nodes[0].path in data,
                        msg="Did not get data for requested series %s - got data for %s" % (
                            nodes[0].path, data.keys(),))
        for suffix in ['min', 'max', 'last', 'sum']:
            series = self.metric_prefix + ".agg_path.%s" % (suffix,)
            nodes = list(self.finder.find_nodes(Query(series)))
            time_info, data = self.finder.fetch_multi(nodes,
                                                      int(self.start_time.strftime("%s")),
                                                      int(self.end_time.strftime("%s")))
            self.assertTrue(series in data,
                            msg="Did not get data for requested series %s - got data for %s" % (
                                series, data.keys(),))

    def test_memcache_configuration_off_by_default(self):
        self.assertFalse(self.finder.memcache_host)

    def test_memcache_integration(self):
        del self.finder
        config = { 'influxdb' : { 'host' : 'localhost',
                                  'port' : 8086,
                                  'user' : 'root',
                                  'pass' : 'root',
                                  'db' : self.db_name,
                                  'log_level' : 'debug',
                                  'memcache' : { 'host': 'localhost',
                                                 'ttl' : 60,
                                                 'max_value' : 20},
                                  },}
        finder = graphite_influxdb.InfluxdbFinder(config)
        self.assertTrue(finder.memcache_host)
        self.assertEqual(finder.memcache_ttl, 60,
                         msg="Configured TTL of %s sec, got %s sec TTL instead" % (
                             60, finder.memcache_ttl,))
        self.assertEqual(finder.memcache_max_value, 20,
                         msg="Configured max value of %s MB, got %s instead" % (
                             20, finder.memcache_max_value,))
        time.sleep(1)
        query = Query('*')
        nodes = [node.name for node in finder.find_nodes(query)]
        self.assertTrue(self.metric_prefix in nodes,
                        msg="Node list does not contain prefix '%s' - %s" % (
                            self.metric_prefix, nodes))
        self.assertTrue(finder.memcache.get(
            graphite_influxdb.utils.gen_memcache_pattern_key(query.pattern)),
            msg="No memcache data for query %s" % (query.pattern,))
        nodes = list(finder.find_nodes(Query(self.series1)))
        paths = [node.path for node in nodes]
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
        aggregation_func = list(set(graphite_influxdb.utils.get_aggregation_func(
            path, finder.aggregation_functions) for path in paths))[0]
        memcache_key = graphite_influxdb.utils.gen_memcache_key(
            int(self.start_time.strftime("%s")), int(self.end_time.strftime("%s")),
            aggregation_func, paths)
        self.assertTrue(finder.memcache.get(memcache_key),
                        msg="Got no memcache data for query %s with key %s" % (
                            query, memcache_key,))
        time_info, reader_data = nodes[0].reader.fetch(int(self.start_time.strftime("%s")),
                                                       int(self.end_time.strftime("%s")))
        self.assertEqual(len(data[self.series1]), len(reader_data[self.series1]),
                         msg="Reader cached data does not match finder cached data"
                         " for series %s" % (self.series1,))
    
    def test_reader_memcache_integration(self):
        reader = graphite_influxdb.InfluxdbReader(InfluxDBClient(
            database=self.db_name), self.series1, graphite_influxdb.utils.NullStatsd(),
            memcache_host='localhost')
        self.assertTrue(reader.fetch(int(self.start_time.strftime("%s")),
                                     int(self.end_time.strftime("%s"))))
    
    def test_memcache_default_config_values(self):
        del self.finder
        config = { 'influxdb' : { 'host' : 'localhost',
                                  'port' : 8086,
                                  'user' : 'root',
                                  'pass' : 'root',
                                  'db' : self.db_name,
                                  'log_level' : 'debug',
                                  'memcache' : { 'host': 'localhost'},
                                  },}
        finder = graphite_influxdb.InfluxdbFinder(config)
        self.assertTrue(finder.memcache_host)
        self.assertEqual(finder.memcache_ttl, 900,
                         msg="Default TTL should be 900 sec, got %s sec TTL instead" % (
                             finder.memcache_ttl,))
        self.assertEqual(finder.memcache_max_value, 15,
                         msg="Default max value should be 15 MB, got %s instead" % (
                             finder.memcache_max_value,))

if __name__ == '__main__':
    unittest.main()
