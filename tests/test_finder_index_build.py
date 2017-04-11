import unittest
import fcntl
import datetime
import time
import os
from random import randint

from influxdb import InfluxDBClient
import influxdb.exceptions
import influxgraph
import influxgraph.utils
from influxgraph.utils import Query, gen_memcache_key, get_aggregation_func, \
     Node
from influxgraph.constants import SERIES_LOADER_MUTEX_KEY, \
     MEMCACHE_SERIES_DEFAULT_TTL, LOADER_LIMIT, DEFAULT_AGGREGATIONS, \
     _INFLUXDB_CLIENT_PARAMS, FILE_LOCK
from influxgraph.classes.finder import logger as finder_logger

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
        self.db_name = 'integration_test'
        self.start_time, self.end_time = (datetime.datetime.utcnow() - datetime.timedelta(hours=1)), \
          datetime.datetime.utcnow()
        self.config = { 'influxdb': { 'db' : self.db_name,
                                      'reindex_interval': 0,
                                      },
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

    def test_multi_finder_index_build(self):
        """Test index build lock with multiple finders"""
        self.assertRaises(IOError, fcntl.flock(
            open(FILE_LOCK, 'w'), fcntl.LOCK_EX | fcntl.LOCK_NB))

if __name__ == '__main__':
    unittest.main()
