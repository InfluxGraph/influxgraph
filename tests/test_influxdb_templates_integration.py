# -*- coding: utf-8 -*-

import memcache
import os
import unittest
import datetime
import time
import influxdb.exceptions
import influxgraph
import influxgraph.utils
from influxgraph.utils import Query
from influxgraph.constants import SERIES_LOADER_MUTEX_KEY, \
     MEMCACHE_SERIES_DEFAULT_TTL, LOADER_LIMIT
from influxdb import InfluxDBClient

os.environ['TZ'] = 'UTC'

class InfluxGraphTemplatesIntegrationTestCase(unittest.TestCase):
    """Test node lookup and data retrieval when using tags on and Graphite
    templates configured on InfluxGraph"""
    
    def setUp(self):
        self.metric_prefix = "template_integration_test"
        self.paths = ['test_type', 'host']
        self.tags = {
            # Tags parsed from metric path
            self.paths[0]: self.metric_prefix,
            self.paths[1]: 'localhost',
            # Default tags not in metric path
            'env': 'int',
            'region': 'the_west',
            }
        self.template = "%s %s.measurement* env=int,region=the_west" % (
            self.metric_prefix, ".".join([p for p in self.paths]))
        self.measurements = ['cpu', 'memory', 'load', 'iops']
        self.graphite_series = ["%s" % (".".join(
            [self.tags[p] for p in self.paths] + [m])) for m in self.measurements]
        self.step, self.num_datapoints, self.db_name = 60, 2, 'integration_test'
        self.start_time, self.end_time = (datetime.datetime.utcnow() - datetime.timedelta(hours=1)), \
          datetime.datetime.utcnow()
        self.steps = int(round((int(self.end_time.strftime("%s")) - \
                                int(self.start_time.strftime("%s"))) * 1.0 / self.step)) + 1
        self.config = {'influxdb': {
            'host': 'localhost',
            'port': 8086,
            'user': 'root',
            'pass': 'root',
            'db': self.db_name,
            'log_level': 'debug',
            'templates': [
                self.template,
                ],
            },
            }
        self.client = InfluxDBClient(database=self.db_name)
        self.default_nodes_limit = LOADER_LIMIT
        self.setup_db()
        self.finder = influxgraph.InfluxdbFinder(self.config)

    def setup_db(self):
        try:
            self.client.drop_database(self.db_name)
        except influxdb.exceptions.InfluxDBClientError:
            pass
        self.client.create_database(self.db_name)
        data = [{
            "measurement": measurement,
            "tags": self.tags,
            "time": _time,
            "fields": {
                "value": 1,
                }
            }
            for measurement in self.measurements
            for _time in [
                (self.end_time - datetime.timedelta(minutes=30)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                (self.end_time - datetime.timedelta(minutes=2)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                ]]
        self.assertTrue(self.client.write_points(data))

    def tearDown(self):
        self.client.drop_database(self.db_name)
        try:
            os.unlink('index')
        except OSError:
            pass

    def test_templated_index_find(self):
        query = Query('*')
        nodes = [n.name for n in self.finder.find_nodes(query)]
        expected = [self.metric_prefix]
        self.assertEqual(nodes, expected,
                         msg="Got root branch query result %s - wanted %s" % (
                             nodes, expected,))
        query = Query("%s.*" % (self.metric_prefix,))
        nodes = [n.name for n in self.finder.find_nodes(query)]
        expected = [self.tags[self.paths[1]]]
        self.assertEqual(nodes, expected,
                         msg="Got sub branch query result %s - wanted %s" % (
                             nodes, expected,))
        query = Query("%s.%s.*" % (self.metric_prefix, self.tags[self.paths[1]]))
        nodes = sorted([n.name for n in self.finder.find_nodes(query)])
        expected = sorted(self.measurements)
        self.assertEqual(nodes, expected,
                         msg="Got sub branch query result %s - wanted %s" % (
                             nodes, expected,))

    def test_templated_data_query(self):
        serie = self.graphite_series[0]
        nodes = list(self.finder.find_nodes(Query(serie)))
        time_info, data = self.finder.fetch_multi(nodes,
                                                  int(self.start_time.strftime("%s")),
                                                  int(self.end_time.strftime("%s")))
        self.assertTrue(serie in data,
                        msg="Did not get data for requested series %s - got data for %s" % (
                            serie, data.keys(),))
        self.assertEqual(time_info,
                         (int(self.start_time.strftime("%s")),
                          int(self.end_time.strftime("%s")),
                         self.step),
                         msg="Time info and step do not match our requested values")
        datapoints = [v for v in data[serie] if v]
        self.assertTrue(len(datapoints) == self.num_datapoints,
                        msg="Expected %s datapoints - got %s" % (
                            self.num_datapoints, len(datapoints),))

    def test_multiple_templates(self):
        del self.finder
        # Filter out first part of metric, keep the remainder as
        # measurement name
        template = "..measurement*"
        self.config['influxdb']['templates'] = [self.template,
                                                template]
        measurements = ['my_host.cpu.load', 'my_host.cpu.idle',
                        'my_host.cpu.usage', 'my_host.cpu.user']
        data = [{
            "measurement": measurement,
            "tags": {},
            "time": _time,
            "fields": {
                "value": 1,
                }
            }
            for measurement in measurements
            for _time in [
                (self.end_time - datetime.timedelta(minutes=30)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                (self.end_time - datetime.timedelta(minutes=2)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                ]]
        self.assertTrue(self.client.write_points(data))
        self.finder = influxgraph.InfluxdbFinder(self.config)
        query = Query('*')
        nodes = sorted([n.name for n in self.finder.find_nodes(query)])
        expected = sorted([measurements[0].split('.')[0]] + [self.metric_prefix])
        self.assertEqual(nodes, expected,
                         msg="Got root branch query result %s - wanted %s" % (
                             nodes, expected,))
        split_measurement = measurements[0].split('.')
        query = Query('%s.*' % (split_measurement[0]))
        nodes = [n.name for n in self.finder.find_nodes(query)]
        expected = [split_measurement[1]]
        self.assertEqual(nodes, expected,
                         msg="Got sub branch query result %s - wanted %s" % (
                             nodes, expected,))
        query = Query('%s.%s.*' % (split_measurement[0], split_measurement[1],))
        nodes = sorted([n.name for n in self.finder.find_nodes(query)])
        expected = sorted([m.split('.')[2] for m in measurements])
        self.assertEqual(nodes, expected,
                         msg="Got sub branch query result %s - wanted %s" % (
                             nodes, expected,))

    def test_template_measurement_first(self):
        del self.finder
        template = "..measurement.host.resource"
        self.config['influxdb']['templates'] = [template]
        measurements = ['load', 'idle',
                        'usage', 'user']
        tags = {'host': 'my_host',
                'resource': 'cpu',
                }
        data = [{
            "measurement": measurement,
            "tags": tags,
            "time": _time,
            "fields": {
                "value": 1,
                }
            }
            for measurement in measurements
            for _time in [
                (self.end_time - datetime.timedelta(minutes=30)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                (self.end_time - datetime.timedelta(minutes=2)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                ]]
        self.assertTrue(self.client.write_points(data))
        self.finder = influxgraph.InfluxdbFinder(self.config)
        query = Query('*')
        nodes = sorted([n.name for n in self.finder.find_nodes(query)])
        expected = sorted(dict.fromkeys(measurements + self.measurements).keys())
        self.assertEqual(nodes, expected,
                         msg="Got root branch query result %s - wanted %s" % (
                             nodes, expected,))
        query = Query('%s.*' % (measurements[0]))
        nodes = sorted([n.name for n in self.finder.find_nodes(query)])
        expected = sorted([tags['host'], self.tags[self.paths[1]]])
        self.assertEqual(nodes, expected,
                         msg="Got query %s result %s - wanted %s" % (
                             query.pattern, nodes, expected,))
        query = Query('%s.%s.*' % (measurements[0], tags['host'],))
        nodes = sorted([n.name for n in self.finder.find_nodes(query)])
        expected = sorted([tags['resource']])
        self.assertEqual(nodes, expected,
                         msg="Got query %s result %s - wanted %s" % (
                             query.pattern, nodes, expected,))

    def test_data_with_fields(self):
        del self.finder
        template = "host.measurement.field*"
        self.config['influxdb']['templates'] = [template]
        measurements = ['cpu']
        fields = {'load': 1, 'idle': 1,
                  'usage': 1, 'user': 1,
        }
        tags = {'host': 'my_host',
                'env': 'my_env',
                }
        data = [{
            "measurement": measurement,
            "tags": tags,
            "time": _time,
            "fields": fields,
            }
            for measurement in measurements
            for _time in [
                (self.end_time - datetime.timedelta(minutes=30)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                (self.end_time - datetime.timedelta(minutes=2)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                ]]
        self.client.drop_database(self.db_name)
        self.client.create_database(self.db_name)
        self.assertTrue(self.client.write_points(data))
        self.finder = influxgraph.InfluxDBFinder(self.config)
        query = Query('%s.*' % (tags['host']))
        nodes = sorted([n.name for n in self.finder.find_nodes(query)])
        expected = measurements
        self.assertEqual(nodes, expected,
                         msg="Got query %s result %s - wanted %s" % (
                             query.pattern, nodes, expected,))
        query = Query('%s.%s.*' % (tags['host'], measurements[0], ))
        nodes = sorted([n.name for n in self.finder.find_nodes(query)])
        expected = sorted(fields.keys())
        self.assertEqual(nodes, expected,
                         msg="Got query %s result %s - wanted %s" % (
                             query.pattern, nodes, expected,))

    def test_tagged_data_no_template_config(self):
        del self.finder
        self.config['influxdb']['templates'] = None
        self.finder = influxgraph.InfluxdbFinder(self.config)
        query = Query('*')
        nodes = sorted([n.name for n in self.finder.find_nodes(query)])
        # expected = [self.metric_prefix]
        expected = sorted(self.measurements)
        self.assertEqual(nodes, expected,
                         msg="Expected only measurements in index with "
                         "no templates configured, got %s" % (nodes,))
        # query = Query('%s.*' % (self.metric_prefix,))
        # nodes = sorted([n.name for n in self.finder.find_nodes(query)])
        # expected = [self.tags[self.paths[0]]]
        # self.assertEqual(nodes, expected)

if __name__ == '__main__':
    unittest.main()
