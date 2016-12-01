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
     MEMCACHE_SERIES_DEFAULT_TTL, LOADER_LIMIT, _MEMCACHE_FIELDS_KEY
from influxdb import InfluxDBClient
from influxgraph.templates import InvalidTemplateError

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
        self.step, self.num_datapoints, self.db_name = 60, 31, 'integration_test'
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
            'templates': [
                self.template,
                ],
            },
            }
        self.client = InfluxDBClient(database=self.db_name)
        self.default_nodes_limit = LOADER_LIMIT
        self.setup_db()
        self.finder = influxgraph.InfluxDBFinder(self.config)

    def write_data(self, measurements, tags, fields):
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
        self.assertTrue(self.client.write_points(data))

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

    def _test_data_in_nodes(self, nodes):
        time_info, data = self.finder.fetch_multi(
            nodes, int(self.start_time.strftime("%s")),
            int(self.end_time.strftime("%s")))
        for metric in [n.path for n in nodes]:
            datapoints = [v for v in data[metric] if v]
            self.assertTrue(len(datapoints) == self.num_datapoints,
                            msg="Expected %s datapoints for %s - got %s" % (
                                self.num_datapoints, metric, len(datapoints),))

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
                        msg="Expected %s datapoints for %s - got %s" % (
                            self.num_datapoints, serie, len(datapoints),))

    def test_template_drop_path_part(self):
        del self.finder
        # Filter out first part of metric, keep the remainder as
        # measurement name
        template = "..measurement*"
        self.config['influxdb']['templates'] = [self.template,
                                                template]
        measurements = ['my_host.cpu.load', 'my_host.cpu.idle',
                        'my_host.cpu.usage', 'my_host.cpu.user']
        fields = {"value": 1}
        self.write_data(measurements, {}, fields)
        self.finder = influxgraph.InfluxDBFinder(self.config)
        query = Query('*')
        nodes = sorted([n.name for n in self.finder.find_nodes(query)])
        expected = sorted(['my_host', self.metric_prefix])
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

    def test_multiple_templates(self):
        del self.finder
        templates = ["*.diskio. host.measurement.name.field*",
                     "*.disk. host.measurement.path.field*",
                     "*.cpu. host.measurement.cpu.field*",
                     "host.measurement.field*",
                     ]
        self.client.drop_database(self.db_name)
        self.client.create_database(self.db_name)
        measurements = ['cpu', 'diskio', 'disk']
        fields = [{'load': 1, 'idle': 1,
                  'usage': 1, 'user': 1,
                  },
                 {'io_time': 1,
                  'read_bytes': 1,
                  'write_bytes': 1,
                  'read_time': 1,
                  'write_time': 1,
                  },
                 {'free': 1,
                  'total': 1,
                  'used': 1,
                 }]
        tags = [{'host': 'my_host',
                'cpu': 'cpu-total'
                },
                {'host': 'my_host',
                'name': 'sda',
                },
                {'host': 'my_host',
                 'path': 'somepath',
                }]
        for i in range(len(measurements)):
            self.write_data([measurements[i]], tags[i], fields[i])
        self.config['influxdb']['templates'] = templates
        self.finder = influxgraph.InfluxDBFinder(self.config)
        metrics_cpu = ['.'.join([tags[0]['host'], measurements[0], tags[0]['cpu'], f])
                       for f in fields[0].keys()]
        metrics_diskio = ['.'.join([tags[1]['host'], measurements[1], tags[1]['name'], f])
                           for f in fields[1].keys()]
        metrics_disk = ['.'.join([tags[2]['host'], measurements[2], tags[2]['path'], f])
                        for f in fields[2].keys()]
        cpu_query = Query('%s.%s.*.*' % (tags[0]['host'], measurements[0],))
        diskio_query = Query('%s.%s.*.*' % (tags[1]['host'], measurements[1],))
        disk_query = Query('%s.%s.*.*' % (tags[2]['host'], measurements[2],))
        cpu_nodes = list(self.finder.find_nodes(cpu_query))
        diskio_nodes = list(self.finder.find_nodes(diskio_query))
        disk_nodes = list(self.finder.find_nodes(disk_query))
        for i in range(len([cpu_nodes, diskio_nodes, disk_nodes])):
            nodes = [cpu_nodes, diskio_nodes, disk_nodes][i]
            metrics = [metrics_cpu, metrics_diskio, metrics_disk][i]
            node_paths = [n.path for n in nodes]
            self.assertEqual(sorted(node_paths), sorted(metrics))
        for nodes in [cpu_nodes, diskio_nodes, disk_nodes]:
            _, data = self.finder.fetch_multi(cpu_nodes,
                                              int(self.start_time.strftime("%s")),
                                              int(self.end_time.strftime("%s")))
            for metric in metrics_cpu:
                datapoints = [v for v in data[metric] if v]
                self.assertTrue(len(datapoints) == self.num_datapoints,
                                msg="Expected %s datapoints for %s - got %s" % (
                                    self.num_datapoints, metric, len(datapoints),))

    def test_template_filter_patterns(self):
        del self.finder
        self.client.drop_database(self.db_name)
        self.client.create_database(self.db_name)
        templates = ["*.*.memory. host.filter.measurement",
                     "*.*.interface host.filter.measurement",
                     "*.*.trailing.* host.filter.measurement",
                     "*.stats host.prefix.filter.measurement",
                     ]
        tags = {'host': 'my_host',
                }
        fields = {'value': 1,
                  }
        measurements = ['memory', 'interface', 'trailing']
        for measurement in measurements:
            tags['filter'] = measurement
            self.write_data([measurement], tags, fields)
        prefix_measurement = 'prefix_measurement'
        tags['prefix'] = 'stats'
        tags['filter'] = 'prefix'
        self.write_data([prefix_measurement], tags, fields)
        self.config['influxdb']['templates'] = templates
        self.finder = influxgraph.InfluxDBFinder(self.config)
        ##
        query = Query('%s.*' % (
            tags['host'],))
        nodes = list(self.finder.find_nodes(query))
        self.assertEqual(sorted([n.name for n in nodes]),
                         sorted(measurements + [tags['prefix']]))
        for measurement in measurements:
            query = Query('%s.%s.*' % (
                tags['host'], measurement))
            nodes = list(self.finder.find_nodes(query))
            self.assertEqual(sorted([n.name for n in nodes]), [measurement])
            self._test_data_in_nodes(nodes)
        ## 
        query = Query('%s.*.*' % (tags['host'],))
        nodes = list(self.finder.find_nodes(query))
        # 
        self.assertEqual(sorted([n.name for n in nodes]), sorted(measurements + [tags['filter']]))
        # import ipdb; ipdb.set_trace()
        self._test_data_in_nodes([n for n in nodes if n.is_leaf])
        query = Query('%s.*.*.*' % (tags['host'],))
        nodes = list(self.finder.find_nodes(query))
        self.assertEqual(sorted([n.name for n in nodes]), [prefix_measurement])
        self._test_data_in_nodes(nodes)

    def test_template_multi_tag_no_field(self):
        self.client.drop_database(self.db_name)
        self.client.create_database(self.db_name)
        templates = ["*.memory.* host.measurement.metric",
                     "*.interface.* host.measurement.device.metric",
                     ]
        mem_measurement = 'memory'
        tags = {'host': 'my_host',
                'metric': 'some_metric',
                }
        fields = {'value': 1,
                  }
        self.write_data([mem_measurement], tags, fields)
        int_measurement = 'interface'
        tags = {'host': 'my_host',
                'device': 'dev',
                'metric': 'some_metric',
                }
        fields = {'value': 1,
                  }
        self.write_data([int_measurement], tags, fields)
        self.config['influxdb']['templates'] = templates
        self.finder = influxgraph.InfluxDBFinder(self.config)
        ##
        query = Query('%s.%s.*' % (
            tags['host'], mem_measurement,))
        nodes = list(self.finder.find_nodes(query))
        self.assertEqual(sorted([n.name for n in nodes]), [tags['metric']])
        time_info, data = self.finder.fetch_multi(
            nodes, int(self.start_time.strftime("%s")),
            int(self.end_time.strftime("%s")))
        datapoints = [v for v in data[nodes[0].path] if v]
        self.assertTrue(len(datapoints) == self.num_datapoints,
                        msg="Expected %s datapoints for %s - got %s" % (
                            self.num_datapoints, nodes[0].path, len(datapoints),))
        ##
        query = Query('%s.%s.*' % (
            tags['host'], int_measurement,))
        nodes = list(self.finder.find_nodes(query))
        self.assertEqual(sorted([n.name for n in nodes]), [tags['device']])
        time_info, data = self.finder.fetch_multi(
            nodes, int(self.start_time.strftime("%s")),
            int(self.end_time.strftime("%s")))
        for metric in [n.path for n in nodes]:
            datapoints = [v for v in data[metric] if v]
            self.assertTrue(len(datapoints) == self.num_datapoints,
                            msg="Expected %s datapoints for %s - got %s" % (
                                self.num_datapoints, metric, len(datapoints),))
        query = Query('%s.%s.*.*' % (
            tags['host'], int_measurement,))
        nodes = list(self.finder.find_nodes(query))
        self.assertEqual(sorted([n.name for n in nodes]), [tags['metric']])
        time_info, data = self.finder.fetch_multi(
            nodes, int(self.start_time.strftime("%s")),
            int(self.end_time.strftime("%s")))
        for metric in [n.path for n in nodes]:
            datapoints = [v for v in data[metric] if v]
            self.assertTrue(len(datapoints) == self.num_datapoints,
                            msg="Expected %s datapoints for %s - got %s" % (
                                self.num_datapoints, metric, len(datapoints),))

    def test_template_multiple_tags(self):
        self.client.drop_database(self.db_name)
        self.client.create_database(self.db_name)
        template = "*.disk. host.measurement.path.fstype.field*"
        measurement = 'disk'
        tags = {'host': 'my_host',
                'path': '/',
                'fstype': 'ext4',
                }
        fields = {'free': 1,
                  'used': 1,
                  }
        self.write_data([measurement], tags, fields)
        self.config['influxdb']['templates'] = [template]
        self.finder = influxgraph.InfluxDBFinder(self.config)
        query = Query('%s.%s.%s.%s.*' % (
            tags['host'], measurement, tags['path'], tags['fstype']))
        nodes = list(self.finder.find_nodes(query))
        self.assertEqual(sorted([n.name for n in nodes]), sorted(fields.keys()))

    def test_template_measurement_first(self):
        del self.finder
        template = "measurement.host.resource"
        self.config['influxdb']['templates'] = [template]
        measurements = ['load', 'idle',
                        'usage', 'user']
        tags = {'host': 'my_host',
                'resource': 'cpu',
                }
        fields = {'value': 1}
        self.write_data(measurements, tags, fields)
        self.finder = influxgraph.InfluxDBFinder(self.config)
        query = Query('*')
        nodes = sorted([n.name for n in self.finder.find_nodes(query)])
        expected = sorted(measurements)
        self.assertEqual(nodes, expected,
                         msg="Got root branch query result %s - wanted %s" % (
                             nodes, expected,))
        query = Query('%s.*' % (measurements[0]))
        nodes = sorted([n.name for n in self.finder.find_nodes(query)])
        expected = [tags['host']]
        self.assertEqual(nodes, expected,
                         msg="Got query %s result %s - wanted %s" % (
                             query.pattern, nodes, expected,))
        query = Query('%s.%s.*' % (measurements[0], tags['host'],))
        nodes = sorted([n.name for n in self.finder.find_nodes(query)])
        expected = sorted([tags['resource']])
        self.assertEqual(nodes, expected,
                         msg="Got query %s result %s - wanted %s" % (
                             query.pattern, nodes, expected,))

    def test_non_greedy_field(self):
        measurements = ['cpu-0', 'cpu-1', 'cpu-2', 'cpu-3']
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
        metrics = ['.'.join([tags['host'], m, f])
                   for f in fields.keys()
                   for m in measurements]
        self.client.drop_database(self.db_name)
        self.client.create_database(self.db_name)
        self.assertTrue(self.client.write_points(data))
        template = "host.measurement.field"
        self.config['influxdb']['templates'] = [template]
        self.finder = influxgraph.InfluxDBFinder(self.config)
        query = Query('%s.*.*' % (tags['host'],))
        nodes = list(self.finder.find_nodes(query))
        node_paths = sorted([n.path for n in nodes])
        _metrics = ['.'.join([tags['host'], m, f])
                    for f in fields.keys() if not '.' in f
                    for m in measurements ]
        expected = sorted(_metrics)
        self.assertEqual(node_paths, expected,
                         msg="Expected nodes %s from template with non-greedy field - got %s" % (
                             expected, node_paths))

    def test_data_with_fields(self):
        del self.finder
        template = "host.measurement.field*"
        self.config['influxdb']['templates'] = [template]
        measurements = ['cpu-0', 'cpu-1', 'cpu-2', 'cpu-3']
        fields = {'load': 1, 'idle': 1,
                  'usage': 1, 'user': 1,
                  'user.io': 1, 'idle.io': 1,
                  'load.io': 1, 'usage.io': 1,
        }
        tags = {'host': 'my_host',
                'env': 'my_env',
                }
        cpu_metrics = ['.'.join(['my_host', m, f])
                       for m in measurements
                       for f in ['load', 'usage', 'user', 'idle']]
        io_metrics = ['.'.join(['my_host', m, f])
                       for m in measurements
                       for f in ['user.io', 'idle.io', 'load.io', 'usage.io']]
        self.client.drop_database(self.db_name)
        self.client.create_database(self.db_name)
        self.write_data(measurements, tags, fields)
        self.finder = influxgraph.InfluxDBFinder(self.config)
        query = Query('%s.*' % (tags['host']))
        nodes = sorted([n.name for n in self.finder.find_nodes(query)])
        expected = measurements
        self.assertEqual(nodes, expected,
                         msg="Got query %s result %s - wanted %s" % (
                             query.pattern, nodes, expected,))
        query = Query('%s.*.*' % (tags['host'],))
        nodes = list(self.finder.find_nodes(query))
        node_paths = sorted([n.path for n in nodes])
        expected = sorted(cpu_metrics)
        self.assertEqual(node_paths, expected,
                         msg="Got query %s result %s - wanted %s" % (
                             query.pattern, node_paths, expected,))
        time_info, data = self.finder.fetch_multi(nodes,
                                                  int(self.start_time.strftime("%s")),
                                                  int(self.end_time.strftime("%s")))
        for metric in cpu_metrics:
            self.assertTrue(metric in data,
                            msg="Did not get data for requested series %s - got data for %s" % (
                                metric, data.keys(),))
            datapoints = [v for v in data[metric] if v]
            self.assertTrue(len(datapoints) == self.num_datapoints,
                            msg="Expected %s datapoints for %s - got %s" % (
                                self.num_datapoints, metric, len(datapoints),))
        time_info, _data = self.finder.fetch_multi(nodes,
                                                   int(self.start_time.strftime("%s")),
                                                   int(self.end_time.strftime("%s")))
        for metric in cpu_metrics:
            datapoints = [v for v in _data[metric] if v]
            self.assertTrue(len(datapoints) == self.num_datapoints,
                            msg="Expected %s datapoints for %s - got %s" % (
                                self.num_datapoints, metric, len(datapoints),))

    def test_multi_tag_values_multi_measurements(self):
        measurements = ['cpu-0', 'cpu-1', 'cpu-2', 'cpu-3']
        fields = {'load': 1, 'idle': 1,
                  'usage': 1, 'user': 1,
        }
        tags = {'host': 'my_host1',
                'env': 'my_env1',
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
        metrics = ['.'.join([tags['host'], m, f])
                   for f in fields.keys()
                   for m in measurements]
        self.client.drop_database(self.db_name)
        self.client.create_database(self.db_name)
        self.assertTrue(self.client.write_points(data))
        tags_env2 = {'host': 'my_host1',
                     'env': 'my_env2',
                     }
        for d in data:
            d['tags'] = tags_env2
        self.assertTrue(self.client.write_points(data))
        template = "env.host.measurement.field*"
        self.config['influxdb']['templates'] = [template]
        self.finder = influxgraph.InfluxDBFinder(self.config)
        query = Query('*.*.*.*')
        nodes = list(self.finder.find_nodes(query))
        node_paths = sorted([n.path for n in nodes])
        tag_values = set(['.'.join([t['env'], t['host']])
                          for t in [tags, tags_env2]])
        _metrics = ['.'.join([t, m, f])
                    for t in tag_values
                    for f in fields.keys() if not '.' in f
                    for m in measurements]
        expected = sorted(_metrics)
        self.assertEqual(node_paths, expected,
                         msg="Expected %s nodes - got %s" % (
                             len(expected), len(node_paths)))
        _, multi_tag_data = self.finder.fetch_multi(nodes,
                                          int(self.start_time.strftime("%s")),
                                          int(self.end_time.strftime("%s")))
        for metric in _metrics:
            datapoints = [v for v in multi_tag_data[metric] if v]
            self.assertTrue(len(datapoints) == self.num_datapoints,
                            msg="Expected %s datapoints for %s - got %s" % (
                                self.num_datapoints, metric, len(datapoints),))

    def test_field_data_part_or_no_template_match(self):
        del self.finder
        measurements = ['test']
        fields = {'field1': 1, 'field2': 2}
        tags = {'env': 'my_env',
                'region': 'my_region',
                'dc': 'dc1'
                }
        self.client.drop_database(self.db_name)
        self.client.create_database(self.db_name)
        self.write_data(measurements, tags, fields)
        self.config['influxdb']['templates'] = ['env.template_tag.measurement.field*']
        self.finder = influxgraph.InfluxDBFinder(self.config)
        query = Query('*')
        nodes = [n.name for n in self.finder.find_nodes(query)]
        expected = []
        self.assertEqual(nodes, expected)
        self.config['influxdb']['templates'] = ['env.template_tag.measurement.field*',
                                                'env.region.measurement.field*']
        self.finder = influxgraph.InfluxDBFinder(self.config)
        query = Query('*')
        nodes = sorted([n.path for n in self.finder.find_nodes(query)])
        expected = [tags['env']]
        self.assertEqual(nodes, expected)
        query = Query('*.*')
        nodes = sorted([n.path for n in self.finder.find_nodes(query)])
        expected = sorted(['.'.join([tags['env'], tags['region']])])
        self.assertEqual(nodes, expected)
        query = Query('*.*.*')
        nodes = sorted([n.path for n in self.finder.find_nodes(query)])
        expected = sorted(['.'.join([tags['env'], tags['region'], measurements[0]])])
        self.assertEqual(nodes, expected)
        query = Query('*.*.*.*')
        nodes = sorted([n.path for n in self.finder.find_nodes(query)])
        expected = sorted(['.'.join([tags['env'], tags['region'], measurements[0], f])
                           for f in fields.keys()])
        self.assertEqual(nodes, expected)

    def test_tagged_data_no_template_config(self):
        del self.finder
        self.config['influxdb']['templates'] = None
        self.finder = influxgraph.InfluxDBFinder(self.config)
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

    def test_tagged_data_multi_greedy_field(self):
        del self.finder
        measurements = ['cpu']
        fields = {'cpu0.load': 1, 'cpu0.idle': 1,
                  'cpu0.usage': 1, 'cpu0.user': 1,
                  'cpu1.load': 1, 'cpu1.idle': 1,
                  'cpu1.usage': 1, 'cpu1.user': 1,
                  'cpu2.load': 1, 'cpu2.idle': 1,
                  'cpu2.usage': 1, 'cpu2.user': 1,
                  'cpu3.load': 1, 'cpu3.idle': 1,
                  'cpu4.usage': 1, 'cpu3.user': 1,
                  'total.load': 4, 'total.idle': 4,
                  'total.usage': 4, 'total.user': 4,
                  'cpu_number': 4, 'wio': 1,
        }
        tags = {'host': 'my_host1',
                'env': 'my_env1',
                }
        metrics = ['.'.join([tags['env'], tags['host'], m, f])
                   for f in fields.keys()
                   for m in measurements] + \
                   ['.'.join(['my_env2', 'my_host2', m, f])
                   for f in fields.keys()
                   for m in measurements]
        self.client.drop_database(self.db_name)
        self.client.create_database(self.db_name)
        self.write_data(measurements, tags, fields)
        tags['host'] = 'my_host2'
        tags['env'] = 'my_env2'
        self.write_data(measurements, tags, fields)
        template = "env.host.measurement.field*"
        self.config['influxdb']['templates'] = [template]
        self.finder = influxgraph.InfluxDBFinder(self.config)
        nodes = sorted([n.name for n in self.finder.find_nodes(Query('*'))])
        expected = sorted(['my_env1', 'my_env2'])
        self.assertEqual(nodes, expected)
        nodes = sorted([n.path for n in self.finder.find_nodes(Query('*.*'))])
        expected = sorted(['my_env1.my_host1',
                           'my_env2.my_host2'])
        self.assertEqual(nodes, expected)
        nodes = sorted([n.path for n in self.finder.find_nodes(Query('*.*.*'))])
        expected = sorted(['my_env1.my_host1.cpu',
                           'my_env2.my_host2.cpu'])
        self.assertEqual(nodes, expected)
        nodes = sorted(dict.fromkeys([n.name for n in self.finder.find_nodes(Query('*.*.*.*'))]).keys())
        expected = sorted(['cpu0', 'cpu1', 'cpu2', 'cpu3', 'cpu4', 'total', 'wio', 'cpu_number'])
        self.assertEqual(nodes, expected)
        nodes = list(self.finder.find_nodes(Query('*.*.*.cpu0.*')))
        node_names = sorted(dict.fromkeys([n.name for n in nodes]).keys())
        expected = sorted(['load', 'usage', 'idle', 'user'])
        self.assertEqual(node_names, expected)
        _, data = self.finder.fetch_multi(nodes, int(self.start_time.strftime("%s")),
                                          int(self.end_time.strftime("%s")))
        metrics = [n.path for n in nodes]
        for metric in metrics:
            datapoints = [v for v in data[metric] if v]
            self.assertTrue(len(datapoints) == self.num_datapoints)
        template = "env.host.measurement.field"
        self.config['influxdb']['templates'] = [template]
        self.finder = influxgraph.InfluxDBFinder(self.config)
        nodes = list(self.finder.find_nodes(Query('*.*.*.*')))
        node_names = sorted(dict.fromkeys([n.name for n in nodes]).keys())
        expected = sorted(['cpu0', 'cpu1', 'cpu2', 'cpu3', 'cpu4', 'total', 'wio', 'cpu_number'])
        self.assertEqual(node_names, expected)
        _, data = self.finder.fetch_multi(nodes, int(self.start_time.strftime("%s")),
                                          int(self.end_time.strftime("%s")))
        metrics = ['.'.join([tags['env'], tags['host'], measurements[0], f])
                   for f in ['wio', 'cpu_number']]
        for metric in metrics:
            datapoints = [v for v in data[metric] if v]
            self.assertTrue(len(datapoints) == self.num_datapoints)
        bad_metrics = ['.'.join([tags['env'], tags['host'], measurements[0], f])
                       for f in ['cpu0', 'cpu1', 'cpu2', 'cpu3', 'cpu4', 'total']]
        for metric in bad_metrics:
            datapoints = [v for v in data[metric] if v]
            self.assertTrue(len(datapoints) == 0)

    def test_field_template_with_value_field(self):
        template = "env.host.measurement.field*"
        del self.finder
        measurements = ['cpuusage']
        fields = {'value': 1}
        tags = {'host': 'my_host1',
                'env': 'my_env1',
                }
        metrics = ['.'.join([tags['env'], tags['host'], m])
                   for m in measurements]
        self.client.drop_database(self.db_name)
        self.client.create_database(self.db_name)
        self.write_data(measurements, tags, fields)
        self.config['influxdb']['templates'] = [template]
        self.finder = influxgraph.InfluxDBFinder(self.config)
        cpu_nodes = list(self.finder.find_nodes(Query('my_env1.my_host1.*')))
        expected = measurements
        self.assertEqual([n.name for n in cpu_nodes], expected)
        nodes = list(self.finder.find_nodes(Query('my_env1.my_host1.*.*')))
        self.assertEqual(nodes, [])
        _, data = self.finder.fetch_multi(cpu_nodes, int(self.start_time.strftime("%s")),
                                          int(self.end_time.strftime("%s")))
        for metric in metrics:
            datapoints = [v for v in data[metric] if v]
            self.assertTrue(len(datapoints) == self.num_datapoints)

    def test_bad_templates(self):
        self.config['influxdb']['templates'] = ['host.measurement*.field*']
        self.assertRaises(InvalidTemplateError, influxgraph.InfluxDBFinder, self.config)
        self.config['influxdb']['templates'] = ['host.field.field']
        self.assertRaises(InvalidTemplateError, influxgraph.InfluxDBFinder, self.config)

    def test_template_nofilter_extra_tags(self):
        self.config['influxdb']['templates'] = ['host.measurement* env=int,region=the_west']
        finder = influxgraph.InfluxDBFinder(self.config)
        self.assertTrue(finder.graphite_templates)
        self.assertEqual({'env': 'int', 'region': 'the_west'}, finder.graphite_templates[0][2])

    def test_memcache_field_keys(self):
        self.config['influxdb']['memcache'] = {'host': 'localhost'}
        self.config['influxdb']['series_loader_interval'] = 2
        self.finder = influxgraph.InfluxDBFinder(self.config)
        time.sleep(2)
        self.assertTrue(self.finder.memcache.get(_MEMCACHE_FIELDS_KEY),
                        msg="Expected field key list to be loaded to cache "
                        "at startup")
        self.finder.memcache.delete(_MEMCACHE_FIELDS_KEY)
        keys_list = self.finder.get_field_keys()
        keys_memcache = self.finder.memcache.get(_MEMCACHE_FIELDS_KEY)
        self.assertEqual(keys_list, keys_memcache)

    def test_template_measurement_no_tags(self):
        template = "env.host.measurement.field*"
        del self.finder
        measurements = ['cpuusage']
        fields = {'value': 1}
        tags = {'host': 'my_host1',
                'env': 'my_env1',
                }
        metrics = ['.'.join([tags['env'], tags['host'], m])
                   for m in measurements]
        self.client.drop_database(self.db_name)
        self.client.create_database(self.db_name)
        self.write_data(measurements, tags, fields)
        fields = {'value': 1,
                  }
        self.write_data(measurements, {}, fields)
        self.config['influxdb']['templates'] = [template]
        self.finder = influxgraph.InfluxDBFinder(self.config)
        nodes = [n.name for n in self.finder.find_nodes(Query('*'))]
        expected = [tags['env']]
        self.assertEqual(nodes, expected)
        cpu_nodes = list(self.finder.find_nodes(Query('my_env1.my_host1.*')))
        expected = measurements
        self.assertEqual([n.name for n in cpu_nodes], expected)

if __name__ == '__main__':
    unittest.main()
