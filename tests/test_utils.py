import unittest
import influxgraph.utils
from influxgraph.constants import DEFAULT_AGGREGATIONS
import datetime

class InfluxGraphUtilsTestCase(unittest.TestCase):

    def test_interval_calculation(self):
        start_time, end_time = (datetime.datetime.now() - datetime.timedelta(days=2)), \
          datetime.datetime.now()
        interval = influxgraph.utils.calculate_interval(int(start_time.strftime("%s")),
                                                              int(end_time.strftime("%s")))
        self.assertEqual(interval, 60,
                         msg="Expected interval of 60s for start/end times %s-%s, got %s" % (
                             start_time, end_time, interval))
        # More than 4 years time range
        start_time, end_time = (datetime.datetime.now() - datetime.timedelta(days=1461)), \
          datetime.datetime.now()
        interval = influxgraph.utils.calculate_interval(int(start_time.strftime("%s")),
                                                              int(end_time.strftime("%s")))
        self.assertEqual(interval, 86400,
                         msg="Expected interval of 1day/86400s for start/end times %s-%s, got %s" % (
                             start_time, end_time, interval))

    def test_get_retention_policy(self):
        policies = {60: 'default', 600: '10min', 1800: '30min'}
        for interval, _retention in policies.items():
            retention = influxgraph.utils.get_retention_policy(
                interval, policies)
            self.assertEqual(retention, _retention,
                             msg="Expected retention period %s for interval %s, got %s" % (
                                 _retention, interval, retention,))
        policy = influxgraph.utils.get_retention_policy(1900, policies)
        self.assertEqual(policy,'30min',
                         msg="Expected retention policy %s for interval %s - got %s" % (
                             '30min', 1900, policy))
        self.assertFalse(influxgraph.utils.get_retention_policy(60, None))
    
    def test_null_statsd(self):
        statsd = influxgraph.utils.NullStatsd()
        statsd.timer('key', 'val')
        statsd.timing('key', 'val')
        statsd.start()
        statsd.stop()

    def test_aggregation_functions(self):
        config = {'aggregation_functions': {
            '\.min$' : 'min',
            'pattern' : 'notvalidagg',
            'notvalidpattern[' : 'sum',
            }}
        aggregation_functions = influxgraph.utils._compile_aggregation_patterns(
            config.get('aggregation_functions', DEFAULT_AGGREGATIONS))
        self.assertTrue(config.get('aggregation_functions', None) is not None,
                        msg="Aggregation functions are empty")
        self.assertTrue('notvalidagg' not in aggregation_functions,
                        msg="Expected invalid aggregation function '%s' to not be in parsed functions" % (
                            'notvalidagg',))
        self.assertTrue('notvalidpattern[' not in aggregation_functions,
                        msg="Expected invalid regex pattern '%s' to not be in parsed functions" % (
                            'notvalidpattern[',))
        path = 'my.path.min'
        func = influxgraph.utils.get_aggregation_func(path, aggregation_functions)
        self.assertTrue(func == 'min',
                        msg="Expected aggregation function 'min' for path '%s' - got '%s' instead" % (
                            path, func))
        path = 'my.path.not.in.config'
        func = influxgraph.utils.get_aggregation_func(path, aggregation_functions)
        self.assertTrue(func == 'mean',
                        msg="Expected aggregation function 'mean' for path '%s' - got '%s' instead" % (
                            path, func))

    def test_empty_aggregation_functions(self):
        self.assertFalse(influxgraph.utils._compile_aggregation_patterns(None))

    def test_parse_empty_template(self):
        self.assertFalse(influxgraph.templates._parse_influxdb_graphite_templates(['']))
