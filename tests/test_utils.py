import unittest
import graphite_influxdb.utils
import datetime

class GraphiteInfluxdbUtilsTestCase(unittest.TestCase):

    def test_interval_calculation(self):
        start_time, end_time = (datetime.datetime.now() - datetime.timedelta(days=2)), \
          datetime.datetime.now()
        interval = graphite_influxdb.utils.calculate_interval(int(start_time.strftime("%s")),
                                                              int(end_time.strftime("%s")))
        self.assertEqual(interval, 60,
                         msg="Expected interval of 60s for start/end times %s-%s, got %s" % (
                             start_time, end_time, interval))
        # More than 4 years time range
        start_time, end_time = (datetime.datetime.now() - datetime.timedelta(days=1461)), \
          datetime.datetime.now()
        interval = graphite_influxdb.utils.calculate_interval(int(start_time.strftime("%s")),
                                                              int(end_time.strftime("%s")))
        self.assertEqual(interval, 86400,
                         msg="Expected interval of 1day/86400s for start/end times %s-%s, got %s" % (
                             start_time, end_time, interval))

    def test_get_retention_policy(self):
        policies = {60: 'default', 600: '10min', 1800: '30min'}
        for interval, _retention in policies.items():
            retention = graphite_influxdb.utils.get_retention_policy(
                interval, policies)
            self.assertEqual(retention, _retention,
                             msg="Expected retention period %s for interval %s, got %s" % (
                                 _retention, interval, retention,))
        policy = graphite_influxdb.utils.get_retention_policy(1900, policies)
        self.assertEqual(policy,'30min',
                         msg="Expected retention policy %s for interval %s - got %s" % (
                             '30min', 1900, policy))
    
    def test_config_parsing(self):
        cfg = {}
        self.assertRaises(SystemExit, graphite_influxdb.utils.normalize_config, cfg)

    def test_null_statsd(self):
        statsd = graphite_influxdb.utils.NullStatsd()
        statsd.timer('key', 'val')
        statsd.timing('key', 'val')
        statsd.start()
        statsd.stop()

    def test_retention_policies_cfg(self):
        cfg = { 'influxdb' : {'retention_policies': {60: 'default', 600: '10m', 1800: '30m'}}}
        config = graphite_influxdb.utils.normalize_config(cfg)
        self.assertTrue('retention_policies' in config)

    def test_aggregation_functions(self):
        cfg = { 'influxdb' : {
            'aggregation_functions': {
                '\.min$' : 'min',
                'pattern' : 'notvalidagg',
                'notvalidpattern[' : 'sum',
                }}}
        config = graphite_influxdb.utils.normalize_config(cfg)
        self.assertTrue(config.get('aggregation_functions', None) is not None,
                        msg="Aggregation functions are empty")
        self.assertTrue('notvalidagg' not in config['aggregation_functions'].values(),
                        msg="Expected invalid aggregation function '%s' to not be in parsed functions" % (
                            'notvalidagg',))
        self.assertTrue('notvalidpattern[' not in config['aggregation_functions'].values(),
                        msg="Expected invalid regex pattern '%s' to not be in parsed functions" % (
                            'notvalidpattern[',))
        path = 'my.path.min'
        func = graphite_influxdb.utils.get_aggregation_func(path, config['aggregation_functions'])
        self.assertTrue(func == 'min',
                        msg="Expected aggregation function 'min' for path '%s' - got '%s' instead" % (
                            path, func))
        path = 'my.path.not.in.config'
        func = graphite_influxdb.utils.get_aggregation_func(path, config['aggregation_functions'])
        self.assertTrue(func == 'mean',
                        msg="Expected aggregation function 'mean' for path '%s' - got '%s' instead" % (
                            path, func))

    def test_empty_aggregation_functions(self):
        self.assertFalse(graphite_influxdb.utils._compile_aggregation_patterns(None))
