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


    def test_config_parsing(self):
        cfg = {}
        with self.assertRaises(SystemExit) as cm:
            graphite_influxdb.utils.normalize_config(cfg)
        

    def test_null_statsd(self):
        statsd = graphite_influxdb.utils.NullStatsd()
        statsd.timer('key', 'val')
        statsd.timing('key', 'val')
        statsd.start()
        statsd.stop()
