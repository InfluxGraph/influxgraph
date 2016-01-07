import uuid
import unittest
import graphite_influxdb

class GraphiteInfluxDBLogFileFailureTestCase(unittest.TestCase):
    
    def test_create_log_file_should_succeed(self):
        config = { 'influxdb' : { 'host' : 'localhost',
                                       'port' : 8086,
                                       'user' : 'root',
                                       'pass' : 'root',
                                       'db' : 'fakey',
                                       'log_file' : '/tmp/fakey',
                                       'log_level' : 'debug',
                                       },
                        }
        finder = graphite_influxdb.InfluxdbFinder(config)
        self.assertTrue(finder)

if __name__ == '__main__':
    unittest.main()
