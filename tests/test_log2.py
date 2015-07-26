import os
import uuid
import unittest
import graphite_influxdb

class GraphiteInfluxDBLogFileConfigTestCase(unittest.TestCase):

    def setUp(self):
        pass
    
    def test_create_root_log_file_should_fail(self):
        _config = { 'influxdb' : { 'host' : 'localhost',
                                       'port' : 8086,
                                       'user' : 'root',
                                       'pass' : 'root',
                                       'db' : 'fakey',
                                       'log_file' : '/' + str(uuid.uuid4()),
                                       'log_level' : 'debug',
                                       },
                        }
        finder = graphite_influxdb.InfluxdbFinder(_config)
        self.assertTrue(finder)

if __name__ == '__main__':
    unittest.main()
