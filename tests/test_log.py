import uuid
import unittest
import influxgraph
from influxgraph.influxdb import InfluxDBClient


class InfluxGraphLogFileTestCase(unittest.TestCase):

    def setUp(self):
        self.db_name = 'fakey'
        self.client = InfluxDBClient('localhost', db=self.db_name)
        self.client.create_database(self.db_name)
        _logger = influxgraph.classes.finder.logger
        _logger.handlers = []

    def tearDown(self):
        self.client.drop_database(self.db_name)
    
    def test_create_log_file_should_succeed(self):
        config = { 'influxdb' : { 'host' : 'localhost',
                                  'port' : 8086,
                                  'user' : 'root',
                                  'pass' : 'root',
                                  'db' : self.db_name,
                                  'log_file' : '/tmp/fakey',
                                  'log_level' : 'debug',
                              },
        }
        finder = influxgraph.InfluxDBFinder(config)
        self.assertTrue(finder)

    def test_create_root_log_file_should_fail(self):
        _config = { 'influxdb' : { 'host' : 'localhost',
                                   'port' : 8086,
                                   'user' : 'root',
                                   'pass' : 'root',
                                   'db' : self.db_name,
                                   'log_file' : '/' + str(uuid.uuid4()),
                                   'log_level' : 'debug',
                               },
        }
        finder = influxgraph.InfluxDBFinder(_config)
        self.assertTrue(finder)


if __name__ == '__main__':
    unittest.main()
