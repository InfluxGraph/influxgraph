import uuid
import unittest
import graphite_influxdb
from influxdb import InfluxDBClient

class GraphiteInfluxDBLogFileConfigTestCase(unittest.TestCase):

    def setUp(self):
        self.db_name = 'fakey'
        self.client = InfluxDBClient(database=self.db_name)
        self.client.create_database(self.db_name)

    def tearDown(self):
        self.client.drop_database(self.db_name)
    
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
        finder = graphite_influxdb.InfluxdbFinder(_config)
        self.assertTrue(finder)

if __name__ == '__main__':
    unittest.main()
