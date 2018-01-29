import unittest
from random import randint
import datetime

from influxgraph.influxdb import InfluxDBClient


class InfluxDBClientTest(unittest.TestCase):

    def setUp(self):
        self.client = InfluxDBClient('localhost', db='integration_test')
        self.client.drop_database(self.client.db)
        self.client.create_database(self.client.db)

    def test_query(self):
        query = 'SHOW SERIES LIMIT 1'
        data = self.client.query(query)
        self.assertIsNotNone(data)

    def test_create_drop_db(self):
        self.client.create_database(self.client.db)
        self.client.drop_database(self.client.db)
        self.client.create_database(self.client.db)

    def test_write(self):
        tag_val, field_val = "my_tag_val", 15
        data = """measure,mytag=%s myfield=%s""" % (tag_val, field_val)
        self.client.write(data, params={'precision': 's'})
        data = self.client.query(
            "SELECT * FROM measure where time > now() - 10m")
        self.assertIsNotNone(data)
        self.assertEqual(len(data), 1)
        self.assertEqual(len(data[0]['series']), 1)
        self.assertEqual(len(data[0]['series'][0]['values']), 1)
        self.assertEqual(len(data[0]['series'][0]['values'][0]), 3)
        _time, field, tag = data[0]['series'][0]['values'][0]
        self.assertEqual(field, field_val)
        self.assertEqual(tag, tag_val)
