from __future__ import print_function

from string import ascii_letters
from random import randint, choice
from influxgraph.influxdb import InfluxDBClient
from influxgraph.templates import parse_influxdb_graphite_templates
from influxgraph.classes.finder import InfluxDBFinder
from datetime import datetime
from time import sleep
from itertools import islice


def make_str(length=8):
    return u''.join([choice(ascii_letters) for _ in range(length)])


_templates = ["dc.env.host.measurement.field*"]

client = InfluxDBClient('localhost', db='perf_test')
client.drop_database(client.db)
client.create_database(client.db)

tags = [[u','.join(['='.join([tag, val])
                    for tag in ['host', 'dc', 'env']])
         for val in [u''.join([choice(ascii_letters)
                               for _ in range(4)])
                     for _ in range(10)]]]

print("Generating series..")

measurements = [u''.join([choice(ascii_letters) for _ in range(8)])
                for _ in range(1000)]

fields = ["%s=%s" % (".".join([make_str() for _ in range (100)]),
                     randint(1,100))
          for _ in range(100)]

all_series = [" ".join([tag_m, field])
              for tag_m in [u','.join([m, t])
                            for m in measurements
                            for _t in tags
                            for t in _t
                            for field in fields]
]

print("Writing to DB..")

step = 100000

for i in range(0, len(all_series), step):
    _slice = all_series[i:i+step]
    batch = u"\n".join(_slice)
    client.write(batch, params={'precision': 's'})

config = {'influxdb': {'host': 'localhost',
                       'templates': _templates,
                       'loader_startup_block': False}
}

finder = InfluxDBFinder(config)
start = datetime.now()
finder.build_index()
end = datetime.now()
print("Index build for %s series finished in %s" % (
    # None, end - start))
    len(all_series), end - start))
