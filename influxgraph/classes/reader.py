# Copyright (C) [2015-2017] [Thomson Reuters LLC]
# Copyright (C) [2015-2017] [Panos Kittenis]

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Package containing InfluxdbReader class"""

from __future__ import absolute_import, print_function
import logging

from ..constants import _INFLUXDB_CLIENT_PARAMS
from ..utils import calculate_interval, read_influxdb_values, \
     get_aggregation_func, gen_memcache_key


logger = logging.getLogger('influxgraph')


class Interval(object):
    """No-op Interval class used by Graphite-API for whisper backends"""
    intervals = set()


class InfluxDBReader(object):
    """Graphite-Api reader class for InfluxDB.

    Retrieves a single metric series from InfluxDB
    """
    __slots__ = ('client', 'path', 'aggregation_functions',
                 'memcache', 'deltas', 'intervals')

    def __init__(self, client, path,
                 memcache=None,
                 aggregation_functions=None,
                 deltas=None):
        self.client = client
        self.path = path
        self.aggregation_functions = aggregation_functions
        self.memcache = memcache
        self.deltas = deltas
        self.intervals = Interval()

    def fetch(self, start_time, end_time):
        """Fetch single series' data from > start_time and <= end_time

        :param start_time: start_time in seconds from epoch
        :param end_time: end_time in seconds from epoch
        """
        interval = calculate_interval(start_time, end_time, deltas=self.deltas)
        time_info = start_time, end_time, interval
        aggregation_func = get_aggregation_func(
            self.path, self.aggregation_functions)
        logger.debug(
            "fetch() path=%s start_time=%s, end_time=%s, "
            "interval=%d, aggregation=%s",
            self.path, start_time, end_time, interval, aggregation_func)
        memcache_key = gen_memcache_key(start_time, end_time, aggregation_func,
                                        [self.path])
        data = self.memcache.get(memcache_key) if self.memcache else None
        if data and self.path in data:
            logger.debug("Found cached data for key %s", memcache_key)
            return time_info, data[self.path]
        _query = 'select %s(value) as value from "%s" where (time > %ds and ' \
                 'time <= %ds) GROUP BY time(%ss) fill(previous)' % (
                     aggregation_func, self.path, start_time,
                     end_time, interval)
        logger.debug("fetch() path=%s querying influxdb query: '%s'",
                     self.path, _query)
        data = self.client.query(_query, params=_INFLUXDB_CLIENT_PARAMS)
        logger.debug("fetch() path=%s returned data: %s", self.path, data)
        data = read_influxdb_values(data, [self.path], None)
        if self.memcache:
            self.memcache.set(
                memcache_key, data, time=interval, min_compress_len=50)
        return time_info, data.get(self.path, [])

    def get_intervals(self):
        """Noop function - Used for whisper backends but not
        needed for InfluxDB"""
        return self.intervals
