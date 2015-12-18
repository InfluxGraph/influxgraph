# Copyright (C) [2015-] [Panos Kittenis]
# Copyright (C) [2014-2015] [Vimeo, LLC]

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Graphite-Api storage finder for InfluxDB.

Read metric series from an InfluxDB database via a Graphite compatible API.
"""

import datetime
from influxdb import InfluxDBClient
import logging
from logging.handlers import TimedRotatingFileHandler
import re
import sys
from graphite_api.node import BranchNode
from ..constants import INFLUXDB_AGGREGATIONS, _INFLUXDB_CLIENT_PARAMS
from ..utils import NullStatsd, normalize_config, \
     calculate_interval, read_influxdb_values, get_aggregation_func, \
     gen_memcache_key, gen_memcache_pattern_key
from .reader import InfluxdbReader
from .leaf import InfluxDBLeafNode
try:
    import statsd
except ImportError:
    pass
try:
    import memcache
except ImportError:
    pass

logger = logging.getLogger('graphite_influxdb')

class InfluxdbFinder(object):
    """Graphite-Api finder for InfluxDB.
    
    Finds and fetches metric series from InfluxDB.
    """
    __fetch_multi__ = 'influxdb'
    __slots__ = ('client', 'config', 'statsd_client', 'aggregation_functions',
                 'memcache', 'memcache_host', 'memcache_ttl', 'memcache_max_value',
                 'deltas')

    def __init__(self, config):
        config = normalize_config(config)
        self.config = config
        self.client = InfluxDBClient(config.get('host', 'localhost'),
                                     config.get('port', '8086'),
                                     config.get('user', 'root'),
                                     config.get('passw', 'root'),
                                     config['db'],
                                     config.get('ssl', 'false'),)
        try:
            self.statsd_client = statsd.StatsClient(config['statsd'].get('host'),
                                                    config['statsd'].get('port', 8125)) \
                if 'statsd' in config and config['statsd'].get('host') else NullStatsd()
        except NameError:
            logger.warning("Statsd client configuration present but 'statsd' module "
                           "not installed - ignoring statsd configuration..")
            self.statsd_client = NullStatsd()
        self.memcache_host = config.get('memcache_host', None)
        self.memcache_ttl = config['memcache_ttl']
        self.memcache_max_value = config.get('memcache_max_value', 15)
        if self.memcache_host:
            self.memcache = memcache.Client(
                [self.memcache_host], pickleProtocol=-1,
                server_max_value_length=1024*1014*self.memcache_max_value)
        else:
            self.memcache = None
        self._setup_logger(config['log_level'], config['log_file'])
        self.aggregation_functions = config.get('aggregation_functions', None)
        self.deltas = config.get('deltas', None)
        logger.debug("Configured aggregation functions - %s", self.aggregation_functions,)

    def _setup_logger(self, level, log_file):
        """Setup log level and log file if set"""
        if logger.handlers:
            return
        level = getattr(logging, level.upper())
        logger.setLevel(level)
        formatter = logging.Formatter(
            '[%(levelname)s] %(asctime)s - %(module)s.%(funcName)s() - %(message)s')
        handler = logging.StreamHandler()
        logger.addHandler(handler)
        handler.setFormatter(formatter)
        if not log_file:
            return
        try:
            handler = TimedRotatingFileHandler(log_file)
        except IOError:
            logger.error("Could not write to %s, falling back to stdout",
                         log_file)
        else:
            logger.addHandler(handler)
            handler.setFormatter(formatter)

    def get_series(self, query):
        """Retrieve series names from InfluxDB according to query pattern
        
        :param query: Query to run to get series names
        :type query: :mod:`graphite_api.storage.FindQuery` compatible class
        """
        memcache_key = gen_memcache_pattern_key(query.pattern)
        cached_series = self.memcache.get(memcache_key) \
          if self.memcache else None
        if cached_series:
            logger.debug("Found cached series for query %s", query.pattern)
            return cached_series
        # regexes in influxdb are not assumed to be anchored, so anchor them
        # explicitly
        regex = self.compile_regex('^{0}', query)
        timer_name = ".".join(['service_is_graphite-api',
                               'ext_service_is_influxdb',
                               'target_type_is_gauge',
                               'unit_is_ms',
                               'action_is_get_series'])
        _query = "show series from /%s/" % (regex.pattern,)
        logger.debug("get_series() Calling influxdb with query - %s", _query)
        timer = self.statsd_client.timer(timer_name)
        timer.start()
        data = self.client.query(_query, params=_INFLUXDB_CLIENT_PARAMS)
        series = [key_name for (key_name, _) in data.keys()]
        timer.stop()
        if self.memcache:
            self.memcache.set(memcache_key,
                              series,
                              time=self.memcache_ttl,
                              min_compress_len=50)
        return (s for s in series)

    def compile_regex(self, fmt, query):
        r"""Turn glob (graphite) queries into compiled regex.
        
        * becomes .*
        . becomes \.
        fmt argument is so that caller can control
        anchoring (must contain exactly one {0} !
        """
        return re.compile(fmt.format(
            query.pattern.replace('.', r'\.').replace('*', r'[^\.]*').replace(
                '{', '(').replace(',', '|').replace('}', ')')
        ))

    def get_branch(self, path, regex, seen_branches):
        """Get branch from path matching regex
        
        :param path: Metric path to find branch for
        :type path: str
        :param regex: Regular expression that branch must match
        :type regex: :mod: `re` pattern
        :param seen_branches: Set of previously seen branches from the query
        :type seen_branches: set()
        """
        while '.' in path:
            path = path.rsplit('.', 1)[0]
            if path not in seen_branches:
                seen_branches.add(path)
                if regex.match(path):
                    logger.debug("get_branch() %s found branch name: %s",
                                 regex.pattern, path)
                    return path
    
    def find_nodes(self, query):
        """Find matching nodes according to query.
        
        :param query: Query to run to find either BranchNode(s) or LeafNode(s)
        :type query: :mod:`graphite_api.storage.FindQuery` compatible class
        """
        logger.debug("find_nodes() query %s", query)
        timer_name = ".".join(['service_is_graphite-api',
                               'action_is_yield_nodes',
                               'target_type_is_gauge',
                               'unit_is_ms.what_is_query_duration'])
        timer = self.statsd_client.timer(timer_name)
        timer.start()
        regex = self.compile_regex('^{0}$', query)
        series = self.get_series(query)
        seen_branches = set()
        for path in series:
            if regex.match(path):
                yield InfluxDBLeafNode(path, InfluxdbReader(
                    self.client, path, self.statsd_client,
                    aggregation_functions=self.aggregation_functions,
                    memcache_host=self.memcache_host,
                    deltas=self.deltas))
            else:
                branch = self.get_branch(
                    path, regex, seen_branches)
                if branch:
                    yield BranchNode(branch)
        timer.stop()
    
    def fetch_multi(self, nodes, start_time, end_time):
        """Fetch datapoints for all series between start and end times
        
        :param nodes: List of nodes to retrieve data for
        :type nodes: list(:mod:`graphite_influxdb.classes.InfluxDBLeafNode`)
        :param start_time: Start time of query
        :param end_time: End time of query
        """
        paths = list(set([n.path for n in nodes]))
        series = ', '.join(['"%s"' % path for path in paths])
        interval = calculate_interval(start_time, end_time, deltas=self.deltas)
        time_info = start_time, end_time, interval
        aggregation_funcs = list(set(get_aggregation_func(path, self.aggregation_functions)
                                     for path in paths))
        if len(aggregation_funcs) > 1:
            logger.warning("Got multiple aggregation functions %s for paths %s - Using '%s'",
                           aggregation_funcs, paths, aggregation_funcs[0])
        aggregation_func = aggregation_funcs[0]
        memcache_key = gen_memcache_key(start_time, end_time, aggregation_func,
                                        paths)
        data = self.memcache.get(memcache_key) if self.memcache else None
        if data:
            logger.debug("Found cached data for key %s", memcache_key)
            return time_info, data
        query = 'select %s(value) as value from %s where (time > %ds and time <= %ds) GROUP BY time(%ss)' % (
            aggregation_func, series, start_time, end_time, interval,)
        logger.debug('fetch_multi() query: %s', query)
        logger.debug('fetch_multi() - start_time: %s - end_time: %s, interval %s',
                     datetime.datetime.fromtimestamp(float(start_time)),
                     datetime.datetime.fromtimestamp(float(end_time)), interval)
        timer_name = ".".join([
            'service_is_graphite-api', 'ext_service_is_influxdb',
            'target_type_is_gauge', 'unit_is_ms', 'action_is_select_datapoints']
            )
        timer = self.statsd_client.timer(timer_name)
        timer.start()
        logger.debug("Calling influxdb multi fetch with query - %s", query)
        data = self.client.query(query, params=_INFLUXDB_CLIENT_PARAMS)
        logger.debug('fetch_multi() - Retrieved %d result set(s)', len(data))
        data = read_influxdb_values(data)
        timer.stop()
        # Graphite API requires that data contain keys for
        # all requested paths even if they have no data
        query_keys = set([node.path for node in nodes])
        for key in query_keys:
            data.setdefault(key, [])
        for key in data:
            data[key] = [v for v in data[key]]
        if self.memcache:
            self.memcache.set(memcache_key, data,
                              time=interval,
                              min_compress_len=50)
        return time_info, data
