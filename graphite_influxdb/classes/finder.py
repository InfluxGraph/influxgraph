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

Read metric series from an InfluxDB database via a Graphite-API storage plugin
compatible API.
"""

try:
    import memcache
    import gevent
    import gevent.monkey
except ImportError:
    pass
else:
    gevent.monkey.patch_socket()
    gevent.monkey.patch_select()
import datetime
from influxdb import InfluxDBClient
import logging
from logging.handlers import TimedRotatingFileHandler
import re
import sys
from graphite_api.node import BranchNode
from graphite_api.utils import is_pattern
from ..constants import INFLUXDB_AGGREGATIONS, _INFLUXDB_CLIENT_PARAMS
from ..utils import NullStatsd, normalize_config, \
     calculate_interval, read_influxdb_values, get_aggregation_func, \
     gen_memcache_key, gen_memcache_pattern_key, Query
from .reader import InfluxdbReader
from .leaf import InfluxDBLeafNode
try:
    import statsd
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
                 'deltas', 'leaf_paths', 'branch_paths', 'compiled_queries',
                 'loader')

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
        self.memcache_max_value = config.get('memcache_max_value', 1)
        if self.memcache_host:
            self.memcache = memcache.Client(
                [self.memcache_host], pickleProtocol=-1,
                server_max_value_length=1024**2*self.memcache_max_value)
        else:
            self.memcache = None
        self._setup_logger(config['log_level'], config['log_file'])
        self.aggregation_functions = config.get('aggregation_functions', None)
        series_loader_interval = config.get('series_loader_interval', 900)
        self.deltas = config.get('deltas', None)
        self.leaf_paths = set()
        self.branch_paths = {}
        self.compiled_queries = {}
        if self.memcache:
            # No memcached configured? No need for series loader
            self.loader = gevent.spawn(self._series_loader,
                                       interval=series_loader_interval)
        logger.debug("Configured aggregation functions - %s",
                     self.aggregation_functions,)
        gevent.sleep(0)

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

    def get_series(self, query, cache=True, limit=500, offset=0):
        """Retrieve series names from InfluxDB according to query pattern
        
        :param query: Query to run to get series names
        :type query: :mod:`graphite_api.storage.FindQuery` compatible class
        """
        memcache_key = gen_memcache_pattern_key("_".join([
            query.pattern, str(limit), str(offset)]))
        cached_series = self.memcache.get(memcache_key) \
          if self.memcache and cache else None
        if cached_series:
            logger.debug("Found cached series for query %s", query.pattern)
            return cached_series
        timer_name = ".".join(['service_is_graphite-api',
                               'ext_service_is_influxdb',
                               'target_type_is_gauge',
                               'unit_is_ms',
                               'action_is_get_series'])
        _query, _params = self._make_series_query(
            query, limit=limit, offset=offset)
        _query = _query % _params
        logger.debug("get_series() Calling influxdb with query - %s", _query)
        timer = self.statsd_client.timer(timer_name)
        timer.start()
        data = self.client.query(_query, params=_INFLUXDB_CLIENT_PARAMS)
        series = [d['name'] for d in data['measurements']]
        timer.stop()
        if self.memcache:
            self.memcache.set(memcache_key,
                              series,
                              time=self.memcache_ttl,
                              min_compress_len=50)
        return series

    def _make_series_query(self, query, limit=500, offset=0):
        regex_string = self.make_regex_string(query)
        _query = "SHOW MEASUREMENTS"
        _params = {}
        if regex_string:
            _query += " WITH measurement =~ /%(regex)s/"
            _params['regex'] = regex_string
        if limit or offset:
            _params['limit'] = limit
            _params['offset'] = offset
            _query += " LIMIT %(limit)s OFFSET %(offset)s"
        return _query, _params

    def get_all_series(self, query, cache=True, limit=500, offset=0, _data=None):
        data = self.get_series(
            query, cache=cache, limit=limit, offset=offset)
        if data:
            offset = limit + offset
            if not _data:
                _data = []
            return data + self.get_all_series(
                query, cache=cache, limit=limit, offset=offset, _data=_data)
        return _data

    def make_regex_string(self, query):
        """Make InfluxDB regex strings from Graphite wildcard queries"""
        if not is_pattern(query.pattern):
            return query.pattern
        if query.pattern == '*':
            return '^[a-zA-Z0-9-_:#]+\.'
        pat = "^%s" % (query.pattern.replace('.', r'\.').replace(
            '*', '([a-zA-Z0-9-_:#]+(\.)?)+').replace(
                '{', '(').replace(',', '|').replace('}', ')'))
        if not self.is_wildcard_suffix_query(query):
            return "%s$" % (pat)
        return pat
    
    def _series_loader(self, interval=900):
        """Loads influxdb series list into memcache at a rate of no
        more than once a minute
        """
        series_loader_mutex_key = 'graphite_influxdb_series_loader'
        pattern = '*'
        query = Query(pattern)
        while True:
            if self.memcache.get(series_loader_mutex_key):
                logger.debug("Series loader mutex exists %s - "
                             "skipping series load",
                             series_loader_mutex_key)
                gevent.sleep(interval)
                continue
            self.memcache.set(series_loader_mutex_key, 1, time=60)
            start_time = datetime.datetime.now()
            logger.debug("Starting series list loader..")
            [b for b in self.find_nodes(query, cache=False)]
            dt = datetime.datetime.now() - start_time
            logger.debug("Series list loader finished in %s", dt)
            gevent.sleep(interval)

    def get_branch(self, path, query, seen_branches):
        if not is_pattern(query.pattern):
            return
        if path in seen_branches:
            return
        # Return root branch immediately for single wildcard query
        if query.pattern == '*':
            return_path = path[:path.find('.')]
            if return_path in seen_branches:
                return
            seen_branches.add(return_path)
            return return_path
        pattern_no_expansion = query.pattern.replace('{', '').replace('}', '')
        query_path_prefix_ind = pattern_no_expansion.rfind('.')
        query_path_prefix = pattern_no_expansion[:query_path_prefix_ind] \
          if query_path_prefix_ind else None
        prefix_ind = path.rfind('.')
        if query_path_prefix:
            if not path.startswith(query_path_prefix):
                return
        return_path = path[:prefix_ind]
        if return_path in seen_branches:
            return
        seen_branches.add(return_path)
        return return_path

    def is_wildcard_suffix_query(self, query):
        """Check if query ends with wildcard"""
        return query.pattern.endswith('*') \
          or query.pattern.endswith('}')
    
    def find_leaf_node(self, path):
        return path[path.rfind('.')+1:]
    
    def find_nodes(self, query, cache=True):
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
        series = self.get_all_series(query, cache=cache)
        seen_branches = set()
        for path in series:
            leaf_path_key = path + query.pattern
            if not query.pattern == '*' \
              and ('.' in query.pattern or ('.' in query.pattern \
                   and self.is_wildcard_suffix_query(query))) \
              and (not is_pattern(query.pattern)
                   or self.is_wildcard_suffix_query(query)) \
                   or leaf_path_key in self.leaf_paths:
                leaf = self.find_leaf_node(path)
                self.leaf_paths.add(leaf_path_key)
                yield InfluxDBLeafNode(path, InfluxdbReader(
                    self.client, path, self.statsd_client,
                    aggregation_functions=self.aggregation_functions,
                    memcache_host=self.memcache_host,
                    memcache_max_value=self.memcache_max_value,
                    deltas=self.deltas))
            else:
                if path in self.branch_paths:
                    yield BranchNode(self.branch_paths[path])
                else:
                    branch = self.get_branch(path, query, seen_branches)
                    if branch:
                        branches = self.branch_paths.get(path, set(branch))
                        branches.add(branch)
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
