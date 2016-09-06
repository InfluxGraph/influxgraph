# Copyright (C) [2015-] [Thomson Reuters LLC]
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
except ImportError:
    pass
import datetime
from influxdb import InfluxDBClient
import logging
from logging.handlers import TimedRotatingFileHandler
from graphite_api.node import BranchNode
from graphite_api.utils import is_pattern
from graphite_api.finders import match_entries
from ..constants import INFLUXDB_AGGREGATIONS, _INFLUXDB_CLIENT_PARAMS, \
     SERIES_LOADER_MUTEX_KEY, LOADER_LIMIT
from ..utils import NullStatsd, normalize_config, \
     calculate_interval, read_influxdb_values, get_aggregation_func, \
     gen_memcache_key, gen_memcache_pattern_key, Query, get_retention_policy
from .reader import InfluxdbReader
from .metric_lookup import MetricLookup
from .leaf import InfluxDBLeafNode
import threading
import time
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
                 'deltas', 'loader', 'retention_policies', 'metric_lookup')
    
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
        self.retention_policies = config.get('retention_policies', None)
        logger.debug("Configured aggregation functions - %s",
                     self.aggregation_functions,)
        # self.loader = self._start_loader(series_loader_interval)
        self.metric_lookup = MetricLookup(self.client, self.memcache, self.memcache_ttl)
        self.metric_lookup.build_index()
        # self.metric_lookup.start_background_refresh()

    def _start_loader(self, series_loader_interval):
        if self.memcache:
            # No memcached configured? Cannot use series loader
            # Run series loader in main thread if due to run to not allow
            # requests to be served before series loader has completed at least once.
            if self.memcache.get(SERIES_LOADER_MUTEX_KEY):
                logger.debug("Series loader mutex exists %s - "
                             "skipping series load",
                             SERIES_LOADER_MUTEX_KEY)
            else:
                self.memcache.set(SERIES_LOADER_MUTEX_KEY, 1,
                                  time=series_loader_interval)
                for _ in self.get_all_series_list():
                    pass
            loader = threading.Thread(target=self._series_loader,
                                      kwargs={'interval': series_loader_interval})
            loader.daemon = True
            loader.start()
        else:
            loader = None
        return loader
    
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

    def get_all_cached_series(self, pattern, limit=LOADER_LIMIT, offset=0):
        """Retrieve all pages of series list from cache only"""
        logger.debug("Finding cached series list for pattern %s, "
                     "limit %s, offset %s", pattern, limit, offset)
        _memcache_key = gen_memcache_pattern_key("_".join([
            pattern, str(limit), str(offset)]))
        series = self.memcache.get(_memcache_key)
        if not series:
            return []
        if len(series) < limit:
            return series
        return series + self.get_all_cached_series(pattern, limit=limit,
                                                   offset=limit+offset)
    
    def _get_parent_branch_series(self, query, limit=LOADER_LIMIT, offset=0):
        """Iterate through parent branches, find cached series for parent
        branch and return series matching query"""
        root_branch_query = False
        _pattern = ".".join(query.pattern.split('.')[:-1])
        if not _pattern:
            _pattern = '*'
            root_branch_query = True
        parent_branch_series = self.get_all_cached_series(
            _pattern, limit=limit, offset=offset)
        while not parent_branch_series and not root_branch_query:
            logger.debug("No cached series list for parent branch query %s, "
                         "continuing with more parents", _pattern)
            _pattern = ".".join(_pattern.split('.')[:-1])
            if not _pattern:
                _pattern = '*'
                root_branch_query = True
            parent_branch_series = self.get_all_cached_series(
                _pattern, limit=limit, offset=offset)
        if not parent_branch_series:
            return
        logger.debug("Found cached parent branch series for parent query %s "
                     "limit %s offset %s",
                     _pattern, limit, offset)
        series = match_entries(parent_branch_series, query.pattern) \
          if is_pattern(query.pattern) \
          else [b for b in parent_branch_series if
                b.startswith(query.pattern)]
        return series
    
    def get_series(self, query, cache=True, limit=LOADER_LIMIT, offset=0):
        """Retrieve series names from InfluxDB according to query pattern
        
        :param query: Query to run to get series names
        :type query: :mod:`graphite_api.storage.FindQuery` compatible class
        """
        memcache_key = gen_memcache_pattern_key("_".join([
            query.pattern, str(limit), str(offset)]))
        cached_series = self.memcache.get(memcache_key) \
          if self.memcache and cache else None
        if cached_series is not None:
            logger.debug("Found cached series for query %s, limit %s, " \
                         "offset %s", query.pattern, limit, offset)
            return cached_series
        if self.memcache and cache and not query.pattern == '*' \
          and not cached_series:
            cached_series_from_parents = self._get_parent_branch_series(
                query, limit=limit, offset=offset)
            if cached_series_from_parents is not None:
                logger.debug("Found cached series from parent branches for "
                             "query %s, limit %s, offset %s",
                             query.pattern, limit, offset)
                self.memcache.set(memcache_key, cached_series_from_parents,
                                  time=self.memcache_ttl,
                                  min_compress_len=50)
                return cached_series_from_parents
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
            self.memcache.set(memcache_key, series, time=self.memcache_ttl,
                              min_compress_len=50)
        return series

    def _get_series(self, limit=LOADER_LIMIT, offset=0):
        memcache_key = gen_memcache_pattern_key("_".join([
            '*', str(limit), str(offset)]))
        _query = "SHOW SERIES LIMIT %s OFFSET %s" % (limit, offset,)
        logger.debug("Series loader calling influxdb with query - %s", _query)
        data = self.client.query(_query, params=_INFLUXDB_CLIENT_PARAMS)
        # >= 0.11 show series API
        if data.keys() and 'results' in data.keys()[0]:
            series = [d.get('key') for k in data for d in k
                      if d]
        else:
            series = [key_name for (key_name, _) in data.keys()]
        if self.memcache:
            self.memcache.set(memcache_key, series, time=self.memcache_ttl,
                              min_compress_len=50)
        return series

    def _make_series_query(self, query, limit=LOADER_LIMIT, offset=0):
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

    def _store_last_offset(self, query, limit, offset):
        if offset and self.memcache:
            # Store empty list at offset+last limit to indicate
            # that this is the last page
            last_offset = offset + limit
            logger.debug("Pagination finished for query pattern %s "
                         "- storing empty array for limit %s and "
                         "last offset %s",
                         query.pattern, limit, offset,)
            memcache_key = gen_memcache_pattern_key("_".join([
                query.pattern, str(limit), str(last_offset)]))
            self.memcache.set(memcache_key, [], time=self.memcache_ttl)

    def get_all_series(self, query, cache=True,
                       limit=LOADER_LIMIT, offset=0, _data=None):
        """Retrieve all series for query"""
        data = self.get_series(
            query, cache=cache, limit=limit, offset=offset)
        return self._pagination_runner(data, query, self.get_all_series, cache=cache,
                                       limit=limit, offset=offset)
    
    def get_all_series_list(self, limit=LOADER_LIMIT, offset=0, _data=None,
                            *args, **kwargs):
        """Retrieve all series for series loader"""
        query = Query('*')
        data = self._get_series(limit=limit, offset=offset)
        return self._pagination_runner(data, query, self.get_all_series_list,
                                       limit=limit, offset=offset)
    
    def _pagination_runner(self, data, query, get_series_func,
                           limit=None, offset=None, _data=None,
                           *args, **kwargs):
        if not _data:
            _data = []
        if data:
            if len(data) < limit:
                self._store_last_offset(query, limit, offset)
                return _data + data
            if len(data) > limit:
                return data
            offset = limit + offset
            return data + get_series_func(
                query=query, *args, limit=limit, offset=offset,
                _data=_data, **kwargs)
        self._store_last_offset(query, limit, offset)
        return data

    def make_regex_string(self, query):
        """Make InfluxDB regex strings from Graphite wildcard queries"""
        if not is_pattern(query.pattern):
            return "^%s" % (query.pattern,)
        if query.pattern == '*':
            return
        pat = "^%s" % (query.pattern.replace('.', r'\.').replace(
            '*', '([a-zA-Z0-9-_:#]+(\.)?)+').replace(
                '{', '(').replace(',', '|').replace('}', ')'))
        if not self.is_suffix_pattern(query.pattern):
            return "%s$" % (pat)
        return pat
    
    def _series_loader(self, interval=900):
        """Loads influxdb series list into memcache at a rate of no
        more than once per interval
        """
        pattern = '*'
        query = Query(pattern)
        while True:
            if self.memcache.get(SERIES_LOADER_MUTEX_KEY):
                logger.debug("Series loader mutex exists %s - "
                             "skipping series load",
                             SERIES_LOADER_MUTEX_KEY)
                time.sleep(interval)
                continue
            self.memcache.set(SERIES_LOADER_MUTEX_KEY, 1, time=interval)
            start_time = datetime.datetime.now()
            logger.debug("Starting series list loader..")
            try:
                for _ in self.get_all_series_list():
                    pass
            except Exception as ex:
                logger.error("Error calling InfluxDB from series loader - %s",
                             ex,)
                time.sleep(interval)
                continue
            dt = datetime.datetime.now() - start_time
            logger.debug("Series list loader finished in %s", dt)
            time.sleep(interval)
    
    def find_branch(self, split_pattern, split_path, path, pattern,
                    seen_branches):
        if path in seen_branches:
            return
        # Return root branch immediately for single wildcard query
        if pattern == '*':
            try:
                return_path = split_path[:1][0]
            except IndexError:
                return
            if return_path in seen_branches:
                return
            seen_branches.add(return_path)
            return return_path
        branch_no = len(split_pattern)
        try:
            return_path = split_path[branch_no-1:][0]
        except IndexError:
            return
        if return_path in seen_branches:
            return
        seen_branches.add(return_path)
        return return_path
    
    def is_suffix_pattern(self, pattern):
        """Check if query ends with wildcard"""
        return pattern.endswith('*') \
          or pattern.endswith('}')
    
    def is_leaf_node(self, split_pattern, split_path):
        """Check if path is a leaf node according to query"""
        branch_no = len(split_pattern)
        if len(split_path) == branch_no == 1:
            return False
        if len(split_path) > branch_no:
            return False
        return True


    def find_nodes(self, query):
        paths = self.metric_lookup.query(query.pattern)
        for path in paths:
            if path['is_leaf']:
                yield InfluxDBLeafNode(path['metric'], InfluxdbReader(
                    self.client, path['metric'], self.statsd_client,
                    aggregation_functions=self.aggregation_functions,
                    memcache_host=self.memcache_host,
                    memcache_max_value=self.memcache_max_value,
                    deltas=self.deltas))
            else:
                yield BranchNode(path['metric'])
    
    # def find_nodes(self, query, cache=True, limit=LOADER_LIMIT):
    #     """Find matching nodes according to query.
        
    #     :param query: Query to run to find either BranchNode(s) or LeafNode(s)
    #     :type query: :mod:`graphite_api.storage.FindQuery` compatible class
    #     """
    #     split_pattern = query.pattern.split('.')
    #     logger.debug("find_nodes() query %s", query.pattern)
    #     # TODO - need a way to determine if path is a branch or leaf node
    #     # prior to querying influxdb for data.
    #     # An InfluxDB query to check if a series exists is quite expensive
    #     # at ~3s with ~300k series so that is not an option.
    #     #
    #     # Perhaps storing found branches as <branch name>: 1 in memcache
    #     # could be used as a key/val lookup for is_this_path_here
    #     # a known branch.
    #     # if not is_pattern(query.pattern):
    #     #     import ipdb; ipdb.set_trace()
    #     #     if self.is_leaf_node(split_pattern, split_pattern):
    #     #         yield InfluxDBLeafNode(query.pattern, InfluxdbReader(
    #     #             self.client, query.pattern, self.statsd_client,
    #     #             aggregation_functions=self.aggregation_functions,
    #     #             memcache_host=self.memcache_host,
    #     #             memcache_max_value=self.memcache_max_value, deltas=self.deltas))
    #     #         raise StopIteration
    #     #     yield BranchNode(query.pattern)
    #     #     raise StopIteration
    #     timer_name = ".".join(['service_is_graphite-api',
    #                            'action_is_yield_nodes',
    #                            'target_type_is_gauge',
    #                            'unit_is_ms.what_is_query_duration'])
    #     timer = self.statsd_client.timer(timer_name)
    #     timer.start()
    #     series = list(set(self.get_all_series(query, cache=cache,
    #                                           limit=limit)))
    #     for node in self._get_nodes(series, query, split_pattern):
    #         yield node
    #     timer.stop()

    def _get_nodes(self, series, query, split_pattern):
        seen_branches = set()
        for path in series:
            split_path = path.split('.')
            if self.is_leaf_node(split_pattern, split_path):
                leaf_path_key = path + query.pattern
                yield InfluxDBLeafNode(path, InfluxdbReader(
                    self.client, path, self.statsd_client,
                    aggregation_functions=self.aggregation_functions,
                    memcache_host=self.memcache_host,
                    memcache_max_value=self.memcache_max_value,
                    deltas=self.deltas))
            else:
                branch = self.find_branch(split_pattern, split_path,
                                          path, query.pattern, seen_branches)
                if branch:
                    yield BranchNode(branch)
    
    def fetch_multi(self, nodes, start_time, end_time):
        """Fetch datapoints for all series between start and end times
        
        :param nodes: List of nodes to retrieve data for
        :type nodes: list(:mod:`graphite_influxdb.classes.InfluxDBLeafNode`)
        :param start_time: Start time of query
        :param end_time: End time of query
        """
        # paths = list(set([n.path for n in nodes]))
        paths = [n.path for n in nodes]
        interval = calculate_interval(start_time, end_time, deltas=self.deltas)
        retention = get_retention_policy(interval, self.retention_policies) \
          if self.retention_policies else "default"
        series = ', '.join(['"%s"."%s"' % (retention, path,) for path in paths])
        time_info = start_time, end_time, interval
        if not nodes:
            return time_info, {}
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
        # all requested paths even if they have no datapoints
        for key in paths:
            data.setdefault(key, [])
        for key in data:
            data[key] = [v for v in data[key]]
        if self.memcache:
            self.memcache.set(memcache_key, data,
                              time=interval,
                              min_compress_len=50)
        return time_info, data
