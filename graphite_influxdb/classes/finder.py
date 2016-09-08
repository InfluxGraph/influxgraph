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
from .leaf import InfluxDBLeafNode
from .tree import NodeTreeIndex
import json
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
                 'deltas', 'retention_policies', 'index', 'reader',
                 'index_lock')
    
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
        reindex_interval = config.get('reindex_interval', 900)
        self.deltas = config.get('deltas', None)
        self.retention_policies = config.get('retention_policies', None)
        logger.debug("Configured aggregation functions - %s",
                     self.aggregation_functions,)
        self._start_loader(series_loader_interval)
        self.index = NodeTreeIndex()
        self.index_lock = threading.Lock()
        self.reader = InfluxdbReader(
            self.client, None, self.statsd_client,
            aggregation_functions=self.aggregation_functions,
            memcache_host=self.memcache_host,
            memcache_max_value=self.memcache_max_value,
            deltas=self.deltas)
        self._start_reindexer(reindex_interval)

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

    def _start_reindexer(self, reindex_interval):
        logger.debug("Starting reindexer thread with interval %s", reindex_interval)
        reindexer = threading.Thread(target=self._reindex,
                                     kwargs={'interval': reindex_interval})
        reindexer.daemon = True
        reindexer.start()
    
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
    
    def get_series(self, cache=True, limit=LOADER_LIMIT, offset=0):
        """Retrieve series names from InfluxDB according to query pattern
        
        :param query: Query to run to get series names
        :type query: :mod:`graphite_api.storage.FindQuery` compatible class
        """
        memcache_key = gen_memcache_pattern_key("_".join([
            '*', str(limit), str(offset)]))
        cached_series = self.memcache.get(memcache_key) \
          if self.memcache and cache else None
        if cached_series is not None:
            logger.debug("Found cached series for limit %s, "
                         "offset %s", limit, offset)
            return cached_series
        timer_name = ".".join(['service_is_graphite-api',
                               'ext_service_is_influxdb',
                               'target_type_is_gauge',
                               'unit_is_ms',
                               'action_is_get_series'])
        timer = self.statsd_client.timer(timer_name)
        timer.start()
        series = self._get_series(limit=limit, offset=offset)
        # data = self.client.query(_query, params=_INFLUXDB_CLIENT_PARAMS)
        # series = [d['name'] for d in data['measurements']]
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

    def get_all_series(self, cache=True,
                       limit=LOADER_LIMIT, offset=0, _data=None):
        """Retrieve all series for query"""
        data = self.get_series(
            cache=cache, limit=limit, offset=offset)
        return self._pagination_runner(data, Query('*'), self.get_all_series, cache=cache,
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
                *args, limit=limit, offset=offset,
                _data=_data, **kwargs)
        self._store_last_offset(query, limit, offset)
        return data
    
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
        
    def find_nodes(self, query):
        paths = self.index.query(query.pattern)
        for path in paths:
            if path['is_leaf']:
                # Set path on existing reader to avoid having to create
                # new objects for each path which is expensive
                # Reader is not used for queries when multi fetch is enabled
                # regardless
                self.reader.path = path['metric']
                yield InfluxDBLeafNode(path['metric'], self.reader)
            else:
                yield BranchNode(path['metric'])
    
    def fetch_multi(self, nodes, start_time, end_time):
        """Fetch datapoints for all series between start and end times
        
        :param nodes: List of nodes to retrieve data for
        :type nodes: list(:mod:`graphite_influxdb.classes.InfluxDBLeafNode`)
        :param start_time: Start time of query
        :param end_time: End time of query
        """
        interval = calculate_interval(start_time, end_time, deltas=self.deltas)
        time_info = start_time, end_time, interval
        if not nodes:
            return time_info, {}
        paths = [n.path for n in nodes]
        retention = get_retention_policy(interval, self.retention_policies) \
          if self.retention_policies else "default"
        series = ', '.join(('"%s"."%s"' % (retention, path,) for path in paths))
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

    def _read_static_data(self, data_file):
        data = json.load(open(data_file))['results'][0]['series'][0]['values']
        return (d for k in data for d in k if d)

    def _reindex(self, interval=900):
        while True:
            self.build_index()
            time.sleep(interval)
    
    def build_index(self, data=None):
        logger.info('Starting index build')
        data = self.get_all_series() if not data else data
        # data = self._read_static_data('series.json')
        logger.info("Building index..")
        index = NodeTreeIndex()
        for metric in data:
            index.insert(metric)
        self.index_lock.acquire()
        self.index = index
        logger.info("Finished building index")
        self.index_lock.release()
        del data
