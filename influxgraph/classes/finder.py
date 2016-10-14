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

import memcache
import datetime
from influxdb import InfluxDBClient
import logging
from logging.handlers import TimedRotatingFileHandler
from graphite_api.node import BranchNode
from graphite_api.utils import is_pattern
from graphite_api.finders import match_entries
from ..constants import INFLUXDB_AGGREGATIONS, _INFLUXDB_CLIENT_PARAMS, \
     SERIES_LOADER_MUTEX_KEY, LOADER_LIMIT, MEMCACHE_SERIES_DEFAULT_TTL, \
     DEFAULT_AGGREGATIONS
from ..utils import NullStatsd, calculate_interval, read_influxdb_values, \
     get_aggregation_func, gen_memcache_key, gen_memcache_pattern_key, \
     Query, get_retention_policy, _compile_aggregation_patterns
from ..templates import _parse_influxdb_graphite_templates, _split_series_with_tags
from .reader import InfluxDBReader
from .leaf import InfluxDBLeafNode
from .tree import NodeTreeIndex
import json
import threading
from multiprocessing import Lock as processLock
import time
try:
    import statsd
except ImportError:
    pass
_SERIES_LOADER_LOCK = processLock()

logger = logging.getLogger('graphite_influxdb')

class InfluxDBFinder(object):
    """Graphite-Api finder for InfluxDB.
    
    Finds and fetches metric series from InfluxDB.
    """
    __fetch_multi__ = 'influxdb'
    __slots__ = ('client', 'statsd_client', 'aggregation_functions',
                 'memcache', 'memcache_host', 'memcache_ttl', 'memcache_max_value',
                 'deltas', 'retention_policies', 'index', 'reader',
                 'index_lock', 'index_path', 'graphite_templates')
    
    def __init__(self, config):
        influxdb_config = config.get('influxdb', {})
        self.client = InfluxDBClient(influxdb_config.get('host', 'localhost'),
                                     influxdb_config.get('port', '8086'),
                                     influxdb_config.get('user', 'root'),
                                     influxdb_config.get('passw', 'root'),
                                     influxdb_config.get('db', 'graphite'),
                                     influxdb_config.get('ssl', 'false'),)
        try:
            self.statsd_client = statsd.StatsClient(influxdb_config['statsd'].get('host'),
                                                    influxdb_config['statsd'].get('port', 8125)) \
                if 'statsd' in influxdb_config and influxdb_config['statsd'].get('host') else NullStatsd()
        except NameError:
            logger.warning("Statsd client configuration present but 'statsd' module "
                           "not installed - ignoring statsd configuration..")
            self.statsd_client = NullStatsd()
        memcache_conf = influxdb_config.get('memcache', {})
        self.memcache_host = memcache_conf.get('host')
        self.memcache_ttl = memcache_conf.get('ttl', MEMCACHE_SERIES_DEFAULT_TTL)
        self.memcache_max_value = memcache_conf.get('max_value', 1)
        if self.memcache_host:
            self.memcache = memcache.Client(
                [self.memcache_host], pickleProtocol=-1,
                server_max_value_length=1024**2*self.memcache_max_value)
        else:
            self.memcache = None
        self._setup_logger(influxdb_config.get('log_level', 'info'),
                           influxdb_config.get('log_file', None))
        self.aggregation_functions = _compile_aggregation_patterns(
            influxdb_config.get('aggregation_functions', DEFAULT_AGGREGATIONS))
        series_loader_interval = influxdb_config.get('series_loader_interval', 900)
        reindex_interval = influxdb_config.get('reindex_interval', 900)
        self.deltas = influxdb_config.get('deltas', None)
        self.retention_policies = influxdb_config.get('retention_policies', None)
        logger.debug("Configured aggregation functions - %s",
                     self.aggregation_functions,)
        templates = influxdb_config.get('templates')
        self.graphite_templates = _parse_influxdb_graphite_templates(templates) \
            if templates else None
        self._start_loader(series_loader_interval)
        self.index = None
        self.index_lock = threading.Lock()
        self.index_path = config.get('search_index')
        # If memcache is not enabled build index on startup
        if not self.memcache:
            self.build_index()
        self.reader = InfluxDBReader(
            self.client, None, self.statsd_client,
            aggregation_functions=self.aggregation_functions,
            memcache_host=self.memcache_host,
            memcache_max_value=self.memcache_max_value,
            deltas=self.deltas)
        self._start_reindexer(reindex_interval)

    def _start_loader(self, series_loader_interval):
        # No memcached configured? Cannot use series loader
        if not self.memcache:
            return
        # Run series loader in main thread if due to run to not allow
        # requests to be served before series loader has completed at least once.
        if _SERIES_LOADER_LOCK.acquire(block=False):
            if self.memcache.get(SERIES_LOADER_MUTEX_KEY):
                logger.debug("Series loader mutex exists %s - "
                             "skipping series load",
                             SERIES_LOADER_MUTEX_KEY)
            else:
                logger.info("Starting initial series list load - this may "
                            "take several minutes on databases with a large "
                            "number of series..")
                self.memcache.set(SERIES_LOADER_MUTEX_KEY, 1,
                                  time=series_loader_interval)
                try:
                    for _ in self.get_all_series_list():
                        pass
                except Exception as ex:
                    logger.error("Error calling InfluxDB from initial series "
                                 "load - %s", ex)
                finally:
                    _SERIES_LOADER_LOCK.release()
        loader = threading.Thread(target=self._series_loader,
                                  kwargs={'interval': series_loader_interval})
        loader.daemon = True
        loader.start()
    
    def _start_reindexer(self, reindex_interval):
        self.load_index()
        if not self.index:
            self.build_index()
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
        logger.info("Starting background series loader with interval %s", interval)
        while True:
            time.sleep(interval)
            if _SERIES_LOADER_LOCK.acquire(block=False):
                _SERIES_LOADER_LOCK.release()
                if self.memcache.get(SERIES_LOADER_MUTEX_KEY):
                    logger.debug("Series loader mutex exists %s - "
                                 "skipping series load", SERIES_LOADER_MUTEX_KEY)
                    time.sleep(interval)
                    continue
            self.memcache.set(SERIES_LOADER_MUTEX_KEY, 1, time=interval)
            start_time = datetime.datetime.now()
            logger.debug("Starting series list loader..")
            _SERIES_LOADER_LOCK.acquire()
            try:
                for _ in self.get_all_series_list():
                    pass
            except Exception as ex:
                logger.error("Error calling InfluxDB from series loader - %s",
                             ex,)
                time.sleep(interval)
                continue
            finally:
                _SERIES_LOADER_LOCK.release()
            dt = datetime.datetime.now() - start_time
            logger.debug("Series list loader finished in %s", dt)
    
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

    def _gen_aggregation_func(self, paths):
        aggregation_funcs = list(set(get_aggregation_func(path, self.aggregation_functions)
                                     for path in paths))
        if len(aggregation_funcs) > 1:
            logger.warning("Got multiple aggregation functions %s for paths %s - Using '%s'",
                           aggregation_funcs, paths, aggregation_funcs[0])
        aggregation_func = aggregation_funcs[0]
        return aggregation_func

    def _gen_query_values_from_templates(self, paths, retention):
        _measurements = []
        _tags = []
        for path in paths:
            for (_filter, template, default_tags, separator) in self.graphite_templates:
                if _filter and not _filter.match(path):
                    continue
                matcher = template.match(path)
                if not matcher:
                    continue
                for key in matcher.groupdict():
                    if not key == 'measurement':
                        _tags.append((key, matcher.groupdict()[key]))
                for key,val in iter(default_tags.items()):
                    _tags.append((key, val))
                _measurements.append(matcher.groupdict()['measurement'])
        measurements = ', '.join(
            ('"%s"."%s"' % (retention, measure,) for measure in _measurements)) \
            if retention else ', '.join(('"%s"' % (measure,) for measure in _measurements))
        tags = "AND ".join([""""%s" = '%s' """ % (key,val,) for (key,val) in _tags]) \
          if _tags else None
        return measurements, tags

    def _gen_query_values(self, paths, retention):
        if self.graphite_templates:
            return self._gen_query_values_from_templates(paths, retention)
        tags = None
        measurement = ', '.join(('"%s"."%s"' % (retention, path,) for path in paths)) \
          if retention \
          else ', '.join(('"%s"' % (path,) for path in paths))
        return measurement, tags

    def _gen_influxdb_query(self, start_time, end_time, paths, interval):
        retention = get_retention_policy(interval, self.retention_policies) \
          if self.retention_policies else None
        aggregation_func = self._gen_aggregation_func(paths)
        memcache_key = gen_memcache_key(start_time, end_time, aggregation_func,
                                        paths)
        measurement, tags = self._gen_query_values(paths, retention)
        query = 'select %s("value") as value from %s where (time > %ds and time <= %ds) ' % (
            aggregation_func, measurement, start_time, end_time,)
        if tags:
            query += "AND %s" % (tags,)
        group_by = 'GROUP BY time(%ss)' % (interval,)
        query += group_by
        return query, memcache_key
    
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
        query, memcache_key = self._gen_influxdb_query(
                start_time, end_time, paths, interval)
        data = self.memcache.get(memcache_key) if self.memcache else None
        if data:
            logger.debug("Found cached data for key %s", memcache_key)
            return time_info, data
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
        data = read_influxdb_values(data, paths)
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
            time.sleep(interval)
            try:
                self.build_index()
            except Exception as ex:
                logger.error("Error occured in reindexing thread - %s", ex)
    
    def build_index(self, data=None):
        logger.info('Starting index build')
        try:
            data = self.get_all_series() if not data else data
        except Exception as ex:
            logger.error("Error getting series list from InfluxDB - %s -"
                         "Retrying after 30sec..", ex)
            time.sleep(30)
            return self.build_index()
        # data = self._read_static_data('series.json')
        logger.info("Building index..")
        index = NodeTreeIndex()
        for serie in data:
            # import ipdb; ipdb.set_trace()
            # If we have metrics with tags in them split them out and
            # pre-generate a correctly ordered split path for that metric
            # to be inserted into index
            if ',' in serie:
                index.insert_split_path(_split_series_with_tags(
                    serie, self.graphite_templates))
            else:
                index.insert(serie)
        self.index_lock.acquire()
        self.index = index
        logger.info("Finished building index")
        self.index_lock.release()
        del data
        self.save_index()
    
    def save_index(self):
        if not self.index_path:
            return
        try:
            with open(self.index_path, 'wt') as index_fh:
                index_fh.write(self.index.to_json())
        except IOError as ex:
            logger.error("Error writing to index file %s - %s",
                         self.index_path, ex)
            return
        logger.info("Wrote index file to %s", self.index_path)
    
    def load_index(self):
        if not self.index_path:
            return
        try:
            index_fh = open(self.index_path, 'rt')
        except IOError as ex:
            logger.error("Error reading index file %s - %s", self.index_path, ex)
            return
        except Exception as ex:
            logger.error("Error loading index from %s - %s", self.index_path, ex)
            return
        try:
            index = NodeTreeIndex.from_json(index_fh)
        except Exception as ex:
            logger.error("Error reading index file - %s", ex)
            return
        finally:
            index_fh.close()
        self.index = index
        logger.info("Loaded index from disk")
