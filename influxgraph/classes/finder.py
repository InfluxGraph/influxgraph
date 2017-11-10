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

"""Graphite-Api storage finder for InfluxDB.

Read metric series from an InfluxDB database via a Graphite-API storage plugin
compatible API.
"""

from __future__ import absolute_import, print_function
import json
import threading
from multiprocessing import Lock as processLock
import time
import datetime
import logging
from logging.handlers import WatchedFileHandler
from collections import deque

from influxdb import InfluxDBClient
from graphite_api.node import BranchNode
from ..constants import _INFLUXDB_CLIENT_PARAMS, \
     SERIES_LOADER_MUTEX_KEY, LOADER_LIMIT, MEMCACHE_SERIES_DEFAULT_TTL, \
     DEFAULT_AGGREGATIONS, _MEMCACHE_FIELDS_KEY, FILL_PARAMS, FILE_LOCK
from ..utils import calculate_interval, \
     get_aggregation_func, gen_memcache_key, gen_memcache_pattern_key, \
     get_retention_policy, _compile_aggregation_patterns, \
     make_memcache_client
from ..templates import parse_influxdb_graphite_templates, apply_template, \
     TemplateMatchError
try:
    from ..ext.templates import parse_series, read_influxdb_values
except ImportError:
    from ..utils import parse_series, read_influxdb_values
from .reader import InfluxDBReader
from .leaf import InfluxDBLeafNode
from .tree import NodeTreeIndex
from .lock import FileLock

_SERIES_LOADER_LOCK = processLock()

logger = logging.getLogger('influxgraph')


class InfluxDBFinder(object):
    """Graphite-Api finder for InfluxDB.

    Finds and fetches metric series from InfluxDB.
    """
    __fetch_multi__ = 'influxdb'
    __slots__ = ('client', 'aggregation_functions',
                 'memcache', 'memcache_host', 'memcache_ttl',
                 'memcache_series_loader_mutex_key', 'memcache_fields_key',
                 'deltas', 'retention_policies', 'index', 'reader',
                 'index_lock', 'index_path', 'graphite_templates',
                 'loader_limit', 'fill_param')

    def __init__(self, config):
        influxdb_config = config.get('influxdb', {})
        self.client = InfluxDBClient(influxdb_config.get('host', 'localhost'),
                                     influxdb_config.get('port', '8086'),
                                     influxdb_config.get('user', 'root'),
                                     influxdb_config.get('pass', 'root'),
                                     influxdb_config.get('db', 'graphite'),
                                     influxdb_config.get('ssl', 'false'),)
        self._setup_logger(influxdb_config.get('log_level', 'info'),
                           influxdb_config.get('log_file', None))
        memcache_conf = influxdb_config.get('memcache', {})
        memcache_host = memcache_conf.get('host')
        self.memcache_ttl = memcache_conf.get(
            'ttl', MEMCACHE_SERIES_DEFAULT_TTL)
        self.memcache_series_loader_mutex_key = memcache_conf.get(
            'series_loader_mutex_key', SERIES_LOADER_MUTEX_KEY)
        self.memcache_fields_key = memcache_conf.get(
            'fields_key', _MEMCACHE_FIELDS_KEY)
        self.memcache = make_memcache_client(
            memcache_host, memcache_max_value=memcache_conf.get('max_value', 1))
        self.aggregation_functions = _compile_aggregation_patterns(
            influxdb_config.get('aggregation_functions', DEFAULT_AGGREGATIONS))
        self.fill_param = influxdb_config.get('fill', 'null')
        if self.fill_param not in FILL_PARAMS and not (
                isinstance(self.fill_param, int) or isinstance(
                    self.fill_param, float)):
            raise Exception("Configured fill param %s is not a valid parameter "
                            "nor integer or float number", self.fill_param,)
        series_loader_interval = influxdb_config.get(
            'series_loader_interval', 900)
        reindex_interval = influxdb_config.get('reindex_interval', 900)
        self.loader_limit = influxdb_config.get('loader_limit', LOADER_LIMIT)
        if not isinstance(self.loader_limit, int):
            raise Exception("Configured loader limit %s is not an integer",
                            self.loader_limit)
        self.deltas = influxdb_config.get('deltas', None)
        self.retention_policies = influxdb_config.get(
            'retention_policies', None)
        logger.debug("Configured aggregation functions - %s",
                     self.aggregation_functions,)
        templates = influxdb_config.get('templates')
        self.graphite_templates = parse_influxdb_graphite_templates(templates) \
            if templates else None
        self._start_loader(series_loader_interval)
        self.index = None
        self.index_path = config.get('search_index')
        self.index_lock = FileLock(influxdb_config.get('index_lock_file',
                                                       FILE_LOCK))
        self.reader = InfluxDBReader(
            self.client, None,
            aggregation_functions=self.aggregation_functions,
            memcache=self.memcache,
            deltas=self.deltas)
        self._start_reindexer(reindex_interval)

    def _start_loader(self, series_loader_interval):
        # No memcached configured? Cannot use series loader
        if not self.memcache:
            return
        # Run series loader in main thread if due to run to not allow
        # requests to be served before series loader has completed at
        # least once.
        if _SERIES_LOADER_LOCK.acquire(block=False):
            if self.memcache.get(self.memcache_series_loader_mutex_key):
                logger.debug("Series loader mutex exists %s - "
                             "skipping series load",
                             self.memcache_series_loader_mutex_key)
            else:
                logger.info("Starting initial series list load - this may "
                            "take several minutes on databases with a large "
                            "number of series..")
                self.memcache.set(self.memcache_series_loader_mutex_key, 1,
                                  time=series_loader_interval)
                try:
                    if self.graphite_templates:
                        self.get_field_keys()
                    for _ in self.get_all_series_list():
                        pass
                except Exception as ex:
                    logger.error("Error calling InfluxDB from initial series "
                                 "and field list load - %s", ex)
                finally:
                    _SERIES_LOADER_LOCK.release()
        loader = threading.Thread(target=self._series_loader,
                                  kwargs={'interval': series_loader_interval})
        loader.daemon = True
        loader.start()

    def _start_reindexer(self, reindex_interval):
        new_index = False
        if not self.index:
            self.load_index()
        if not self.index:
            self.build_index()
            new_index = True
        logger.debug("Starting reindexer thread with interval %s",
                     reindex_interval)
        reindexer = threading.Thread(target=self._reindex,
                                     kwargs={'interval': reindex_interval,
                                             'new_index': new_index})
        reindexer.daemon = True
        reindexer.start()

    def _setup_logger(self, level, log_file):
        """Setup log level and log file if set"""
        if not level:
            return
        if logger.handlers:
            return
        if hasattr(logging, 'NullHandler'):
            logger.addHandler(logging.NullHandler())
        formatter = logging.Formatter(
            '[%(levelname)s] %(asctime)s - %(module)s.%(funcName)s() '
            '- %(message)s')
        level = getattr(logging, level.upper())
        logger.setLevel(level)
        handler = logging.StreamHandler()
        logger.addHandler(handler)
        handler.setFormatter(formatter)
        if not log_file:
            return
        try:
            _handler = WatchedFileHandler(log_file)
        except IOError:
            logger.error("Could not write to %s, falling back to stdout",
                         log_file)
        else:
            logger.addHandler(_handler)
            _handler.setFormatter(formatter)

    def get_series(self, cache=True, offset=0):
        """Retrieve series names from InfluxDB according to query pattern

        :param query: Query to run to get series names
        :type query: :mod:`graphite_api.storage.FindQuery` compatible class
        """
        memcache_key = gen_memcache_pattern_key("_".join([
            '*', str(self.loader_limit), str(offset)]))
        cached_series = self.memcache.get(memcache_key) \
            if self.memcache and cache else None
        if cached_series is not None:
            logger.debug("Found cached series for limit %s, "
                         "offset %s", self.loader_limit, offset)
            return cached_series
        series = self._get_series(offset=offset)
        if self.memcache:
            self.memcache.set(memcache_key, series, time=self.memcache_ttl,
                              min_compress_len=50)
        return series

    def _get_series(self, offset=0):
        memcache_key = gen_memcache_pattern_key("_".join([
            '*', str(self.loader_limit), str(offset)]))
        _query = "SHOW SERIES LIMIT %s OFFSET %s" % (self.loader_limit, offset,)
        logger.debug("Series loader calling influxdb with query - %s", _query)
        data = self.client.query(_query, params=_INFLUXDB_CLIENT_PARAMS)
        series = [d.get('key') for k in data for d in k if d]
        if self.memcache:
            self.memcache.set(memcache_key, series, time=self.memcache_ttl,
                              min_compress_len=50)
        return series

    def _store_last_offset(self, query_pattern, limit, offset):
        if offset and self.memcache:
            # Store empty list at offset+last limit to indicate
            # that this is the last page
            last_offset = offset + limit
            logger.debug("Pagination finished for query pattern %s "
                         "- storing empty array for limit %s and "
                         "last offset %s",
                         query_pattern, limit, offset,)
            memcache_key = gen_memcache_pattern_key("_".join([
                query_pattern, str(limit), str(last_offset)]))
            self.memcache.set(memcache_key, [], time=self.memcache_ttl)

    def get_all_series(self, cache=True,
                       offset=0, _data=None, **kwargs):
        """Retrieve all series"""
        # pylint: disable=unused-argument
        data = self.get_series(
            cache=cache, offset=offset)
        return self._pagination_runner(data, '*', self.get_all_series,
                                       limit=self.loader_limit,
                                       cache=cache,
                                       offset=offset)

    def get_all_series_list(self, offset=0, _data=None,
                            *args, **kwargs):
        """Retrieve all series for series loader"""
        # pylint: disable=unused-argument
        query_pattern = '*'
        data = self._get_series(offset=offset)
        return self._pagination_runner(
            data, query_pattern, self.get_all_series_list,
            limit=self.loader_limit, offset=offset)

    def _pagination_runner(self, data, query_pattern, get_series_func,
                           limit=None, offset=None, _data=None,
                           *args, **kwargs):
        if not _data:
            _data = []
        if data:
            if len(data) < limit:
                self._store_last_offset(query_pattern, limit, offset)
                return _data + data
            offset = limit + offset
            return data + get_series_func(
                *args, limit=limit, offset=offset,
                _data=_data, **kwargs)
        self._store_last_offset(query_pattern, limit, offset)
        return data

    def _series_loader(self, interval=900):
        """Loads influxdb series list into memcache at a rate of no
        more than once per interval
        """
        logger.info("Starting background series loader with interval %s",
                    interval)
        while True:
            time.sleep(interval)
            if _SERIES_LOADER_LOCK.acquire(block=False):
                _SERIES_LOADER_LOCK.release()
                if self.memcache.get(self.memcache_series_loader_mutex_key):
                    logger.debug("Series loader mutex exists %s - "
                                 "skipping series load",
                                 self.memcache_series_loader_mutex_key)
                    time.sleep(interval)
                    continue
            self.memcache.set(self.memcache_series_loader_mutex_key, 1,
                              time=interval)
            start_time = datetime.datetime.now()
            logger.debug("Starting series list loader..")
            _SERIES_LOADER_LOCK.acquire()
            try:
                if self.graphite_templates:
                    self.get_field_keys()
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
        """Find and return nodes matching query

        :param query: Query to search for
        :type query: :mod:`influxgraph.utils.Query`
        """
        node_paths = self.index.query(query.pattern)
        for path, node in node_paths:
            if node.is_leaf():
                # Set path on existing reader to avoid having to create
                # new objects for each path which is expensive
                # Reader is not used for queries when multi fetch is enabled
                # regardless
                self.reader.path = path
                yield InfluxDBLeafNode(path, self.reader)
            else:
                yield BranchNode(path)

    def _gen_aggregation_func(self, paths):
        aggregation_funcs = list(set(get_aggregation_func(
            path, self.aggregation_functions) for path in paths))
        if len(aggregation_funcs) > 1:
            logger.warning("Got multiple aggregation functions %s for paths %s "
                           "- Using '%s'",
                           aggregation_funcs, paths, aggregation_funcs[0])
        aggregation_func = aggregation_funcs[0]
        return aggregation_func

    def _get_template_values_from_paths(self, paths, _filter, template,
                                        default_tags, separator,
                                        measurement_data):
        _measurements = deque()
        _tags = {}
        _fields = deque()
        matched_paths = deque()
        for path in paths:
            if _filter and not _filter.match(path):
                continue
            try:
                measurement, tags, field = apply_template(
                    path.split('.'), template, default_tags, separator)
            except TemplateMatchError:
                continue
            if measurement not in _measurements:
                _measurements.append(measurement)
            for tag in tags:
                if tag not in _tags or tags[tag] not in _tags[tag]:
                    _tags.setdefault(tag, []).append(tags[tag])
            if not field:
                field = 'value'
            if field not in _fields:
                _fields.append(field)
            matched_paths.append(path)
            measurement_data.setdefault(measurement, {}).setdefault(
                'paths', []).append(path)
            if field not in measurement_data[measurement].setdefault(
                    'fields', []):
                measurement_data[measurement].setdefault(
                    'fields', []).append(field)
            measurement_data[measurement].setdefault(
                'template', template)
        return _measurements, _tags, _fields, matched_paths

    def _get_all_template_values(self, paths):
        paths = paths[:]
        measurement_data = {}
        measurements, tags, fields = deque(), deque(), set()
        for (_filter, template,
             default_tags, separator) in self.graphite_templates:
            # One influx measurement queried per template
            if not paths:
                break
            _measurements, _tags, \
                _fields, matched_paths = self._get_template_values_from_paths(
                    paths, _filter, template, default_tags, separator,
                    measurement_data)
            if _measurements:
                # Found template match for path, append query data and
                # remove matched paths so we do not try to match them again
                measurements.extend(_measurements)
                if _tags:
                    tags.append(_tags)
                fields = fields.union(_fields)
                for path in matched_paths:
                    del paths[paths.index(path)]
        return measurements, tags, fields, measurement_data

    def _gen_query(self, measurements, tags, fields, retention):
        groupings = set([k for t in tags for k in t.keys()])
        measurements = ', '.join(
            ('"%s"."%s"' % (retention, measure,) for measure in measurements)) \
            if retention \
            else ', '.join(('"%s"' % (measure,) for measure in measurements))
        _tags = ' OR '.join(['(%s)' % (tag_set,) for tag_set in [
            ' AND '.join(['(%s)' % ' OR '.join([
                """"%s" = '%s'""" % (tag, tag_val,)
                for tag_val in __tags[tag]])
                          for tag in __tags])
            for __tags in tags]]) if tags else None
        fields = fields if fields else ['value']
        return measurements, _tags, fields, groupings

    def _gen_query_values_from_templates(self, paths, retention):
        measurements, tags, fields, measurement_data = \
          self._get_all_template_values(paths)
        measurements, tags, fields, groupings = self._gen_query(
            measurements, tags, fields, retention)
        return measurements, tags, fields, groupings, measurement_data

    def _gen_query_values(self, paths, retention):
        if self.graphite_templates:
            return self._gen_query_values_from_templates(paths, retention)
        measurement = ', '.join(('"%s"."%s"' % (retention, path,)
                                 for path in paths)) if retention \
                      else ', '.join(('"%s"' % (path,)
                                      for path in paths))
        return measurement, None, ['value'], None, None

    def _gen_infl_stmt(self, measurements, tags, fields, groupings, start_time,
                       end_time, aggregation_func, interval):
        time_clause = "(time > %ds and time <= %ds)" % (start_time, end_time,)
        query_fields = ', '.join(['%s("%s") as "%s"' % (
            aggregation_func, field, field) for field in fields])
        groupings = ['"%s"' % (grouping,) for grouping in groupings] \
            if groupings else []
        groupings.insert(0, 'time(%ss)' % (interval,))
        groupings = ', '.join(groupings)
        where_clause = "%s AND %s" % (time_clause, tags,) if tags else \
                       time_clause
        group_by = '%s fill(%s)' % (groupings, self.fill_param,)
        query = 'select %s from %s where %s GROUP BY %s' % (
            query_fields, measurements, where_clause, group_by,)
        return query

    def _gen_influxdb_stmt(self, start_time, end_time, paths, interval,
                           aggregation_func):
        retention = get_retention_policy(interval, self.retention_policies) \
                    if self.retention_policies else None
        measurements, tags, fields, \
            groupings, measurement_data = self._gen_query_values(
                paths, retention)
        query = self._gen_infl_stmt(measurements, tags, fields, groupings,
                                    start_time, end_time, aggregation_func,
                                    interval)
        return query, measurement_data

    def _make_empty_multi_fetch_result(self, time_info, paths):
        data = {}
        for key in paths:
            data[key] = []
        return time_info, data

    def fetch_multi(self, nodes, start_time, end_time):
        """Fetch datapoints for all series between start and end times

        :param nodes: List of nodes to retrieve data for
        :type nodes: list(:mod:`influxgraph.classes.InfluxDBLeafNode`)
        :param start_time: Start time of query
        :param end_time: End time of query
        """
        interval = calculate_interval(start_time, end_time, deltas=self.deltas)
        time_info = start_time, end_time, interval
        if not nodes:
            return time_info, {}
        paths = sorted([n.path for n in nodes if n.is_leaf])
        if not len(paths) > 0:
            return self._make_empty_multi_fetch_result(
                time_info, [n.path for n in nodes])
        aggregation_func = self._gen_aggregation_func(paths)
        memcache_key = gen_memcache_key(start_time, end_time, aggregation_func,
                                        paths)
        data = self.memcache.get(memcache_key) if self.memcache else None
        if data:
            logger.debug("Found cached data for key %s", memcache_key)
            return time_info, data
        logger.debug('fetch_multi() - start_time: %s - '
                     'end_time: %s, interval %s',
                     datetime.datetime.fromtimestamp(float(start_time)),
                     datetime.datetime.fromtimestamp(float(end_time)), interval)
        try:
            query, measurement_data = self._gen_influxdb_stmt(
                start_time, end_time, paths, interval, aggregation_func)
        except TypeError as ex:
            logger.error("Type error generating query statement - %s", ex)
            return self._make_empty_multi_fetch_result(time_info, paths)
        data = self._run_infl_query(query, paths, measurement_data)
        # Do not cache empty responses
        if self.memcache and sum([len(vals) for vals in data.values()]) > 0:
            self.memcache.set(memcache_key, data,
                              time=interval,
                              min_compress_len=50)
        return time_info, data

    def _run_infl_query(self, query, paths, measurement_data):
        logger.debug("Calling influxdb multi fetch with query - %s", query)
        data = self.client.query(query, params=_INFLUXDB_CLIENT_PARAMS)
        logger.debug('fetch_multi() - Retrieved %d result set(s)', len(data))
        data = read_influxdb_values(data, paths, measurement_data)
        # Graphite API requires that data contain keys for
        # all requested paths even if they have no datapoints
        for key in paths:
            data.setdefault(key, [])
        return data

    def _read_static_data(self, data_file):
        data = json.load(open(data_file))['results'][0]['series'][0]['values']
        return [d for k in data for d in k if d]

    def _reindex(self, new_index=False, interval=900):
        """Perform re-index"""
        save_thread = threading.Thread(target=self.save_index)
        if new_index:
            save_thread.start()
        del new_index
        while True:
            time.sleep(interval)
            try:
                save_thread.join()
            except RuntimeError:
                pass
            finally:
                del save_thread
            try:
                self.build_index()
            except Exception as ex:
                logger.error("Error occured in reindexing thread - %s", ex)
            save_thread = threading.Thread(target=self.save_index)
            save_thread.start()

    def build_index(self, data=None, separator=b'.'):
        """Build new node tree index

        :param data: (Optional) data to use to build index
        :type data: list
        """
        logger.info('Starting index build')
        try:
            data = self.get_all_series() if not data else data
        except Exception as ex:
            logger.error("Error getting series list from InfluxDB - %s -"
                         "Retrying after 30sec..", ex)
            time.sleep(30)
            return self.build_index()
        all_fields = self.get_field_keys() if self.graphite_templates \
            else None
        with self.index_lock:
            logger.info("Building index..")
            start_time = datetime.datetime.now()
            index = parse_series(data, all_fields, self.graphite_templates,
                                 separator=separator)
            self.index = index
        logger.info("Finished building index in %s",
                    datetime.datetime.now() - start_time)

    def _save_index_file(self, file_h):
        """Dump tree contents to file handle"""
        json.dump(self.index.to_array(), file_h)

    def save_index(self):
        """Save index to file"""
        if not self.index_path:
            return
        if not (hasattr(self, 'index') and self.index
                and hasattr(self.index, 'to_array')):
            return
        logger.info("Saving index to file %s", self.index_path,)
        start_time = datetime.datetime.now()
        try:
            index_fh = open(self.index_path, 'wt')
            self._save_index_file(index_fh)
        except IOError as ex:
            logger.error("Error writing to index file %s - %s",
                         self.index_path, ex)
            return
        except Exception as ex:
            logger.error("Error saving index file %s - %s",
                         self.index_path, ex)
            raise
        else:
            index_fh.close()
        dt = datetime.datetime.now() - start_time
        logger.info("Wrote index file to %s in %s", self.index_path, dt)

    def load_index(self):
        """Load index from file"""
        if not self.index_path:
            return
        if not (self.index and hasattr(self.index, 'from_file')):
            return
        logger.info("Loading index from file %s", self.index_path,)
        try:
            index_fh = open(self.index_path, 'rt')
        except Exception as ex:
            logger.error("Error reading index file %s - %s",
                         self.index_path, ex)
            return
        try:
            index = NodeTreeIndex.from_file(index_fh)
        except Exception as ex:
            logger.error("Error loading index file - %s", ex)
            return
        finally:
            index_fh.close()
        self.index = index
        logger.info("Loaded index from disk")

    def get_field_keys(self):
        """Get field keys for all measurements"""
        field_keys = self.memcache.get(self.memcache_fields_key) \
            if self.memcache else None
        if field_keys:
            logger.debug("Found cached field keys")
            return field_keys
        logger.debug("Calling InfluxDB for field keys")
        data = self.client.query('SHOW FIELD KEYS')
        field_keys = {}
        for ((key, _), vals) in data.items():
            field_keys[key] = [val['fieldKey'] for val in vals]
        if self.memcache:
            if not self.memcache.set(self.memcache_fields_key, field_keys,
                                     time=self.memcache_ttl,
                                     min_compress_len=1):
                logger.error("Could not add field key list to memcache - "
                             "likely field list size over max memcache value")
        return field_keys
