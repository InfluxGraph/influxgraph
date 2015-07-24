# Copyright (C) [2014-2015] [Vimeo, LLC]
# Copyright (C) [2015-] [Panos Kittenis]

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

Read metric series from an InfluxDB database via a Graphite compatible API."""

import datetime
from influxdb import InfluxDBClient
import logging
from logging.handlers import TimedRotatingFileHandler
import re
import sys
from graphite_api.node import LeafNode, BranchNode
try:
    import statsd
except ImportError:
    pass

logger = logging.getLogger('graphite_influxdb')
# logging.basicConfig()
# logger.setLevel(logging.DEBUG)

# Tell influxdb to return time as seconds from epoch
_INFLUXDB_CLIENT_PARAMS = {'epoch': 's'}


class NullStatsd(object):
    """Fake StatsClient compatible class to use when statsd is not configured"""

    def __enter__(self):
        return self

    def __exit__(self, _type, value, traceback):
        pass

    def timer(self, key, val=None):
        return self

    def timing(self, key, val):
        pass

    def start(self):
        pass

    def stop(self):
        pass


def normalize_config(config):
    cfg = config.get('influxdb', None)
    ret = {}
    if not cfg:
        logger.critical("Missing required 'influxdb' configuration in graphite-api"
                        "config - please update your configuration file to include"
                        "at least 'influxdb: db: <db_name>'")
        sys.exit(1)
    ret['host'] = cfg.get('host', 'localhost')
    ret['port'] = cfg.get('port', 8086)
    ret['user'] = cfg.get('user', 'graphite')
    ret['passw'] = cfg.get('pass', 'graphite')
    ret['db'] = cfg.get('db', 'graphite')
    ssl = cfg.get('ssl', False)
    ret['ssl'] = (ssl == 'true')
    ret['schema'] = cfg.get('schema', [])
    ret['log_file'] = cfg.get('log_file', None)
    ret['log_level'] = cfg.get('log_level', 'info')
    cfg = config.get('es', {})
    if config.get('statsd', None):
        ret['statsd'] = config.get('statsd')
    return ret


def _make_graphite_api_points_list(influxdb_data):
    """Make graphite-api data points dictionary from Influxdb ResultSet data"""
    _data = {}
    for key in influxdb_data.keys():
        _data[key[0]] = [(datetime.datetime.fromtimestamp(float(d['time'])),
                          d['value']) for d in influxdb_data.get_points(key[0])]
    return _data


class InfluxdbReader(object):
    """Graphite-Api reader class for InfluxDB.
    
    Retrieves a single metric series from InfluxDB
    """
    __slots__ = ('client', 'path', 'step', 'statsd_client')

    def __init__(self, client, path, step, statsd_client):
        self.client = client
        self.path = path
        self.step = step
        self.statsd_client = statsd_client

    def fetch(self, start_time, end_time):
        """Fetch single series' data from > start_time and <= end_time
        
        :param start_time: start_time in seconds from epoch
        :param end_time: end_time in seconds from epoch
        """
        logger.debug("fetch() path=%s start_time=%s, end_time=%s, step=%d", self.path, start_time, end_time, self.step)
        timer_name = ".".join(['service_is_graphite-api',
                               'ext_service_is_influxdb',
                               'target_type_is_gauge',
                               'unit_is_ms',
                               'what_is_query_individual_duration'])
        with self.statsd_client.timer(timer_name):
            _query = 'select mean(value) as value from "%s" where (time > %ds and time <= %ds) GROUP BY time(%ss)' % (
                self.path, start_time, end_time, self.step)
            logger.debug("fetch() path=%s querying influxdb query: '%s'", self.path, _query)
            data = self.client.query(_query, params=_INFLUXDB_CLIENT_PARAMS)
        logger.debug("fetch() path=%s returned data: %s", self.path, data)
        try:
            data = _make_graphite_api_points_list(data)
        except Exception:
            logger.debug("fetch() path=%s COULDN'T READ POINTS. SETTING TO EMPTY LIST", self.path)
            data = []
        time_info = start_time, end_time, self.step
        return time_info, (v[1] for v in data[self.path])
    
    def get_intervals(self):
        """Noop function - Used by Graphite-Web but not needed for Graphite-Api"""
        pass


class InfluxLeafNode(LeafNode):
    """Tell Graphite-Api that our leaf node supports multi-fetch"""
    __fetch_multi__ = 'influxdb'


class InfluxdbFinder(object):
    """Graphite-Api finder for InfluxDB.
    
    Finds and fetches metric series from InfluxDB.
    """
    __fetch_multi__ = 'influxdb'
    __slots__ = ('client', 'schemas', 'config', 'statsd_client')

    def __init__(self, config=None):
        config = normalize_config(config)
        self.config = config
        self.client = InfluxDBClient(config.get('host', 'localhost'),
                                     config.get('port', '8086'),
                                     config.get('user', 'root'),
                                     config.get('passw', 'root'),
                                     config['db'],
                                     config.get('ssl', 'false'),)
        self.schemas = [(re.compile(patt), step) for (patt, step) in config['schema']]
        try:
            self.statsd_client = statsd.StatsClient(config['statsd'].get('host'),
                                                    config['statsd'].get('port', 8125)) \
                if 'statsd' in config and config['statsd'].get('host') else NullStatsd()
        except NameError:
            logger.warning("Statsd client configuration present but 'statsd' module"
                           "not installed - ignoring statsd configuration..")
            self.statsd_client = NullStatsd()
        self._setup_logger(config['log_level'], config['log_file'])

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
        # regexes in influxdb are not assumed to be anchored, so anchor them explicitly
        regex = self.compile_regex('^{0}', query)
        timer_name = ".".join(['service_is_graphite-api',
                               'ext_service_is_influxdb',
                               'target_type_is_gauge',
                               'unit_is_ms',
                               'action_is_get_series'])
        with self.statsd_client.timer(timer_name):
            _query = "show series from /%s/" % regex.pattern
            logger.debug("get_series() Calling influxdb with query - %s", _query)
            data = self.client.query(_query, params=_INFLUXDB_CLIENT_PARAMS)
            # as long as influxdb doesn't have good safeguards against
            # series with bad data in the metric names, we must filter out
            # like so:
            series = [key_name for (key_name, _) in data.keys()]
        return series

    def compile_regex(self, fmt, query):
        r"""Turn glob (graphite) queries into compiled regex.
        
        * becomes .*
        . becomes \.
        fmt argument is so that caller can control anchoring (must contain exactly one {0} !
        """
        return re.compile(fmt.format(
            query.pattern.replace('.', r'\.').replace('*', r'[^\.]*').replace(
                '{', '(').replace(',', '|').replace('}', ')')
        ))

    def get_leaves(self, query):
        """Get LeafNode according to query pattern

        :param query: Query to run to get LeafNodes
        :type query: :mod:`graphite_api.storage.FindQuery` compatible class
        """
        key_leaves = "%s_leaves" % query.pattern
        series = self.get_series(query)
        regex = self.compile_regex('^{0}$', query)
        logger.debug("get_leaves() key %s", key_leaves)
        timer_name = ".".join(['service_is_graphite-api',
                               'action_is_find_leaves',
                               'target_type_is_gauge',
                               'unit_is_ms'])
        timer = self.statsd_client.timer(timer_name)
        now = datetime.datetime.now()
        timer.start()
        # return every matching series and its
        # resolution (based on first pattern match in schema, fallback to 60s)
        leaves = [(name, next((res for (patt, res) in self.schemas if patt.match(name)), 60))
                  for name in series if regex.match(name)
                  ]
        timer.stop()
        end = datetime.datetime.now()
        dt = end - now
        logger.debug("get_leaves() key %s Finished find_leaves in %s.%ss",
                     key_leaves,
                     dt.seconds,
                     dt.microseconds)
        return leaves

    def get_branches(self, query):
        """Get branches according to query.

        :param query: Query to run to get BranchNodes
        :type query: :mod:`graphite_api.storage.FindQuery` compatible class
        """
        seen_branches = set()
        key_branches = "%s_branches" % query.pattern
        # Very inefficient call to list
        series = self.get_series(query)
        regex = self.compile_regex('^{0}$', query)
        logger.debug("get_branches() %s", key_branches)
        timer_name = ".".join(['service_is_graphite-api',
                               'action_is_find_branches',
                               'target_type_is_gauge',
                               'unit_is_ms'])
        timer = self.statsd_client.timer(timer_name)
        start_time = datetime.datetime.now()
        timer.start()
        branches = []
        for name in series:
            while '.' in name:
                name = name.rsplit('.', 1)[0]
                if name not in seen_branches:
                    seen_branches.add(name)
                    if regex.match(name) is not None:
                        logger.debug("get_branches() %s found branch name: %s", key_branches, name)
                        branches.append(name)
        timer.stop()
        end_time = datetime.datetime.now()
        dt = end_time - start_time
        logger.debug("get_branches() %s Finished find_branches in %s.%ss",
                     key_branches,
                     dt.seconds, dt.microseconds)
        return branches

    def find_nodes(self, query):
        """Find matching nodes according to query.

        :param query: Query to run to find either BranchNode(s) or LeafNode(s)
        :type query: :mod:`graphite_api.storage.FindQuery` compatible class
        """
        logger.debug("find_nodes() query %s", query)
        # TODO(retention_periods): once we can query influx better for retention periods,
        # honour the start/end time in the FindQuery object
        timer_name = ".".join(['service_is_graphite-api',
                               'action_is_yield_nodes',
                               'target_type_is_gauge',
                               'unit_is_ms.what_is_query_duration'])
        with self.statsd_client.timer(timer_name):
            for (name, res) in self.get_leaves(query):
                yield InfluxLeafNode(name, InfluxdbReader(
                    self.client, name, res, self.statsd_client))
            for name in self.get_branches(query):
                logger.debug("Yielding branch %s", name,)
                yield BranchNode(name)

    def fetch_multi(self, nodes, start_time, end_time):
        """Fetch datapoints for all series between start and end times

        :param nodes: List of nodes to retrieve data for
        :type nodes: list(:mod:`graphite_influxdb.InfluxdbLeafNode`)
        :param start_time: Start time of query
        :param end_time: End time of query
        """
        series = ', '.join(['"%s"' % node.path for node in nodes])
        # use the step of the node that is the most coarse
        # not sure if there's a better way? can we combine series
        # with different steps (and use the optimal step for each?)
        # probably not
        step = max([node.reader.step for node in nodes])
        query = 'select mean(value) as value from %s where (time > %ds and time <= %ds) GROUP BY time(%ss)' % (
                series, start_time, end_time, step)
        logger.debug('fetch_multi() query: %s', query)
        logger.debug('fetch_multi() - start_time: %s - end_time: %s, step %s',
                     datetime.datetime.fromtimestamp(float(start_time)),
                     datetime.datetime.fromtimestamp(float(end_time)), step)
        timer_name = ".".join(['service_is_graphite-api',
                               'ext_service_is_influxdb',
                               'target_type_is_gauge',
                               'unit_is_ms',
                               'action_is_select_datapoints'])
        with self.statsd_client.timer(timer_name):
            logger.debug("Calling influxdb multi fetch with query - %s", query)
            data = self.client.query(query, params=_INFLUXDB_CLIENT_PARAMS)
        logger.debug('fetch_multi() - Retrieved %d result set(s)', len(data))
        data = _make_graphite_api_points_list(data)
        # some series we requested might not be in the resultset.
        # this is because influx doesn't include series that had no values
        # this is a behavior that some people actually appreciate when graphing,
        # but graphite doesn't do this (yet),
        # and we want to look the same, so we must add those back in.
        # a better reason though, is because for advanced alerting cases like bosun,
        # you want all entries even if they have no data, so you can properly
        # compare, join, or do logic with the targets returned for requests for the
        # same data but from different time ranges, you want them to all
        # include the same keys.
        query_keys = set([node.path for node in nodes])
        for key in query_keys:
            data.setdefault(key, [])
        time_info = start_time, end_time, step
        for key in data:
            data[key] = (v[1] for v in data[key])
        return time_info, data
