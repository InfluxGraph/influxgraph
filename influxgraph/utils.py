# Copyright (C) [2015-] [Thomson Reuters LLC]
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

"""InfluxGraph utility functions"""

from __future__ import absolute_import, print_function
import datetime
import sys
import re
import hashlib

import memcache
from .constants import INFLUXDB_AGGREGATIONS
try:
    from .ext.classes.tree import NodeTreeIndex
    from .ext.templates import get_series_with_tags
except ImportError:
    from .classes.tree import NodeTreeIndex
    from .templates import get_series_with_tags

def calculate_interval(start_time, end_time, deltas=None):
    """Calculates wanted data series interval according to start and end times

    Returns interval in seconds
    :param start_time: Start time in seconds from epoch
    :param end_time: End time in seconds from epoch
    :type start_time: int
    :type end_time: int
    :param deltas: Delta configuration to use. Defaults hardcoded if no
    configuration is provided
    :type deltas: dict(max time range of query in seconds: interval to use in seconds)

    :rtype: int - *Interval in seconds*
    """
    time_delta = end_time - start_time
    deltas = deltas if deltas else {
        # 15 min -> 10s
        900: 10,
        # 30 min -> 30s
        1800: 30,
        # # 1 hour -> 1s
        # 3600 : 1,
        # # 1 day -> 30s
        # 86400 : 30,
        # 3 days -> 1min
        259200: 60,
        # 7 days -> 5min
        604800: 300,
        # 14 days -> 10min
        1209600: 600,
        # 28 days -> 15min
        2419200: 900,
        # 2 months -> 30min
        4838400: 1800,
        # 4 months -> 1hour
        9676800: 3600,
        # 12 months -> 3hours
        31536000: 7200,
        # 4 years -> 12hours
        126144000: 43200,
        }
    for delta in sorted(deltas.keys()):
        if time_delta <= delta:
            return deltas[delta]
    # 1 day default, or if time range > max configured (4 years default max)
    return 86400

def get_retention_policy(interval, retention_policies):
    """Get appropriate retention policy for interval provided

    :param interval: Interval of query in seconds
    :type interval: int
    :param retention_policies: Retention policy configuration
    :type retention_policies: dict(max time range of interval in seconds: retention policy name)

    :rtype: ``str`` or ``None``
    """
    if not retention_policies:
        return
    for retention_interval in sorted(retention_policies.keys()):
        if interval <= retention_interval:
            return retention_policies[retention_interval]
    # In the case that desired interval is beyond configured interval range,
    # return policy for max interval
    return retention_policies[max(sorted(retention_policies.keys()))]

class Query(object):
    """Graphite-API compatible query class"""

    def __init__(self, pattern):
        self.pattern = pattern


class NullStatsd(object):
    """Fake StatsClient compatible class to use when statsd is not configured"""

    def __enter__(self):
        return self

    def __exit__(self, _type, value, traceback):
        pass

    def timer(self, key, val=None):
        """No-Op"""
        return self

    def timing(self, key, val):
        """No-Op"""
        pass

    def start(self):
        """No-Op"""
        pass

    def stop(self):
        """No-Op"""
        pass

def _compile_aggregation_patterns(aggregation_functions):
    """Compile aggregation function patterns to compiled regex objects"""
    if not aggregation_functions:
        return
    compiled_aggregations = {}
    for pattern in aggregation_functions.keys():
        if not aggregation_functions[pattern] in INFLUXDB_AGGREGATIONS:
            sys.stderr.write("Requested aggregation function '%s' is not a valid InfluxDB "
                             "aggregation function - ignoring..\n" % (
                                 aggregation_functions[pattern],))
            continue
        try:
            compiled_aggregations[re.compile(r'%s' % (pattern,))] = aggregation_functions[pattern]
        except re.error:
            sys.stderr.write("Error compiling regex pattern '%s' - ignoring..\n" % (
                pattern,))
    return compiled_aggregations

def get_aggregation_func(path, aggregation_functions):
    """Lookup aggregation function for path, if any.
    Defaults to 'mean'.

    :param path: Path to lookup
    :type path: str
    :param aggregation_functions: Aggregation function configuration
    :type aggregation_functions: dict(<pattern>: <compiled regex>)
    """
    if not aggregation_functions:
        return 'mean'
    for pattern in aggregation_functions:
        if pattern.search(path):
            return aggregation_functions[pattern]
    return 'mean'

def _find_metric_name(measurement_paths, tag_sets, field, fields):
    for tag_set in tag_sets:
        for path in measurement_paths:
            if field in fields \
            and field in path \
            and len([t for t in tag_set if t in path]) == len(tag_set):
                del measurement_paths[measurement_paths.index(path)]
                return path

def _retrieve_named_field_data(infl_data, path_measurements, measurement, _data):
    measurement_paths = path_measurements[measurement]['paths'][:]
    tag_sets = path_measurements[measurement]['tags'][:]
    field_keys = next(infl_data.get_points(measurement)).keys()
    point_fields = sorted([k for k in field_keys if k != 'time'])
    for field in point_fields:
        metric = _find_metric_name(
            measurement_paths, tag_sets, field,
            path_measurements[measurement]['fields'])
        if not metric:
            continue
        _data[metric] = [d[field]
                         for d in infl_data.get_points(measurement)]
    path_measurements[measurement]['paths'] = measurement_paths

def _retrieve_field_data(infl_data, path_measurements, measurement,
                         metric, _data):
    # Retrieve value field data
    if 'value' in  path_measurements[measurement]['fields']:
        _data[metric] = [d['value']
                         for d in infl_data.get_points(measurement)]
    # Retrieve non value named field data with fields from path_measurements
    _retrieve_named_field_data(infl_data, path_measurements,
                                measurement, _data)

def read_influxdb_values(influxdb_data, paths, path_measurements):
    """Return key -> values dict for values from InfluxDB data"""
    _data = {}
    if not isinstance(influxdb_data, list):
        influxdb_data = [influxdb_data]
    m_path_ind = 0
    seen_measurements = set()
    for infl_data in influxdb_data:
        for infl_keys in infl_data.keys():
            measurement = infl_keys[0]
            if not path_measurements:
                if not measurement in paths:
                    continue
                _data[measurement] = [d['value']
                                      for d in infl_data.get_points(measurement)]
                continue
            elif not measurement in path_measurements:
                continue
            if measurement not in seen_measurements:
                seen_measurements = set(tuple(seen_measurements) + (measurement,))
                m_path_ind = 0
            elif m_path_ind >= len(path_measurements[measurement]['paths']):
                m_path_ind = 0
            metric = path_measurements[measurement]['paths'][m_path_ind]
            m_path_ind += 1
            if metric not in paths:
                continue
            _retrieve_field_data(infl_data, path_measurements,
                                 measurement, metric, _data)
    return _data

def gen_memcache_pattern_key(pattern):
    """Generate memcache key from pattern"""
    return hashlib.md5(pattern.encode('utf8')).hexdigest()

def gen_memcache_key(start_time, end_time, aggregation_func, paths):
    """Generate memcache key to use to cache request data"""
    start_time_dt, end_time_dt = datetime.datetime.fromtimestamp(float(start_time)), \
      datetime.datetime.fromtimestamp(float(end_time))
    td = end_time_dt - start_time_dt
    delta = (td.microseconds + (td.seconds + td.days * 24 * 3600) * 10**6) / 10**6
    key_prefix = hashlib.md5("".join(paths).encode('utf8')).hexdigest()
    return "".join([key_prefix, aggregation_func, str(delta)]).encode('utf8')

def make_memcache_client(memcache_host, memcache_max_value=1):
    """Make memcache client if given a memcache host or `None`"""
    if not memcache_host:
        return
    return memcache.Client(
        [memcache_host], pickleProtocol=-1,
        server_max_value_length=1024**2*memcache_max_value)

def parse_series(series, fields, graphite_templates, separator='.'):
    """Parses series and fields with/without graphite templates
    and returns built Index

    :param series: Series to load
    :type series: list(unicode str)
    :param fields: Per measurement field keys from InfluxDB. May be `None`
    :type fields: dict(measurement: [field1, field2, ..])
    :param graphite_templates: Graphite templates to use to parse series
    and fields.
    :type graphite_templates: list(tuple) as returned by \
    :mod:`influxgraph.templates.parse_influxdb_graphite_templates`

    :rtype: :mod:`influxgraph.classes.tree.NodeTreeIndex`
    """
    index = NodeTreeIndex()
    for serie in series:
        # If we have metrics with tags in them split them out and
        # pre-generate a correctly ordered split path for that metric
        # to be inserted into index
        if graphite_templates or ',' in serie:
            serie_with_tags = serie.split(',')
            if graphite_templates:
                for split_path in get_series_with_tags(
                        serie_with_tags, fields, graphite_templates,
                        separator=separator):
                    index.insert_split_path(split_path)
            # Series with tags and no templates,
            # add only measurement to index
            else:
                index.insert(serie_with_tags[0])
        # No tags, no template
        else:
            index.insert(serie)
    return index
