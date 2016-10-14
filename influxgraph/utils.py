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

import datetime
import sys
import re
import datetime
import hashlib
from .constants import INFLUXDB_AGGREGATIONS, DEFAULT_AGGREGATIONS, \
     MEMCACHE_SERIES_DEFAULT_TTL

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
        # # 1 hour -> 1s
        # 3600 : 1,
        # # 1 day -> 30s
        # 86400 : 30,
        # 3 days -> 1min
        259200 : 60,
        # 7 days -> 5min
        604800 : 300,
        # 14 days -> 10min
        1209600 : 600,
        # 28 days -> 15min
        2419200 : 900,
        # 2 months -> 30min
        4838400 : 1800,
        # 4 months -> 1hour
        9676800 : 3600,
        # 12 months -> 3hours
        31536000 : 7200,
        # 4 years -> 12hours
        126144000 : 43200,
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

    def __init__(self, pattern):
        self.pattern = pattern


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

def read_influxdb_values(influxdb_data, paths):
    """Return generator for values from InfluxDB data"""
    _data = {}
    for i in range(len(influxdb_data.keys())):
        key = influxdb_data.keys()[i]
        _data[paths[i]] = (d['value'] for d in influxdb_data.get_points(key[0]))
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
