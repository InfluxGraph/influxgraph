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

"""Constants and default settings for InfluxGraph"""

# https://influxdb.com/docs/v0.9/query_language/functions.html
# NB - Transformation functions like derivative are invalid without an aggregation
# when used in a 'group by' query - we leave those to graphite-api to perform
INFLUXDB_AGGREGATIONS = [
    # aggregations
    'count', 'distinct', 'integral', 'mean', 'median', 'mode', 'spread', 'sum',
    # selectors
    'bottom', 'first', 'last', 'max', 'min', 'percentile', 'top']

DEFAULT_AGGREGATIONS = {r'\.min$' : 'min',
                        r'\.max$' : 'max',
                        r'\.last$' : 'last',
                        r'\.sum$' : 'sum',
                        }

# Tell influxdb to return time as seconds from epoch
_INFLUXDB_CLIENT_PARAMS = {'epoch': 's'}
SERIES_LOADER_MUTEX_KEY = 'influxgraph_series_loader'
MEMCACHE_SERIES_DEFAULT_TTL = 1800
LOADER_LIMIT = 100000

# Memcache key for field keys list
_MEMCACHE_FIELDS_KEY = 'infl_fields_key'
