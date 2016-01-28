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

# https://influxdb.com/docs/v0.9/query_language/functions.html
INFLUXDB_AGGREGATIONS = ['count', 'distinct', 'integral', 'mean', 'median',
                         'sum', 'first', 'last', 'max', 'min', 'percentile',
                         'top', 'bottom', 'derivative', 'nonNegativeDerivative',
                         'stddev']

DEFAULT_AGGREGATIONS = { '\.min$' : 'min',
                         '\.max$' : 'max',
                         '\.last$' : 'last',
                         '\.sum$' : 'sum',
                         }

# Tell influxdb to return time as seconds from epoch
_INFLUXDB_CLIENT_PARAMS = {'epoch': 's'}
SERIES_LOADER_MUTEX_KEY = 'graphite_influxdb_series_loader'
MEMCACHE_SERIES_DEFAULT_TTL = 1800
