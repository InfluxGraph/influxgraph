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
# NB - Transformation functions like derivative are invalid without an aggregation
# when used in a 'group by' query - we leave those to graphite-api to perform
INFLUXDB_AGGREGATIONS = [
    # aggregations
    'count', 'distinct', 'integral', 'mean', 'median', 'mode', 'spread', 'sum',
    # selectors
    'bottom', 'first', 'last', 'max', 'min', 'percentile', 'top']

DEFAULT_AGGREGATIONS = { '\.min$' : 'min',
                         '\.max$' : 'max',
                         '\.last$' : 'last',
                         '\.sum$' : 'sum',
                         }

# Tell influxdb to return time as seconds from epoch
_INFLUXDB_CLIENT_PARAMS = {'epoch': 's'}
SERIES_LOADER_MUTEX_KEY = 'graphite_influxdb_series_loader'
MEMCACHE_SERIES_DEFAULT_TTL = 1800
LOADER_LIMIT = 100000

# Best guess. Graphite project has never published a metric path format
# and will accept any character with no validation
# However, certain characters like ),(, \,/ etc will cause issues
# with storing and lookup as well as opening up storage layers to
# injection vulnerabilities
# https://github.com/graphite-project/carbon/issues/417
GRAPHITE_PATH_REGEX_PATTERN = "[a-zA-Z0-9-_:]"
