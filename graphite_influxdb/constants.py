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
