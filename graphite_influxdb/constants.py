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
