"""Package containing InfluxdbReader class"""

import logging
from logging.handlers import TimedRotatingFileHandler
from ..constants import _INFLUXDB_CLIENT_PARAMS
from ..utils import calculate_interval, read_influxdb_values, get_aggregation_func
try:
    import statsd
except ImportError:
    pass

logger = logging.getLogger('graphite_influxdb')

class InfluxdbReader(object):
    """Graphite-Api reader class for InfluxDB.
    
    Retrieves a single metric series from InfluxDB
    """
    __slots__ = ('client', 'path', 'statsd_client', 'aggregation_functions')

    def __init__(self, client, path, statsd_client,
                 aggregation_functions=None):
        self.client = client
        self.path = path
        self.statsd_client = statsd_client
        self.aggregation_functions = aggregation_functions

    def fetch(self, start_time, end_time):
        """Fetch single series' data from > start_time and <= end_time
        
        :param start_time: start_time in seconds from epoch
        :param end_time: end_time in seconds from epoch
        """
        interval = calculate_interval(start_time, end_time)
        aggregation_func = get_aggregation_func(self.path, self.aggregation_functions)
        logger.debug("fetch() path=%s start_time=%s, end_time=%s, interval=%d, aggregation=%s",
                     self.path, start_time, end_time, interval, aggregation_func)
        timer_name = ".".join(['service_is_graphite-api',
                               'ext_service_is_influxdb',
                               'target_type_is_gauge',
                               'unit_is_ms',
                               'what_is_query_individual_duration'])
        with self.statsd_client.timer(timer_name):
            _query = 'select %s(value) as value from "%s" where (time > %ds and time <= %ds) GROUP BY time(%ss)' % (
                aggregation_func, self.path, start_time, end_time, interval,)
            logger.debug("fetch() path=%s querying influxdb query: '%s'", self.path, _query)
            data = self.client.query(_query, params=_INFLUXDB_CLIENT_PARAMS)
        logger.debug("fetch() path=%s returned data: %s", self.path, data)
        data = read_influxdb_values(data)
        time_info = start_time, end_time, interval
        return time_info, [v for v in data[self.path]] if self.path in data else []
    
    def get_intervals(self):
        """Noop function - Used by Graphite-Web but not needed for Graphite-Api"""
        pass
