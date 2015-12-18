"""Package containing InfluxdbReader class"""

import logging
from logging.handlers import TimedRotatingFileHandler
from ..constants import _INFLUXDB_CLIENT_PARAMS
from ..utils import calculate_interval, read_influxdb_values, \
     get_aggregation_func, gen_memcache_key
try:
    import statsd
except ImportError:
    pass
try:
    import memcache
except ImportError:
    pass

logger = logging.getLogger('graphite_influxdb')

class InfluxdbReader(object):
    """Graphite-Api reader class for InfluxDB.
    
    Retrieves a single metric series from InfluxDB
    """
    __slots__ = ('client', 'path', 'statsd_client', 'aggregation_functions',
                 'memcache', 'deltas')

    def __init__(self, client, path, statsd_client,
                 memcache_host=None,
                 aggregation_functions=None,
                 deltas=None):
        self.client = client
        self.path = path
        self.statsd_client = statsd_client
        self.aggregation_functions = aggregation_functions
        if memcache_host:
            self.memcache = memcache.Client([memcache_host],
                                            pickleProtocol=-1)
        else:
            self.memcache = None
        self.deltas = deltas

    def fetch(self, start_time, end_time):
        """Fetch single series' data from > start_time and <= end_time
        
        :param start_time: start_time in seconds from epoch
        :param end_time: end_time in seconds from epoch
        """
        interval = calculate_interval(start_time, end_time, deltas=self.deltas)
        aggregation_func = get_aggregation_func(self.path, self.aggregation_functions)
        logger.debug("fetch() path=%s start_time=%s, end_time=%s, interval=%d, aggregation=%s",
                     self.path, start_time, end_time, interval, aggregation_func)
        memcache_key = gen_memcache_key(start_time, end_time, aggregation_func,
                                        [self.path])
        data = self.memcache.get(memcache_key) if self.memcache else None
        if data:
            logger.debug("Found cached data for key %s", memcache_key)
            return data
        timer_name = ".".join(['service_is_graphite-api',
                               'ext_service_is_influxdb',
                               'target_type_is_gauge',
                               'unit_is_ms',
                               'what_is_query_individual_duration'])
        _query = 'select %s(value) as value from "%s" where (time > %ds and time <= %ds) GROUP BY time(%ss)' % (
            aggregation_func, self.path, start_time, end_time, interval,)
        logger.debug("fetch() path=%s querying influxdb query: '%s'", self.path, _query)
        timer = self.statsd_client.timer(timer_name)
        timer.start()
        data = self.client.query(_query, params=_INFLUXDB_CLIENT_PARAMS)
        logger.debug("fetch() path=%s returned data: %s", self.path, data)
        data = read_influxdb_values(data)
        timer.stop()
        time_info = start_time, end_time, interval
        values = [v for v in data[self.path]] if self.path in data else []
        if self.memcache:
            self.memcache.set(memcache_key, data,
                              time=interval,
                              min_compress_len=50)
        return time_info, values
    
    def get_intervals(self):
        """Noop function - Used by Graphite-Web but not needed for Graphite-Api"""
        pass
