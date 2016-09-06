from threading import Timer
import json
# from .utils import timed_log
# from .query_engine import QueryEngine
# from .index_storage import FileStorage
from .metric_index import MetricIndex
from ..constants import INFLUXDB_AGGREGATIONS, _INFLUXDB_CLIENT_PARAMS, \
     SERIES_LOADER_MUTEX_KEY, LOADER_LIMIT
from ..utils import NullStatsd, normalize_config, \
     calculate_interval, read_influxdb_values, get_aggregation_func, \
     gen_memcache_key, gen_memcache_pattern_key, Query, get_retention_policy

# from graphite_api.config import logger
import logging

logger = logging.getLogger('graphite_influxdb')

class MetricLookup(object):
    def __init__(self, client, memcache, memcache_ttl,
                 index_path='/tmp', build_interval=900, load_interval=300):
        # self.config = config
        # self.index_config = config.get('index', {})
        self.client = client
        self.memcache = memcache
        self.memcache_ttl = memcache_ttl
        self.build_interval = build_interval
        self.load_interval = load_interval
        self.background_workers_active = False
        self.index = MetricIndex()
        # self.storage = FileStorage(self.index_config)

    def query(self, query):
        return self.index.query(query)

    def delete_index(self):
        self.index = MetricIndex()

    def start_background_refresh(self):
        pass

    def stop_background_refresh(self):
        pass
        
    def periodic_load_index(self):
        pass

    def periodic_build_index(self):
        pass
    
    def read_static_data(self):
        data = json.load(open('series.json'))['results'][0]['series'][0]['values']
        # import ipdb; ipdb.set_trace()
        return [d for k in data for d in k if d]
        # import ipdb; ipdb.set_trace()
        # return [d.get('key') for k in data for d in k if d]
    
    def build_index(self):
        logger.info('index.build.start')

        # storage = FileStorage(self.index_config)
        # query_engine = QueryEngine(self.config)

        # if storage.try_acquire_update_lock():
        logger.info('index.build.lock_acquired')

        # data = self.get_all_series_list()
        data = self.read_static_data()
        logger.info("Building index..")
        index = MetricIndex()
        for metric in data:
            index.insert(metric)
        self.index = index
        logger.info("Finished building index")
            # storage.save(index.to_json())

            # storage.release_update_lock()
        # else:
        #     logger.info('index.build.lock_unavailable')
    
    def get_all_series_list(self, limit=LOADER_LIMIT, offset=0, _data=None,
                            *args, **kwargs):
        """Retrieve all series for series loader"""
        query = Query('*')
        data = self._get_series(limit=limit, offset=offset)
        return self._pagination_runner(data, query, self.get_all_series_list,
                                       limit=limit, offset=offset)
    
    def _pagination_runner(self, data, query, get_series_func,
                           limit=None, offset=None, _data=None,
                           *args, **kwargs):
        if not _data:
            _data = []
        if data:
            if len(data) < limit:
                self._store_last_offset(query, limit, offset)
                return _data + data
            if len(data) > limit:
                return data
            offset = limit + offset
            return data + get_series_func(
                query=query, *args, limit=limit, offset=offset,
                _data=_data, **kwargs)
        self._store_last_offset(query, limit, offset)
        return data
    
    def _get_series(self, limit=LOADER_LIMIT, offset=0):
        memcache_key = gen_memcache_pattern_key("_".join([
            '*', str(limit), str(offset)]))
        _query = "SHOW SERIES LIMIT %s OFFSET %s" % (limit, offset,)
        logger.debug("Series loader calling influxdb with query - %s", _query)
        data = self.client.query(_query, params=_INFLUXDB_CLIENT_PARAMS)
        # >= 0.11 show series API
        if data.keys() and 'results' in data.keys()[0]:
            series = [d.get('key') for k in data for d in k
                      if d]
        else:
            series = [key_name for (key_name, _) in data.keys()]
        if self.memcache:
            self.memcache.set(memcache_key, series, time=self.memcache_ttl,
                              min_compress_len=50)
        return series
    
    def _store_last_offset(self, query, limit, offset):
        if offset and self.memcache:
            # Store empty list at offset+last limit to indicate
            # that this is the last page
            last_offset = offset + limit
            logger.debug("Pagination finished for query pattern %s "
                         "- storing empty array for limit %s and "
                         "last offset %s",
                         query.pattern, limit, offset,)
            memcache_key = gen_memcache_pattern_key("_".join([
                query.pattern, str(limit), str(last_offset)]))
            self.memcache.set(memcache_key, [], time=self.memcache_ttl)
