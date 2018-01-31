# Copyright (C) [2015-2018] [Panos Kittenis]

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import requests
import ujson


class InfluxDBClient(object):
    """High performance, minimal InfluxDB client.

    Query data using ``ujson`` JSON serializer for fastest performance.
    Write data in native line protocol for fast insertion."""

    def __init__(self, host, db=None, port=8086, user='root', passwd='root',
                 ssl=False):
        self.params = {'u': user, 'p': passwd}
        self.db = db
        prefix = 'https' if ssl else 'http'
        self.url = '%s://%s:%s' % (prefix, host, port)
        self.session = requests.Session()
        self._user = user
        self._password = passwd

    def _run_query(self, params):
        _url = "%s/query" % self.url
        resp = self.session.request(method='GET',
                                    auth=(self._user, self._password),
                                    url=_url, params=params)
        resp.raise_for_status()
        return resp

    def query(self, query, db=None, params=None, chunked=False):
        db = db if db is not None else self.db
        if db is None:
            raise ValueError(
                "Database name must be provided either on this function or "
                "the client object")
        params = params if params is not None else self.params
        params.update(self.params)
        params['q'] = query
        params['db'] = self.db
        resp = self._run_query(params)
        return ujson.loads(resp.content).get('results', [])

    def create_database(self, db):
        query = 'CREATE DATABASE "%s"' % db
        params = {}
        params.update(self.params)
        params['q'] = query
        self._run_query(params)

    def drop_database(self, db):
        query = 'DROP DATABASE "%s"' % db
        params = {}
        params.update(self.params)
        params['q'] = query
        self._run_query(params)

    def write(self, data, params=None):
        _url = "%s/write" % self.url
        params = params if params is not None else self.params
        params.update(self.params)
        params['db'] = self.db
        resp = self.session.request(method='post', url=_url,
                                    auth=(self._user, self._password),
                                    data=data, params=params)
        resp.raise_for_status()
