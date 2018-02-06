# cython: boundscheck=False, wraparound=False, optimize.use_switch=True

# Copyright (C) [2015-2017] [Thomson Reuters LLC]
# Copyright (C) [2015-2017] [Panos Kittenis]

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Cython Extension of performance critical templates modules functions"""

from heapq import heappush, heappop
import logging

from libc.string cimport strndup, strsep, strdup, strncmp, strcmp, strchr
from libc.stdlib cimport malloc, realloc, free

from influxgraph.ext.nodetrie cimport Node, to_cstring_array, _encode_bytes
from influxgraph.constants import ENCODING


logger = logging.getLogger('influxgraph')


cdef char ** _parse_serie_no_templates(char **c_paths,
                                       Py_ssize_t *_series_len,
                                       Node index,
                                       unicode serie,
                                       char *c_sep) except NULL:
    cdef size_t path_i = 0
    cdef bytes b_path = _encode_bytes(serie)
    cdef Py_ssize_t path_len = len(b_path)
    cdef char *c_path = b_path
    cdef Py_ssize_t series_len = _series_len[0]
    cdef char **new_paths
    cdef char *to_free, *temp, *token
    with nogil:
        to_free = temp = strndup(c_path, path_len)
        try:
            token = strsep(&temp, c_sep)
            while token is not NULL:
                if path_i + 1 >= series_len:
                    new_paths = <char **>realloc(
                        c_paths, (series_len * 2) * sizeof(char *))
                    if new_paths is NULL:
                        with gil:
                            raise MemoryError
                    c_paths = new_paths
                    series_len *= 2
                    _series_len[0] = series_len
                c_paths[path_i] = strdup(token)
                path_i += 1
                token = strsep(&temp, c_sep)
            c_paths[path_i] = NULL
            index._insert_split_path(<const char **>c_paths)
            return c_paths
        finally:
            for i in range(path_i):
                free(c_paths[i])
                c_paths[i] = NULL
            free(to_free)


cdef char ** _parse_serie_with_tags(char **c_split_tags,
                                    size_t *_split_tags_size,
                                    Node index, unicode serie, dict fields,
                                    list graphite_templates,
                                    char *c_sep) except NULL:
    cdef bytes b_path, b_measurement
    cdef list split_paths
    cdef char *token, *to_free, *c_measurement, *temp
    cdef size_t split_tags_size = _split_tags_size[0]
    cdef size_t tags_i = 0
    cdef char **new_split_tags
    b_path = _encode_bytes(serie)
    cdef Py_ssize_t path_len = len(b_path)
    cdef char *c_path = b_path
    to_free = temp = strndup(c_path, path_len)
    try:
        token = strsep(&temp, ',')
        # We know we have tags at this point
        c_measurement = strdup(token)
        # Copy
        b_measurement = c_measurement
        with nogil:
            token = strsep(&temp, ',')
            while token is not NULL:
                if tags_i >= split_tags_size:
                    new_split_tags = <char **>realloc(
                        c_split_tags, (split_tags_size * 2) * sizeof(char *))
                    if new_split_tags is NULL:
                        with gil:
                            raise MemoryError
                    c_split_tags = new_split_tags
                    split_tags_size *= 2
                    # Set new size on pointer
                    _split_tags_size[0] = split_tags_size
                c_split_tags[tags_i] = strdup(token)
                tags_i += 1
                token = strsep(&temp, ',')
            c_split_tags[tags_i] = NULL
        if graphite_templates:
            # Series with no tags and template configured, ignore
            split_paths = c_get_series_with_tags(
                b_measurement, c_split_tags, fields, graphite_templates,
                tags_i, c_sep)
            split_path_size = len(split_paths)
            for split_path in split_paths[:split_path_size]:
                c_paths = to_cstring_array(split_path)
                try:
                    with nogil:
                        index._insert_split_path(<const char **>c_paths)
                finally:
                    free(c_paths)
        else:
            _parse_serie_no_tags(c_measurement, index)
        return c_split_tags
    finally:
        for i in range(tags_i):
            if c_split_tags[i] is not NULL:
                free(c_split_tags[i])
                c_split_tags[i] = NULL
        free(to_free)
        free(c_measurement)


cdef int _parse_serie_no_tags(char *measurement,
                              Node index) except -1:
    # Series with tags and no templates,
    # add only measurement to index
    cdef list _serie
    cdef char **c_paths
    try:
        with nogil:
            c_paths = <char **>malloc(2 * sizeof(char *))
            if c_paths is NULL:
                with gil:
                    raise MemoryError
            c_paths[0] = measurement
            c_paths[1] = NULL
            index._insert_split_path(<const char **>c_paths)
        return 0
    finally:
        free(c_paths)


def parse_series(list series, dict fields,
                 list graphite_templates, bytes separator=b'.'):
    """Parses series and fields with/without graphite templates
    and returns built Index

    :param series: Series to load
    :type series: list(unicode str)
    :param fields: Per measurement field keys from InfluxDB. May be `None`
    :type fields: dict(measurement: [field1, field2, ..])
    :param graphite_templates: Graphite templates to use to parse series
    and fields.
    :type graphite_templates: list(tuple) as returned by
      :mod:`influxgraph.templates.parse_influxdb_graphite_templates`

    :rtype: :mod:`influxgraph.ext.nodetrie.Node`
    """
    cdef unicode serie
    cdef char **c_paths
    cdef size_t path_i = 0, tags_i = 0
    cdef size_t split_tags_size = 1
    cdef Py_ssize_t series_size = len(series)
    cdef Py_ssize_t series_len = series_size + 1
    cdef char *c_sep = separator
    cdef Node index = Node()
    # Allocate and use single array for paths once
    c_paths = <char **>malloc(series_len * sizeof(char *))
    if c_paths is NULL:
        raise MemoryError
    # Also allocate tags array once here to avoid multiple (re)-allocations
    cdef char **c_split_tags = <char **>malloc(
        split_tags_size * sizeof(char *))
    if c_split_tags is NULL:
        free(c_paths)
        raise MemoryError
    try:
        for serie in series[:series_size]:
            # If we have metrics with tags in them split them out and
            # pre-generate a correctly ordered split path for that metric
            # to be inserted into index
            if graphite_templates is not None or ',' in serie:
                c_split_tags = _parse_serie_with_tags(
                    c_split_tags, &split_tags_size, index, serie, fields,
                    graphite_templates, c_sep)
            # No tags, no template
            else:
                c_paths = _parse_serie_no_templates(
                    c_paths, &series_len, index, serie, c_sep)
        return index
    finally:
        free(c_paths)
        free(c_split_tags)


cpdef list heapsort(list iterable):
    cdef list h = []
    cdef tuple value
    for value in iterable:
        heappush(h, value)
    return [heappop(h) for _ in range(len(h))]


cdef list c_get_series_with_tags(bytes measurement, char **tags_values,
                                 dict all_fields,
                                 list graphite_templates,
                                 size_t tags_size,
                                 char *c_sep):
    cdef list series = []
    cdef list split_path
    cdef dict template
    split_path, template = c_split_series_with_tags(
        measurement, tags_values, tags_size, graphite_templates, c_sep)
    if len(split_path) == 0:
        # No template match
        return series
    cdef list values = list(template.values())
    if 'field' in values or 'field*' in values:
        try:
            _add_fields_to_paths(
                all_fields[measurement.decode(ENCODING)],
                split_path, series, c_sep)
            # _c_add_fields_to_paths(
            #     all_fields[measurement], split_path, series, c_sep)
        except KeyError:
            logger.warning("Measurement %s not in field list", measurement)
        return series
    series.append(split_path)
    return series


cpdef list get_series_with_tags(list paths, dict all_fields,
                                list graphite_templates,
                                bytes separator=b'.'):
    cdef list series = []
    cdef list split_path
    cdef dict template
    split_path, template = _split_series_with_tags(paths, graphite_templates,
                                                   separator)
    if len(split_path) == 0:
        # No template match
        return series
    cdef list values = list(template.values())
    if 'field' in values or 'field*' in values:
        try:
            _add_fields_to_paths(
                all_fields[paths[0]], split_path, series, separator)
        except KeyError:
            logger.warning("Measurement %s not in field list", paths[0])
        return series
    series.append(split_path)
    return series


cdef inline char * _copy_token(char *_copy_to, char *token) nogil except NULL:
    _copy_to = strdup(token)
    if _copy_to is NULL:
        with gil:
            raise MemoryError
    return _copy_to


cdef tuple c_split_series_with_tags(bytes measurement, char **tags_values,
                                    size_t tags_size,
                                    list graphite_templates,
                                    char *c_sep):
    cdef dict template = None
    cdef char *token, *to_free, *temp
    cdef size_t tags_i = 0
    cdef char ***split_tags_values = <char ***>malloc(
        (tags_size + 1) * sizeof(char **))
    if split_tags_values is NULL:
        raise MemoryError
    try:
        for tag_val in tags_values[:tags_size]:
            if strchr(tag_val, '=') == NULL:
                continue
            elif strchr(tag_val, '\\') != NULL:
                continue
            to_free = temp = strdup(tag_val)
            if to_free is NULL:
                raise MemoryError
            try:
                with nogil:
                    token = strsep(&temp, '=')
                    while token is not NULL:
                        split_tags_values[tags_i] = <char **>malloc(
                            2 * sizeof(char *))
                        if split_tags_values[tags_i] is NULL:
                            with gil:
                                raise MemoryError
                        # Tag key
                        split_tags_values[tags_i][0] = _copy_token(
                            split_tags_values[tags_i][0], token)
                        token = strsep(&temp, '=')
                        if token is NULL:
                            with gil:
                                raise MemoryError
                        # Tag value
                        split_tags_values[tags_i][1] = _copy_token(
                            split_tags_values[tags_i][1], token)
                        tags_i += 1
                        token = strsep(&temp, '=')
                    split_tags_values[tags_i] = NULL
            finally:
                free(to_free)
        return c_make_path_with_tags(
            measurement, split_tags_values, tags_i,
            graphite_templates, c_sep)
    finally:
        for i in range(tags_i):
            free(split_tags_values[i][0])
            free(split_tags_values[i][1])
            free(split_tags_values[i])
        free(split_tags_values)
        split_tags_values = NULL


cdef tuple c_make_path_with_tags(bytes measurement,
                                 char ***split_tags_values,
                                 size_t tags_i,
                                 list graphite_templates,
                                 char *c_sep):
    """Make path from split tags and template"""
    cdef list split_path = []
    cdef Py_ssize_t field_inds
    cdef Py_ssize_t num_tmpl_items
    for (_filter, template, _, separator) in graphite_templates:
        _c_make_path_from_template(
            split_path, measurement, template, split_tags_values, tags_i, c_sep)
        # Split path should be at least as large as number of wanted
        # template tags taking into account measurement and number of fields
        # in template
        num_tmpl_items = len([k for k, v in template.items() if v])
        field_inds = len([v for v in template.values()
                          if v and 'field' in v])
        if (len(split_path) + field_inds) >= num_tmpl_items:
            path = [p[1].decode(ENCODING) for p in heapsort(split_path)]
            if _filter:
                if _filter.match_split_path(path):
                    return path, template
            else:
                return path, template
            split_path = []
            continue
        # Reset path if template does not match
        split_path = []
    return [], template


cdef tuple _split_series_with_tags(list paths, list graphite_templates,
                                   bytes separator):
    cdef list split_path = []
    cdef dict template = None
    cdef list tags_values = [p.split('=') for p in paths[1:]]
    cdef Py_ssize_t field_inds
    cdef Py_ssize_t num_tmpl_items
    cdef list path
    # TODO - Configurable separator
    for (_filter, template, _, _) in graphite_templates:
        _make_path_from_template(
            split_path, paths[0], template, tags_values, separator)
        # Split path should be at least as large as number of wanted
        # template tags taking into account measurement and number of fields
        # in template
        num_tmpl_items = len([k for k, v in template.items() if v])
        field_inds = len([v for v in template.values()
                          if v and 'field' in v])
        if (len(split_path) + field_inds) >= num_tmpl_items:
            path = [p[1] for p in heapsort(split_path)]
            if _filter:
                if _filter.match_split_path(path):
                    return path, template
            else:
                return path, template
            split_path = []
            continue
        # Reset path if template does not match
        else:
            split_path = []
    return [], template


cdef _get_first_not_none_tmpl_val(dict template):
    for t in template.values():
        if t:
            return t


cdef _get_measurement_idx(dict template):
    for key in template:
        if template[key] == 'measurement':
            return key


cdef int _split_measurement(list split_path,
                            bytes measurement,
                            char *c_sep) except -1:
    cdef Py_ssize_t m_len = len(measurement)
    cdef char *c_measurement = measurement, *token, *to_free, *temp
    cdef bytes path
    cdef size_t serie_i = 0
    to_free = temp = strndup(c_measurement, m_len)
    if to_free is NULL:
        raise MemoryError
    try:
        token = strsep(&temp, c_sep)
        while token is not NULL:
            path = token
            split_path.append((serie_i, path))
            serie_i += 1
            token = strsep(&temp, c_sep)
        return 0
    finally:
        free(to_free)


cdef int _c_make_path_from_template(list split_path,
                                    bytes measurement,
                                    dict template, char ***tags_values,
                                    size_t tags_i,
                                    char *c_sep) except -1:
    cdef bint measurement_found = 0
    cdef bytes b_tmpl_tag_key, b_tag_val
    cdef char *tag_key, *tag_val, *c_tmpl_tag_key
    if _get_first_not_none_tmpl_val(template) == 'measurement*':
        _split_measurement(split_path, measurement, c_sep)
        return 0
    # Measurement without tags
    elif tags_i == 0:
        split_path.append((_get_measurement_idx(template), measurement))
        return 0
    for tag in tags_values[:tags_i]:
        tag_key = tag[0]
        tag_val = tag[1]
        for i, tmpl_tag_key in template.items():
            if tmpl_tag_key is None:
                continue
            b_tmpl_tag_key = _encode_bytes(tmpl_tag_key)
            c_tmpl_tag_key = b_tmpl_tag_key
            if strncmp(tag_key, c_tmpl_tag_key, len(b_tmpl_tag_key)) == 0:
                # Take copy of tag value so the array can be freed
                b_tag_val = tag_val
                split_path.append((i, b_tag_val))
            elif measurement_found is False and 'measurement' in tmpl_tag_key:
                measurement_found = 1
                split_path.append((i, measurement))
    return 0


cpdef int _make_path_from_template(list split_path, unicode measurement,
                                   dict template, list tags_values,
                                   bytes separator) except -1:
    cdef bint measurement_found = 0
    cdef Py_ssize_t i
    if not tags_values and separator.decode(ENCODING) in measurement and \
      _get_first_not_none_tmpl_val(template) == 'measurement*':
        for i, measurement in enumerate(measurement.split(separator)):
            split_path.append((i, measurement))
        return 0
    # Measurement without tags
    if not tags_values:
        split_path.append((_get_measurement_idx(template), measurement))
        return 0
    cdef unicode tag_key
    cdef unicode tag_val
    for (tag_key, tag_val) in tags_values:
        for i, tmpl_tag_key in template.items():
            if not tmpl_tag_key:
                continue
            if tag_key == tmpl_tag_key:
                split_path.append((i, tag_val))
            elif measurement_found is False and 'measurement' in tmpl_tag_key:
                measurement_found = 1
                split_path.append((i, measurement))
    return 0


# cdef int _c_add_fields_to_paths(list fields, list split_path, list series,
#                                 char *c_sep) except -1:
#     cdef unicode field_key
#     cdef bytes b_field_key, f
#     cdef char *token, *to_free, *temp, *c_field_key
#     cdef size_t field_key_size = 5, field_key_i = 0
#     cdef char **field_keys = <char **>malloc(
#         field_key_size * sizeof(char *))
#     if field_keys is NULL:
#         raise MemoryError
#     cdef char **new_field_keys
#     cdef list _field_keys
#     cdef Py_ssize_t field_key_len
#     try:
#         for field_key in fields:
#             field_keys = <char **>malloc(
#                 field_key_size * sizeof(char *))
#             b_field_key = _encode_bytes(field_key)
#             c_field_key = b_field_key
#             field_key_len = len(b_field_key)
#             with nogil:
#                 to_free = temp = strndup(c_field_key, field_key_len)
#                 if to_free is NULL:
#                     with gil:
#                         raise MemoryError
#                 token = strsep(&temp, c_sep)
#                 while token is not NULL:
#                     if field_key_i >= field_key_size:
#                         new_field_keys = <char **>realloc(
#                             field_keys, (field_key_size * 2) * sizeof(char *))
#                         if new_field_keys is NULL:
#                             with gil:
#                                 raise MemoryError
#                         field_keys = new_field_keys
#                         field_key_size *= 2
#                     if strcmp(token, 'value') != 0:
#                         field_keys[field_key_i] = strdup(token)
#                         field_key_i += 1
#                     token = strsep(&temp, c_sep)
#                 free(to_free)
#                 field_keys[field_key_i] = NULL
#             if field_key_i > 0:
#                 _field_keys = []
#                 for i in range(field_key_i):
#                     try:
#                         # Copy
#                         f = field_keys[i].encode(ENCODING)
#                         _field_keys.append(f)
#                     finally:
#                         free(field_keys[i])
#                         field_keys[i] = NULL
#                 series.append(split_path + _field_keys)
#                 field_key_i = 0
#         return 0
#     finally:
#         for i in range(field_key_i):
#             free(field_keys[i])
#         free(field_keys)


cdef int _add_fields_to_paths(list fields, list split_path, list series,
                              bytes separator) except -1:
    cdef unicode field_key
    cdef list field_keys
    cdef unicode f
    for field_key in fields:
        field_keys = [f for f in field_key.split(separator.decode(ENCODING))
                      if f != 'value']
        if len(field_keys) > 0:
            series.append(split_path + field_keys)
    return 0

#
### Data parsing
#
def _retrieve_named_field_data(infl_data, measurement_data, measurement,
                               tags, _data, bytes separator=b'.'):
    measurement_paths = measurement_data[measurement]['paths'][:]
    for field in measurement_data[measurement]['fields']:
        split_path = []
        _make_path_from_template(
            split_path, measurement,
            measurement_data[measurement]['template'], list(tags.items()),
            separator=separator)
        split_path = [p[1] for p in heapsort(split_path)]
        split_path.append(field)
        metric = separator.decode(ENCODING).join(split_path)
        if metric not in measurement_paths:
            continue
        del measurement_paths[measurement_paths.index(metric)]
        _data[metric] = [d[field]
                         for d in infl_data.get_points(
                                 measurement=measurement, tags=tags)]
    measurement_data[measurement]['paths'] = measurement_paths


def _retrieve_field_data(infl_data, dict measurement_data, measurement,
                         metric, tags, _data):
    # Retrieve value field data
    if 'value' in measurement_data[measurement]['fields']:
        _data[metric] = [d['value']
                         for d in infl_data.get_points(
                                 measurement=measurement, tags=tags)]
        return
    # Retrieve non value named field data with fields from measurement_data
    _retrieve_named_field_data(infl_data, measurement_data,
                               measurement, tags, _data)


def _read_measurement_metric_values(infl_data, measurement,
                                    list paths, dict _data):
    if measurement not in paths:
        return
    _data[measurement] = [d['value']
                          for d in infl_data.get_points(
                                  measurement=measurement)]


def read_influxdb_values(influxdb_data, list paths, dict measurement_data):
    """Return metric path -> datapoints dict for values from InfluxDB data"""
    _data = {}
    if not isinstance(influxdb_data, list):
        influxdb_data = [influxdb_data]
    cdef size_t m_path_ind = 0
    seen_measurements = set()
    for infl_data in influxdb_data:
        for infl_keys in infl_data.keys():
            measurement = infl_keys[0]
            tags = infl_keys[1] if infl_keys[1] is not None else {}
            if not measurement_data:
                _read_measurement_metric_values(infl_data, measurement,
                                                paths, _data)
                continue
            elif measurement not in measurement_data:
                continue
            if measurement not in seen_measurements:
                seen_measurements = set(
                    tuple(seen_measurements) + (measurement,))
                m_path_ind = 0
            elif len(measurement_data[measurement]['paths']) == 0:
                # No paths left for measurement
                continue
            elif m_path_ind >= len(measurement_data[measurement]['paths']):
                m_path_ind = 0
            metric = measurement_data[measurement]['paths'][m_path_ind]
            m_path_ind += 1
            _retrieve_field_data(infl_data, measurement_data,
                                 measurement, metric, tags, _data)
    return _data
