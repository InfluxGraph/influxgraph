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

"""C Extension of performance critical templates modules functions"""

from heapq import heappush, heappop
import logging

logger = logging.getLogger('influxgraph')

# py_byte_string = 'a'
# from cpython cimport array
# import array
# cdef array.array ar = array.array('c', [py_byte_string])
# cdef unsigned char[:] ca = ar
# print(chr(ca[0]))
# chr(ord(u'a'.encode('utf-8'))).decode('utf-8')

cdef list heapsort(list iterable):
    cdef list h = []
    cdef tuple value
    for value in iterable:
        heappush(h, value)
    return [heappop(h) for _ in range(len(h))]

cpdef list get_series_with_tags(list paths, dict all_fields,
                                list graphite_templates,
                                str separator = '.'):
    if not graphite_templates:
        return [paths[0:1]]
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
    else:
        series.append(split_path)
    return series

cdef tuple _split_series_with_tags(list paths, list graphite_templates,
                                   str separator):
    cdef list split_path = []
    cdef dict template = None
    cdef list tags_values = [p.split('=') for p in paths[1:]]
    cdef int field_inds
    cdef list path
    for (_filter, template, _, separator) in graphite_templates:
        _make_path_from_template(
            split_path, paths[0], template, tags_values, separator)
        # Split path should be at least as large as number of wanted
        # template tags taking into account measurement and number of fields
        # in template
        field_inds = len([v for v in template.values()
                          if v and 'field' in v])
        if (len(split_path) + field_inds) >= len(
                [k for k, v in template.items() if v]):
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

cdef void _make_path_from_template(list split_path, unicode measurement,
                                   dict template, list tags_values,
                                   str separator):
    cdef int measurement_found = 0
    cdef int i
    if not tags_values and separator in measurement and \
       'measurement*' == [t for t in template.values() if t][0]:
        for i, measurement in enumerate(measurement.split(separator)):
            split_path.append((i, measurement))
        return
    cdef unicode tag_key
    cdef unicode tag_val
    for (tag_key, tag_val) in tags_values:
        for i, tmpl_tag_key in template.items():
            if not tmpl_tag_key:
                continue
            if tag_key == tmpl_tag_key:
                split_path.append((i, tag_val))
            elif 'measurement' in tmpl_tag_key and not measurement_found:
                measurement_found = 1
                split_path.append((i, measurement))

cdef void _add_fields_to_paths(list fields, list split_path, list series,
                               str separator):
    cdef unicode field_key
    cdef list field_keys
    cdef unicode f
    for field_key in fields:
        field_keys = [f for f in field_key.split(separator)
                      if f != 'value']
        if len(field_keys) > 0:
            series.append(split_path + field_keys)
