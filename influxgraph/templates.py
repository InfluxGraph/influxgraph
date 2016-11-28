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

"""Graphite template parsing functions per InfluxDB's Graphite service template syntax"""

from __future__ import absolute_import, print_function
import re
import logging
from collections import deque

from .utils import heapsort
from .constants import GRAPHITE_PATH_REGEX_PATTERN
from .classes.tree import NodeTreeIndex

logger = logging.getLogger('influxgraph')


class InvalidTemplateError(Exception):
    """Raised on Graphite template configuration validation errors"""
    pass

def parse_series(series, all_fields, graphite_templates,
                 separator='.'):
    index = NodeTreeIndex()
    for serie in series:
        # If we have metrics with tags in them split them out and
        # pre-generate a correctly ordered split path for that metric
        # to be inserted into index
        if graphite_templates:
            for split_path in _get_series_with_tags(
                    serie, all_fields, graphite_templates,
                    separator=separator):
                index.insert_split_path(split_path)
        # Series with tags and no templates,
        # add only measurement to index
        elif ',' in serie:
            index.insert(serie.split(',')[0])
        # No tags, no template
        else:
            index.insert(serie)
    return index

def _parse_influxdb_graphite_templates(templates, separator='.'):
    # Logic converted to Python from InfluxDB's Golang Graphite template parsing
    # Format is [filter] <template> [tag1=value1,tag2=value2]
    parsed_templates = []
    for pattern in templates:
        template = pattern
        _filter = ""
        parts = template.split()
        if len(parts) < 1:
            continue
        elif len(parts) >= 2:
            if '=' in parts[1]:
                template = parts[0]
            else:
                _filter = parts[0]
                template = parts[1]
        # Parse out the default tags specific to this template
        default_tags = {}
        if '=' in parts[-1]:
            tags = [d.strip() for d in parts[-1].split(',')]
            for tag in tags:
                tag_items = [d.strip() for d in tag.split('=')]
                default_tags[tag_items[0]] = tag_items[1]
        parsed_templates.append((generate_filter_regex(_filter),
                                 _generate_template_tag_index(template),
                                 default_tags, separator))
    for (_, template, _, _) in parsed_templates:
        _template_sanity_check(template)
    return parsed_templates

def _template_sanity_check(template):
    field = ""
    measurement_wildcard, field_wildcard = False, False
    for tag in template.values():
        if tag == 'measurement*':
            measurement_wildcard = True
        if tag == 'field*':
            field_wildcard = True
        if tag == 'field':
            if field:
                raise InvalidTemplateError(
                    "'field' can only be used once in each template - %s",
                    template)
            field = tag
    if measurement_wildcard and field_wildcard:
        raise InvalidTemplateError(
            "Either 'field*' or 'measurement*' can be used in each template, not both - %s",
            template)

def apply_template(metric_path_parts, template, default_tags, separator='.'):
    """Apply template to metric path parts and return measurements, tags and field"""
    measurement = []
    tags = {}
    field = ""
    for i, tag in template.items():
        if i >= len(metric_path_parts):
            continue
        if tag == 'measurement':
            measurement.append(metric_path_parts[i])
        elif tag == 'field':
            field = metric_path_parts[i]
        elif tag == 'field*':
            field = separator.join(metric_path_parts[i:])
            break
        elif tag == 'measurement*':
            measurement.extend(metric_path_parts[i:])
            break
        elif tag != "":
            tags.setdefault(tag, []).append(metric_path_parts[i])
    for tag, values in tags.items():
        tags[tag] = separator.join(values)
    if default_tags:
        tags.update(default_tags)
    return separator.join(measurement), tags, field

def generate_filter_regex(_filter):
    """Generate compiled regex pattern from filter string"""
    if not _filter:
        return
    return re.compile("^%s" % (_filter.replace('.', r'\.').replace('*', '%s+' % (
        GRAPHITE_PATH_REGEX_PATTERN,))))

def _generate_template_tag_index(template):
    _tags = template.split('.')
    tags = {}
    for i, tag in enumerate(_tags):
        if not tag:
            tag = None
        tags[i] = tag
    return tags

def _get_series_with_tags(serie, all_fields, graphite_templates,
                          separator='.'):
    paths = serie.split(',')
    if not graphite_templates:
        return [paths[0:1]]
    series = deque()
    split_path, template = _split_series_with_tags(paths, graphite_templates)
    if not split_path:
        # No template match
        return series
    if 'field' in template.values() or 'field*' in template.values():
        _add_fields_to_paths(
            all_fields[paths[0]], split_path, series, separator=separator)
    else:
        series.append(split_path)
    return series

def _split_series_with_tags(paths, graphite_templates):
    split_path, template = deque(), None
    tags_values = [p.split('=') for p in paths[1:]]
    for (_, template, _, separator) in graphite_templates:
        _make_path_from_template(
            split_path, paths[0], template, tags_values)
        # Split path should be at least as large as number of wanted
        # template tags taking into account measurement and number of fields
        # in template
        field_inds = len([v for v in template.values()
                          if v and 'field' in v])
        if (len(split_path) + field_inds) >= len(
                [k for k, v in template.items() if v]):
            break
        # Reset path if template does not match
        else:
            split_path = []
    path = [p[1] for p in heapsort(split_path)] if split_path \
           else split_path
    return path, template

def _make_path_from_template(split_path, measurement, template, tags_values,
                             separator='.'):
    measurement_found = False
    if not tags_values and separator in measurement and \
       'measurement*' == [t for t in template.values() if t][0]:
        for i, measurement in enumerate(measurement.split(separator)):
            split_path.append((i, measurement))
        return
    for (tag_key, tag_val) in tags_values:
        for i, tmpl_tag_key in template.items():
            if not tmpl_tag_key:
                continue
            if tag_key == tmpl_tag_key:
                split_path.append((i, tag_val))
            elif 'measurement' in tmpl_tag_key and not measurement_found:
                measurement_found = True
                split_path.append((i, measurement))

def _add_fields_to_paths(fields, split_path, series,
                         separator='.'):
    for field_key in fields:
        field_keys = [f for f in field_key.split(separator)
                      if f != 'value']
        series.append(split_path + field_keys)
