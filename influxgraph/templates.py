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

"""Graphite template parsing functions per InfluxDB's Graphite
service template syntax
"""

from __future__ import absolute_import, print_function
import logging
from collections import deque
from heapq import heappush, heappop

from .constants import ENCODING


logger = logging.getLogger('influxgraph')


class TemplateFilter(object):
    """Filter metric paths on template pattern"""

    def __init__(self, pattern):
        self.pattern = [p for p in pattern.split('.') if p]

    def match(self, path):
        """Check if path matches template pattern

        :param path: Graphite path to check
        :type path: str

        :rtype: bool"""
        path = path.split('.')
        return self.match_split_path(path)

    def match_split_path(self, split_path):
        """Go through split sub-paths and pattern's sub-paths and check
        if pattern matches all sub-paths

        :param split_path: Graphite metric path split on separator
        :type split_path: list(str)"""
        for i, pat in enumerate(self.pattern):
            if pat == '*':
                continue
            try:
                if not pat == split_path[i]:
                    return False
            except IndexError:
                return False
        return True


class InvalidTemplateError(Exception):
    """Raised on Graphite template configuration validation errors"""
    pass


class TemplateMatchError(Exception):
    """Raised on errors matching template with path"""
    pass


# Function as per Python official documentation
def heapsort(iterable):
    """Perform heap sort on iterable

    :param iterable: Iterable with (index, value) tuple entries to sort
    on index value. `index` must be integer, `value` can be anything
    :type iterable: `tupleiterator`"""
    h = []
    for value in iterable:
        heappush(h, value)
    return [heappop(h) for _ in range(len(h))]


def parse_influxdb_graphite_templates(templates, separator='.'):
    """Parse InfluxDB template configuration and return parsed templates

    :param templates: Template patterns to parse. \
    Format is [filter] <template> [tag1=value1,tag2=value2]
    :type templates: list(str)
    :param separator: (Optional) Separator to use when storing greedy
      matched columns
    :type separator: str

    :raises: :mod:`InvalidTemplateError` on invalid template format used in any
    template pattern
    """
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
        filter_parser = TemplateFilter(_filter) if _filter else None
        parsed_templates.append((filter_parser,
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
            "Either 'field*' or 'measurement*' can be used in each template, "
            "not both - %s", template)
    if not measurement_wildcard and 'measurement' not in template.values():
        raise InvalidTemplateError(
            "At least one of 'measurement' or 'measurement*' is required - %s",
            template)


def apply_template(metric_path_parts, template, default_tags, separator='.'):
    """Apply template to metric path parts and return measurements, tags and
    field

    :raises: mod:`TemplateMatchError` on error matching template"""
    measurement = []
    tags = {}
    field = ""
    if len(metric_path_parts) < len(template.keys()):
        raise TemplateMatchError()
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


def _generate_template_tag_index(template):
    _tags = template.split('.')
    tags = {}
    for i, tag in enumerate(_tags):
        if not tag:
            tag = None
        tags[i] = tag
    return tags


def get_series_with_tags(paths, all_fields, graphite_templates,
                         separator='.'):
    """Get list of metric paths from list of InfluxDB series with tags and
   configured graphite templates if any.

    Without graphite template configuration tags are dropped and only the
    series name is used."""
    if not graphite_templates:
        return [paths[0:1]]
    series = deque()
    split_path, template = _split_series_with_tags(paths, graphite_templates)
    if not split_path:
        # No template match
        return series
    if 'field' in template.values() or 'field*' in template.values():
        try:
            _add_fields_to_paths(
                all_fields[paths[0]], split_path, series, separator=separator)
        except KeyError:
            logger.warning("Measurement %s not in field list", paths[0])
    else:
        series.append(split_path)
    return series


def _split_series_with_tags(paths, graphite_templates):
    split_path, template = deque(), None
    tags_values = [p.split('=') for p in paths[1:]]
    for (_filter, template, _, separator) in graphite_templates:
        _make_path_from_template(
            split_path, paths[0], template, tags_values, separator=separator)
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


def _get_first_not_none_tmpl_val(template):
    for t in template.values():
        if t:
            return t


def _get_measurement_idx(template):
    for key in template:
        if template[key] == 'measurement':
            return key


def _make_path_from_template(split_path, measurement, template, tags_values,
                             separator='.'):
    if not tags_values and separator in measurement and \
      _get_first_not_none_tmpl_val(template) == 'measurement*':
        for i, measurement in enumerate(measurement.split(separator)):
            split_path.append((i, measurement))
        return
    measurement_found = False
    # Measurement without tags
    if not tags_values:
        split_path.append((_get_measurement_idx(template), measurement))
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
        field_keys = [f for f in field_key.split(separator.decode(ENCODING))
                      if f != 'value']
        if len(field_keys) > 0:
            series.append(split_path + field_keys)
