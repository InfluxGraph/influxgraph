# Copyright (C) [2015-] [Thomson Reuters LLC]
# Copyright (C) [2015-] [Panos Kittenis]
# Copyright (C) [2014-2015] [Vimeo, LLC]

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
from .constants import GRAPHITE_PATH_REGEX_PATTERN
import re
import logging

logger = logging.getLogger('graphite_influxdb')


class InvalidTemplateError(Exception):
    pass


def _parse_influxdb_graphite_templates(templates, separator='.', default=None):
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
        if i > len(metric_path_parts):
            continue
        if tag == 'measurement':
            measurement.append(metric_path_parts[i])
        elif tag == 'field':
            if len(field):
                raise InvalidTemplateError(
                    "'field' can only be used once in each template - %s",
                    template)
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
    for i in range(len(_tags)):
        tag = _tags[i]
        if not tag:
            tag = None
        tags[i] = tag
    return tags
