import re
from .constants import GRAPHITE_PATH_REGEX_PATTERN
import logging

logger = logging.getLogger('graphite_influxdb')

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
                                 generate_template_regex(template),
                                 default_tags, separator))
    return parsed_templates

def generate_filter_regex(_filter):
    """Generate compiled regex pattern from filter string"""
    if not _filter:
        return
    return re.compile("^%s" % (_filter.replace('.', '\.').replace('*', '%s+' % (
        GRAPHITE_PATH_REGEX_PATTERN,))))

def generate_template_regex(template):
    """Generate template regex from parsed InfluxDB Graphite template string"""
    # hostname.service.resource.measurement*
    tags = template.split('.')
    patterns = []
    for tag in tags:
        if 'measurement' in tag:
            pattern = r"(?P<measurement>%s+)" % (GRAPHITE_PATH_REGEX_PATTERN,)
            patterns.append(pattern)
            continue
        elif 'field' in tag:
            pattern = r"(?P<field>%s+)" % (GRAPHITE_PATH_REGEX_PATTERN,)
            patterns.append(pattern)
            continue
        # Drop out sub-path
        if not tag:
            patterns.append(r"%s+" % (GRAPHITE_PATH_REGEX_PATTERN,))
            continue
        patterns.append(r"(?P<%s>%s+)" % (tag, GRAPHITE_PATH_REGEX_PATTERN))
    return re.compile(r"\.".join(patterns))
