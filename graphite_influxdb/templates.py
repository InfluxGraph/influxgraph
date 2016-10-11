import re
from .constants import GRAPHITE_PATH_REGEX_PATTERN

def _parse_influxdb_graphite_templates(templates, separator='.', default=None):
    # Logic converted to Python from InfluxDB's Golang Graphite template parsing
    # Format is [filter] <template> [tag1=value1,tag2=value2]
    parsed_templates = []
    for pattern in templates:
        template = pattern
        filter = ""
        parts = template.split()
        if len(parts) < 1:
            continue
        elif len(parts) >= 2:
            if '=' in parts[1]:
                template = parts[0]
            else:
                filter = parts[0]
                template = parts[1]
        # Parse out the default tags specific to this template
        default_tags = {}
        if '=' in parts[-1]:
            tags = [d.strip() for d in parts[-1].split(',')]
            for tag in tags:
                tag_items = [d.strip() for d in tag.split('=')]
                default_tags[tag_items[0]] = tag_items[1]
        parsed_templates.append((generate_filter_regex(filter),
                                 generate_template_regex(template),
                                 default_tags, separator))
    return parsed_templates

def generate_filter_regex(filter):
    """Generate compiled regex pattern from filter string"""
    if not filter:
        return
    return re.compile("^%s" % (filter.replace('.', '\.').replace('*', '%s+' % (
        GRAPHITE_PATH_REGEX_PATTERN,))))

def generate_template_regex(template):
    """Generate template regex from parsed InfluxDB Graphite template string"""
    # hostname.service.resource.measurement*
    tags = template.split('.')
    patterns = []
    for tag in tags:
        if 'measurement' in tag or 'field' in tag:
            patterns.append(r"(?P<measurement>.+)")
            continue
        patterns.append(r"(?P<%s>%s+)" % (tag, GRAPHITE_PATH_REGEX_PATTERN))
    return re.compile(r"\.".join(patterns))
