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

def _get_field_keys(measurement, client):
    field_keys = client.query('SHOW FIELD KEYS FROM "%s"' % (measurement,))
    return field_keys[measurement]

def _get_series_with_fields(serie, graphite_templates, client):
    paths = serie.split(',')
    if not graphite_templates:
        # logger.debug("Found tagged series in DB with no templates configured, "
        #              "guessing structure from tags - configuration needs updating..")
        # for (tag_key, tag_val) in tags_values:
        #     # import ipdb; ipdb.set_trace()
        #     if 'host' or 'hostname' in tag_key:
        #         split_path.insert(0, tag_val)
        #         continue
        #     split_path.append(tag_val)
        #     # TODO - check field keys
        return [paths[0:1]]
        # split_path.append(paths[0])
        # return split_path
    series = []
    for (_, template, _, _) in graphite_templates:
        if 'field' in template.groupindex:
            field_ind = template.groupindex['field']-1
            fields = _get_field_keys(paths[0], client)
            for field in fields:
                field_key = field.get('fieldKey')
                split_path = _split_series_with_tags(
                    paths, graphite_templates, client)
                split_path.append(field_key)
                series.append(split_path)
        else:
            series.append(_split_series_with_tags(
                paths, graphite_templates, client))
    return series

def _split_series_with_tags(paths, graphite_templates, client):
    split_path = []
    tags_values = [p.split('=') for p in paths[1:]]
    measurement_ind = None
    field = None
    for (tag_key, tag_val) in tags_values:
        for (_, template, _, _) in graphite_templates:
            if tag_key in template.groupindex:
                split_path.insert(template.groupindex[tag_key]-1, tag_val)
                if 'measurement' in template.groupindex:
                    measurement_ind = template.groupindex['measurement']-1
    split_path.insert(measurement_ind, paths[0])
    return split_path
