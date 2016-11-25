from ..utils import heapsort

cpdef list _get_series_with_tags(unicode serie, dict all_fields,
                                 list graphite_templates,
                                 str separator = '.'):
    cdef list paths = serie.split(',')
    if not graphite_templates:
        return [paths[0:1]]
    cdef list series = []
    cdef list split_path
    cdef dict template
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

cpdef public _split_series_with_tags(list paths, list graphite_templates):
    cdef str separator
    cdef list split_path = []
    cdef dict template = None
    cdef list tags_values = [p.split('=') for p in paths[1:]]
    cdef int field_inds
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

cpdef void _make_path_from_template(list split_path, str measurement, dict template, list tags_values,
                             str separator='.'):
    cdef int measurement_found = 0
    cdef int i
    if not tags_values and separator in measurement and \
       'measurement*' == [t for t in template.values() if t][0]:
        for i, measurement in enumerate(measurement.split(separator)):
            split_path.append((i, measurement))
        return
    cdef str tag_key
    cdef str tag_val
    for (tag_key, tag_val) in tags_values:
        for i, tmpl_tag_key in template.items():
            if not tmpl_tag_key:
                continue
            if tag_key == tmpl_tag_key:
                split_path.append((i, tag_val))
            elif 'measurement' in tmpl_tag_key and not measurement_found:
                measurement_found = 1
                split_path.append((i, measurement))

cpdef void _add_fields_to_paths(list fields, list split_path, list series,
                              str separator='.'):
    cdef str field_key
    cdef list field_keys
    cdef str f
    for field_key in fields:
        field_keys = [f for f in field_key.split(separator)
                      if f != 'value']
        series.append(split_path + field_keys)
