import json
import weakref
from graphite_api.utils import is_pattern
from graphite_api.finders import match_entries
import re

GRAPHITE_GLOB_REGEX = re.compile('\*|{')


# TODO there are some dumb things done in here, could be faster
class MetricNode(object):
    __slots__ = ['parent', 'children', '__weakref__']

    def __init__(self, parent):
        # weakref here as python's circular reference finder doesn't seem to be able to understand this structure
        self.parent = weakref.ref(parent) if parent else parent
        self.children = {}

    def is_leaf(self):
        return len(self.children) == 0

    def insert(self, path):
        if len(path) == 0: return

        child_name = path.pop(0)
        if child_name in self.children:
            target_child = self.children[child_name]
        else:
            target_child = MetricNode(self)
            self.children[child_name] = target_child

        target_child.insert(path)

    def to_array(self):
        return [[name, node.to_array()] for name, node in self.children.items()]

    @staticmethod
    def from_array(parent, array):
        metric = MetricNode(parent)

        for child_name, child_array in array:
            child = MetricNode.from_array(metric, child_array)
            metric.children[child_name] = child

        return metric


class MetricIndex(object):
    __slots__ = ['index']

    def __init__(self):
        self.index = MetricNode(None)

    def insert(self, metric_name):
        path = metric_name.split('.')
        self.index.insert(path)

    def clear(self):
        self.index.children = {}
    
    def query(self, query):
        result = dict(self.search(self.index, query.pattern.split('.'), []))
        filtered_series = match_entries(result.keys(), query.pattern) \
          if is_pattern(query.pattern) \
          else [b for b in result if
                b.startswith(query.pattern)]
        return [{'metric': path, 'is_leaf': result[path].is_leaf()}
                for path in filtered_series]
    
    def search(self, node, query_path, path):
        # import ipdb; ipdb.set_trace()
        # instruction, arg = query_path[0]

        # if instruction == 'EXACT':
        #     matched_children = [(arg, node.children[arg])] if arg in node.children else []
        # elif instruction == 'ALL':
        #     matched_children = node.children.items()
        # elif instruction == 'REGEX':
        #     matched_children = [(key, value) for key, value in node.children.items() if arg.match(key)]
        # else:
        #     raise 'Unknown Search Instruction: ' + instruction
        # import ipdb; ipdb.set_trace()
        matched_children = node.children.items()
        result = []
        for child_name, child_node in matched_children:
            child_path = list(path)
            child_path.append(child_name)
            child_query = query_path[1:]

            if len(child_query) != 0:
                for sub in self.search(child_node, child_query, child_path):
                    result.append(sub)
            else:
                result.append(('.'.join(child_path), child_node))
        return result

    def to_json(self):
        return json.dumps(self.to_array())

    def to_array(self):
        return self.index.to_array()

    @staticmethod
    def search_instruction(token):
        if token == '*':
            return 'ALL', None
        elif GRAPHITE_GLOB_REGEX.search(token):
            # Convert graphite glob expression token into a regex
            regex = re.compile(token.replace('*', '.*').replace('{', '(').replace(',', '|').replace('}', ')') + '$')
            return 'REGEX', regex
        else:
            return 'EXACT', token

    @staticmethod
    def from_array(model):
        metric_index = MetricIndex()
        metric_index.index = MetricNode.from_array(None, model)
        return metric_index

    @staticmethod
    def from_json(data):
        model = json.loads(data)
        return MetricIndex.from_array(model)
