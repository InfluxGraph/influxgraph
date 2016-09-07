"""Tree representation of Graphite metrics as InfluxDB series"""

import json
import logging
import datetime
from graphite_api.utils import is_pattern
from graphite_api.finders import match_entries

logger = logging.getLogger('graphite_influxdb.index')

class Node(object):
    """Node class of a graphite metric"""
    
    def __init__(self, parent=None):
        self.parent = parent
        self.children = {}

    def is_leaf(self):
        return len(self.children) == 0

    def insert(self, path):
        if len(path) == 0: return

        child_name = path.pop(0)
        if child_name in self.children:
            target_child = self.children[child_name]
        else:
            target_child = Node(self)
            self.children[child_name] = target_child

        target_child.insert(path)

    def to_array(self):
        return [(name, node.to_array()) for name, node in self.children.items()]

    @staticmethod
    def from_array(parent, array):
        metric = Node(parent)

        for child_name, child_array in array:
            child = Node.from_array(metric, child_array)
            metric.children[child_name] = child

        return metric

class NodeTreeIndex(object):
    """Node tree index class with graphite glob searches per sub-part of a
    query
    """
    __slots__ = ['index']

    def __init__(self):
        self.index = Node()

    def insert(self, metric_name):
        path = metric_name.split('.')
        self.index.insert(path)

    def clear(self):
        self.index.children = {}

    def query(self, query):
        nodes = self.search(self.index, query.split('.'), [])
        return [{'metric': '.'.join(path), 'is_leaf': node.is_leaf()}
                for path, node in nodes]

    def search(self, node, split_query, split_path):
        sub_query = split_query[0]
        matched_children = [
            (path, node.children[path])
            for path in match_entries(node.children.keys(), sub_query)] \
            if is_pattern(sub_query) \
            else [(sub_query, node.children[sub_query])] \
            if sub_query in node.children else []
        result = []
        for child_name, child_node in matched_children:
            child_path = split_path[:]
            child_path.append(child_name)
            child_query = split_query[1:]
            if len(child_query):
                for sub in self.search(child_node, child_query, child_path):
                    result.append(sub)
            else:
                result.append((child_path, child_node))
        del matched_children
        return result

    def to_json(self):
        return json.dumps(self.to_array())

    def to_array(self):
        return self.index.to_array()
    
    @staticmethod
    def from_array(model):
        metric_index = NodeTreeIndex()
        metric_index.index = Node.from_array(None, model)
        return metric_index

    @staticmethod
    def from_json(data):
        model = json.load(data)
        return NodeTreeIndex.from_array(model)
