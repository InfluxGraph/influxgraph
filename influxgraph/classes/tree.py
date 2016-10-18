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

"""Tree representation of Graphite metrics as InfluxDB series"""

from __future__ import absolute_import, print_function
import json
import logging

from graphite_api.utils import is_pattern
from graphite_api.finders import match_entries

logger = logging.getLogger('graphite_influxdb.index')

class Node(object):
    """Node class of a graphite metric"""
    
    def __init__(self, parent=None):
        self.parent = parent
        self.children = {}

    def is_leaf(self):
        """Returns True/False depending on whether self is a LeafNode or not"""
        return len(self.children) == 0

    def insert(self, path):
        """Insert path in this node's children"""
        if not len(path):
            return
        child_name = path.pop(0)
        if not child_name in self.children:
            self.children[child_name] = Node(self)
        self.children[child_name].insert(path)

    def to_array(self):
        """Return list of (name, children) items for this node's children"""
        return [(name, node.to_array()) for name, node in self.children.items()]

    @staticmethod
    def from_array(parent, array):
        """Load given parent node's children from array"""
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

    def insert(self, metric_path):
        """Insert metric path into tree index"""
        paths = metric_path.split('.')
        self.index.insert(paths)

    def insert_split_path(self, paths):
        """Insert already split path into tree index"""
        self.index.insert(paths)

    def clear(self):
        """Clear tree index"""
        self.index.children = {}

    def query(self, query):
        """Return nodes matching Graphite glob pattern query"""
        nodes = self.search(self.index, query.split('.'), [])
        return ({'metric': '.'.join(path), 'is_leaf': node.is_leaf()}
                for path, node in nodes)

    def search(self, node, split_query, split_path):
        """Return matching children for each query part in split query starting
        from given node"""
        sub_query = split_query[0]
        matched_children = (
            (path, node.children[path])
            for path in match_entries(node.children.keys(), sub_query)) \
            if is_pattern(sub_query) \
            else [(sub_query, node.children[sub_query])] \
            if sub_query in node.children else []
        for child_name, child_node in matched_children:
            child_path = split_path[:]
            child_path.extend([child_name])
            child_query = split_query[1:]
            if len(child_query):
                for sub in self.search(child_node, child_query, child_path):
                    yield sub
            else:
                yield (child_path, child_node)

    def to_json(self):
        """Reutnr Json representation of tree index"""
        return json.dumps(self.to_array())

    def to_array(self):
        """Return array representation of tree index"""
        return self.index.to_array()
    
    @staticmethod
    def from_array(model):
        """Load tree index from array"""
        metric_index = NodeTreeIndex()
        metric_index.index = Node.from_array(None, model)
        return metric_index

    @staticmethod
    def from_json(data):
        """Load tree index from json data"""
        model = json.load(data)
        index = NodeTreeIndex.from_array(model)
        del model
        return index
