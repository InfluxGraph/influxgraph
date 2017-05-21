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

"""Tree representation of Graphite metrics"""

from __future__ import absolute_import, print_function
import json
from collections import deque

from graphite_api.utils import is_pattern
from graphite_api.finders import match_entries


def _encode_bytes(_str):
    if not isinstance(b'', str):
        return _str.encode('utf-8')
    return bytes(_str)


def _decode_str(_str):
    try:
        return _str.decode('utf-8')
    except AttributeError:
        pass
    return _str


class Node(object):
    """Node class of a graphite metric"""
    __slots__ = ('children')

    def __init__(self):
        self.children = None

    def is_leaf(self):
        """Returns True/False depending on whether self is a LeafNode or not"""
        return self.children is None

    def insert(self, paths):
        """Insert path in this node's children"""
        if len(paths) == 0:
            return
        if self.children is None:
            self.children = ()
        child_name = paths.popleft()
        for (_child_name, node) in self.children:
            # Fast path for end of recursion - avoids extra recursion
            # for empty paths list
            if len(paths) == 0 and child_name == _child_name:
                return
            elif child_name == _child_name:
                return node.insert(paths)
        node = Node()
        self.children += ((child_name, node),)
        return node.insert(paths)

    def to_array(self):
        """Return list of (name, children) items for this node's children"""
        return [(_decode_str(name), node.to_array(),)
                for (name, node,) in self.children] \
            if self.children is not None else None

    @staticmethod
    def from_array(array):
        """Load given parent node's children from array"""
        metric = Node()
        if array is None:
            return metric
        else:
            metric.children = ()
        for child_name, child_array in array:
            child = Node.from_array(child_array)
            metric.children += ((_encode_bytes(child_name), child),)
        return metric


class NodeTreeIndex(object):
    """Node tree index class with graphite glob searches per sub-part of a
    query
    """
    __slots__ = ('index')

    @property
    def children(self):
        return self.index.children if self.index.children else []

    def __init__(self):
        self.index = Node()

    def insert(self, metric_path):
        """Insert metric path into tree index"""
        paths = deque([_encode_bytes(s) for s in metric_path.split('.')])
        self.index.insert(paths)

    def insert_split_path(self, paths):
        """Insert already split path into tree index"""
        self.index.insert(deque([_encode_bytes(s) for s in paths]))

    def clear(self):
        """Clear tree index"""
        self.index.children = None

    def query(self, query):
        """Return nodes matching Graphite glob pattern query"""
        nodes = sorted(self.search(self.index, query.split('.'), []))
        return (('.'.join(path), node,)
                for path, node in nodes)

    def _get_children_from_matched_paths(self, matched_paths, node):
        for (path, _node) in node.children:
            _path = _decode_str(path)
            if _path in matched_paths:
                yield (_path, _node)

    def _get_child_from_string_query(self, sub_query, node):
        for (path, _node) in node.children:
            if _decode_str(path) == sub_query:
                return _node

    def _get_matched_children(self, sub_query, node):
        keys = [_decode_str(key) for (key, _) in node.children] \
          if node.children is not None else []
        matched_paths = match_entries(keys, sub_query)
        if node.children is not None and is_pattern(sub_query):
            matched_children = self._get_children_from_matched_paths(
                matched_paths, node)
        else:
            matched_children = [(sub_query,
                                 self._get_child_from_string_query(
                                     sub_query, node))] \
                                     if node.children is not None \
                                     and sub_query in keys else []
        return matched_children

    def search(self, node, split_query, split_path):
        """Return matching children for each query part in split query starting
        from given node"""
        sub_query = split_query[0]
        matched_children = self._get_matched_children(sub_query, node)
        for child_name, child_node in matched_children:
            child_path = split_path[:]
            child_path.append(child_name)
            child_query = split_query[1:]
            if len(child_query) > 0:
                for sub in self.search(child_node, child_query, child_path):
                    yield sub
            else:
                yield (child_path, child_node)

    def to_array(self):
        """Return array representation of tree index"""
        return self.index.to_array()

    @staticmethod
    def from_array(model):
        """Load tree index from array"""
        metric_index = NodeTreeIndex()
        metric_index.index = Node.from_array(model)
        return metric_index

    @staticmethod
    def from_file(file_h):
        """Load tree index from file handle"""
        index = NodeTreeIndex.from_array(json.load(file_h))
        return index
