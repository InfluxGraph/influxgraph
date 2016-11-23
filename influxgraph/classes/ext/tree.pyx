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

"""Tree representation of Graphite metrics"""

from __future__ import absolute_import, print_function
import sys
import json

from graphite_api.utils import is_pattern
from graphite_api.finders import match_entries
from cpython.string cimport PyString_AsString

cdef class Node:
    """Node class of a graphite metric"""
    __slots__ = ('children')
    cdef readonly children

    def __cinit__(self):
        self.children = None

    def is_leaf(self):
        """Returns True/False depending on whether self is a LeafNode or not"""
        return self.children is None

    cpdef insert(self, paths):
        """Insert path in this node's children"""
        if len(paths) == 0:
            return
        if self.children is None:
            self.children = []
        child_name = paths[0]
        del paths[0]
        for (_child_name, node) in self.children:
            # Fast path for end of recursion - avoids extra recursion
            # for empty paths list
            if len(paths) == 0 and child_name == _child_name:
                return
            if child_name == _child_name:
                return node.insert(paths)
        node = Node()
        self.children.append((child_name, node))
        return node.insert(paths)

    cdef void clear(self):
        self.children = None

    cdef list to_array(self):
        """Return list of (name, children) items for this node's children"""
        cdef bytes name
        cdef Node node
        return [(name, node.to_array()) for (name, node,) in self.children] \
          if self.children is not None else None

    @staticmethod
    def from_array(array):
        """Load given parent node's children from array"""
        metric = Node()
        if array is None:
            return metric
        for child_name, child_array in array:
            child = Node.from_array(child_array)
            metric.children = []
            metric.children.append((child_name, child))
        return metric

cdef class NodeTreeIndex:
    """Node tree index class with graphite glob searches per sub-part of a
    query
    """
    cdef public Node index

    def __cinit__(self):
        self.index = Node()

    cdef bytes _decode_bytes(self, unicode _str):
        if not isinstance(b'', str):
            return bytes(_str, 'utf-8')
        return bytes(_str)

    cpdef insert(self, unicode metric_path):
        """Insert metric path into tree index"""
        paths = [self._decode_bytes(s) for s in metric_path.split('.')]
        self.index.insert(paths)

    cpdef insert_split_path(self, list paths):
        """Insert already split path into tree index"""
        self.index.insert([self._decode_bytes(s) for s in  paths])

    cpdef clear(self):
        """Clear tree index"""
        self.index.clear()

    def query(self, str query):
        """Return nodes matching Graphite glob pattern query"""
        cdef list nodes = sorted(self.search(self.index, query.split('.'), []))
        return ({'metric': '.'.join(path), 'is_leaf': node.is_leaf()}
                for path, node in nodes)

    def search(self, Node node, list split_query, list split_path):
        """Return matching children for each query part in split query starting
        from given node"""
        sub_query = split_query[0]
        keys = [key for (key, _) in node.children] \
          if node.children is not None else []
        matched_paths = match_entries(keys, sub_query)
        matched_children = (
            (path, _node)
            for (path, _node) in node.children
            if path in matched_paths) \
            if node.children is not None and is_pattern(sub_query) \
            else [(sub_query, [n for (k, n) in node.children
                               if k == sub_query][0])] \
                               if node.children is not None \
                               and sub_query in keys else []
        for child_name, child_node in matched_children:
            child_path = split_path[:]
            child_path.extend([child_name])
            child_query = split_query[1:]
            if len(child_query):
                for sub in self.search(child_node, child_query, child_path):
                    yield sub
            else:
                yield (child_path, child_node)

    def to_file(self, file_h):
        """Dump tree contents to file handle"""
        data = bytes(json.dumps(self.to_array()), 'utf-8') \
          if not isinstance(b'', str) else json.dumps(self.to_array())
        file_h.write(data)

    cdef list to_array(self):
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
        data = file_h.read().decode('utf-8') \
          if not isinstance(b'', str) else file_h.read()
        index = NodeTreeIndex.from_array(json.loads(data))
        return index
