# Copyright (C) [2015-] [Thomson Reuters LLC]
# Copyright (C) [2015-] [Panos Kittenis]

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""C Extension of Tree representation of Graphite metrics"""

from __future__ import absolute_import, print_function
import sys
import json

from graphite_api.utils import is_pattern
from graphite_api.finders import match_entries
from cpython.version cimport PY_MAJOR_VERSION

cdef unicode _decode_str(_str):
    if PY_MAJOR_VERSION > 2 and not isinstance(_str, bytes):
        # fast path for most common case(s)
        return <unicode>_str
    elif PY_MAJOR_VERSION < 3 and isinstance(_str, unicode):
        return <unicode>_str
    return _str.decode('utf-8')

cdef bytes _encode_bytes(_str):
    if type(_str) is bytes:
        return <bytes>_str
    elif PY_MAJOR_VERSION < 3 and isinstance(_str, unicode):
        return (<unicode>_str).encode('utf-8')
    elif PY_MAJOR_VERSION >= 3 and isinstance(_str, str):
        return bytes(_str, 'utf-8')
    return bytes(_str)

cdef class Node:
    """Node class of a graphite metric"""
    __slots__ = ('children')

    def __cinit__(self):
        self.children = None

    def is_leaf(self):
        """Returns True/False depending on whether self is a LeafNode or not"""
        return self.children is None

    cpdef insert(self, list paths):
        """Insert path in this node's children"""
        if len(paths) == 0:
            return
        if self.children is None:
            self.children = ()
        cdef bytes child_name = paths[0]
        del paths[0]
        for (_child_name, node) in self.children:
            # Fast path for end of paths - avoids extra recursion
            # on adding leaf nodes
            if len(paths) == 0 and child_name == _child_name:
                return
            if child_name == _child_name:
                return node.insert(paths)
        node = Node()
        self.children += ((child_name, node),)
        return node.insert(paths)

    cdef void clear(self):
        self.children = None

    cdef list to_array(self):
        """Return list of (name, children) items for this node's children"""
        cdef bytes name
        cdef Node node
        return [(_decode_str(name), node.to_array(),) for (name, node,) in self.children] \
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

cdef class NodeTreeIndex:
    """Node tree index class with graphite glob searches per sub-part of a
    query
    """

    def __cinit__(self):
        self.index = Node()

    cpdef void insert(self, unicode metric_path):
        """Insert metric path into tree index"""
        self.index.insert([_encode_bytes(s) for s in metric_path.split('.')])

    cpdef void insert_split_path(self, list paths):
        """Insert already split path into tree index"""
        self.index.insert([_encode_bytes(s) for s in paths])

    cpdef void clear(self):
        """Clear tree index"""
        self.index.clear()

    def query(self, query):
        """Return nodes matching Graphite glob pattern query"""
        cdef list nodes = sorted(self.search(self.index, query.split('.'), []))
        cdef Node node
        return (('.'.join(path), node,)
                for path, node in nodes)

    def _get_children_from_matched_paths(self, list matched_paths, Node node):
        cdef bytes path
        cdef unicode _path
        cdef Node _node
        for (path, _node) in node.children:
            _path = _decode_str(path)
            if _path in matched_paths:
                yield (_path, _node)

    cdef Node _get_child_from_string_query(self, sub_query, Node node):
        cdef bytes path
        cdef Node _node
        for (path, _node) in node.children:
            if _decode_str(path) == sub_query:
                return _node

    def _get_matched_children(self, sub_query, Node node):
        cdef bytes key
        cdef list keys = [_decode_str(key) for (key, _) in node.children] \
          if node.children is not None else []
        cdef list matched_paths = match_entries(keys, sub_query)
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

    def search(self, Node node, list split_query, list split_path):
        """Return matching children for each query part in split query starting
        from given node"""
        sub_query = split_query[0]
        matched_children = self._get_matched_children(sub_query, node)
        cdef Node child_node
        cdef list child_path
        cdef list child_query
        for child_name, child_node in matched_children:
            child_path = split_path[:]
            child_path.append(child_name)
            child_query = split_query[1:]
            if len(child_query) > 0:
                for sub in self.search(child_node, child_query, child_path):
                    yield sub
            else:
                yield (child_path, child_node)

    cpdef list to_array(self):
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
