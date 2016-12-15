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

cdef class Node:
    """Node class of a graphite metric"""
    cdef readonly tuple children
    cpdef insert(self, list paths)
    cdef void clear(self)
    cdef list to_array(self)

cdef class NodeTreeIndex:
    """Node tree index class with graphite glob searches per sub-part of a
    query
    """
    cdef Node index
    cpdef void insert(self, unicode metric_path)
    cpdef void insert_split_path(self, list paths)
    cpdef void clear(self)
    cpdef list to_array(self)
    cdef Node _get_child_from_string_query(self, sub_query, Node node)
