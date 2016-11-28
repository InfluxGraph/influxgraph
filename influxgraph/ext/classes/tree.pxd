cdef class NodeTreeIndex:
    """Node tree index class with graphite glob searches per sub-part of a
    query
    """
    cdef Node index
    cpdef void insert_series(self, list series)
    cdef void insert(self, unicode metric_path)
    cdef void insert_split_path(self, list paths)
    cpdef void clear(self)
    cpdef list to_array(self)

cdef class Node:
    """Node class of a graphite metric"""
    cdef readonly children
    cpdef insert(self, list paths)
    cdef void clear(self)
    cdef list to_array(self)
