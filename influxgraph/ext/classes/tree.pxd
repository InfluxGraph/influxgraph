cdef class NodeTreeIndex:
    """Node tree index class with graphite glob searches per sub-part of a
    query
    """
    # def __cinit__(self)
    cdef public Node index
    cpdef clear(self)
    cpdef insert(self, unicode metric_path)
    cpdef insert_split_path(self, list paths)
    cpdef clear(self)
    cpdef list to_array(self)
    

cdef class Node:
    """Node class of a graphite metric"""
    # def __cinit__(self)
    cdef readonly children
    cpdef insert(self, list paths)
    cdef void clear(self)
    cpdef list to_array(self)
