from __future__ import print_function
import timeit

setup = """\
from string import ascii_letters
from random import randint, choice
from influxgraph.ext.classes.node_tree import Node
split_series = [u''.join([choice(ascii_letters) for _ in range(127)])
                     for _ in range(500)]
series = [u'.'.join(split_series)
          for _ in range(5, 1000)]
queries = ['.'.join(['*' for _ in range(1,10)]) for _ in range(10,20)]
split_series = [serie.split('.') for serie in series]
index = Node()
"""

insert_s = """\
for split_path in split_series:
    index.insert_split_path(split_path)
"""

query_s = """\
for query in queries:
    index.query(query)
"""

if __name__ == '__main__':
    print(timeit.timeit(stmt=insert_s, setup=setup), number=1)
