"""Tree representation of Graphite metrics as InfluxDB series"""

import logging
import datetime
from graphite_api.utils import is_pattern
from graphite_api.finders import match_entries

logger = logging.getLogger('graphite_influxdb.index')

class Node(object):

    def __init__(self, name, path, parent=None):
        self.name = name
        self.path = path
        self.parent = parent
        self.children = {}

    def is_leaf(self):
        return not bool(self.children)

    def insert(self, path, path_parts):
        if not path_parts:
            return
        _node = self
        parent_paths = []
        
        while _node.parent:
            if _node.parent.name:
                parent_paths.append(_node.parent.name)
            _node = _node.parent
        parent_paths.reverse()
        child_name = path_parts.pop(0)
        if self.name:
            parent_paths.append(self.name)
        parent_paths.append(child_name)
        child_path = '.'.join(parent_paths)
        # import ipdb; ipdb.set_trace()
        # elif self.parent.path:
        #     # for parents in self
        #     child_path = '.'.join([self.parent.path, path_parts[0]])
        # else:
        #     child_path = path_parts[0]
        # import ipdb; ipdb.set_trace()
        # child_path = '.'.join(path_parts[0:2])
        # path = '.'.join(path_parts)
        if not child_name in self.children:
            node = Node(child_name, child_path, parent=self)
            self.children[child_name] = node
            return node.insert(child_path, path_parts)
        return self.children[child_name].insert(child_path, path_parts)

class NodeTree(object):

    def __init__(self):
        self.index = Node(None, None)

    def insert(self, path):
        path_parts = [s.strip() for s in path.split('.')]
        return self.index.insert(path, path_parts)

    def search(self, node, split_query):
        _query = split_query[0]
        matched_children = [n for n in node.children.values() if match_entries([n.path], _query)]
        # import ipdb; ipdb.set_trace()
        for child in matched_children:
            if len(split_query) > 1:
                for sub_child in self.search(child, split_query[1:]):
                    matched_children.append(sub_child)
        return matched_children

class IndexTree(object):

    def __init__(self, all_series, index_path):
        self.tree = self.load(all_series)

    def load(self, all_series):
        logger.info("Starting IndexTree load..")
        tree = (None, {})
        entries = 0
        start_dt = datetime.datetime.now()
        for serie in all_series:
            branches = serie.split('.')
            leaf = branches.pop()
            parent = None
            cursor = tree
            for branch in branches:
                if branch not in cursor[1]:
                    cursor[1][branch] = (None, {})
                parent = cursor
                cursor = cursor[1][branch]
            # import ipdb; ipdb.set_trace()
            cursor[1][leaf] = (serie, {})
            entries += 1
        logger.info("IndexTree load took %.6f seconds for %d metrics",
                    (datetime.datetime.now() - start_dt), entries)
        # import ipdb; ipdb.set_trace()
        return tree
    
    def search(self, query, max_results=None, keep_query_pattern=False):
        # import ipdb; ipdb.set_trace()
        query_parts = query.split('.')
        metrics_found = set()
        for result in self.subtree_query(self.tree, query_parts):
            if keep_query_pattern:
                path_parts = result['path'].split('.')
                result['path'] = '.'.join(query_parts) + result['path'][len(query_parts):]
            if result['path'] in metrics_found:
                continue
            # import ipdb; ipdb.set_trace()
            yield result
            metrics_found.add(result['path'])
            if max_results and len(metrics_found) >= max_results:
                return
    
    def subtree_query(self, root, query_parts):
        if query_parts:
            my_query = query_parts[0]
            if is_pattern(my_query):
                matches = [root[1][node] for node in match_entries(root[1], my_query)]
            else:
                matches = [root[1][my_query]] if my_query in root[1] else []
        else:
            matches = root[1].values()
        # import ipdb; ipdb.set_trace()
        for child_node in matches:
            result = {
                'path': child_node[0],
                'is_leaf': bool(child_node[0]),
                }
            if result['path'] and not result['is_leaf']:
                result['path'] = "".join([result['path'], '.'])
            yield result
            if query_parts:
                for result in self.subtree_query(child_node, query_parts[1:]):
                    yield result
