from typing import *

from .mixins import Connectable
from .endpoints import is_source, is_target
from .exceptions import NodeReachabilityError


class DirectedAcyclicGraph:
    def __init__(self) -> None:
        self._nodes = dict()
        self._roots = []

    def insert_node(self, key: str,
                    element: Connectable,
                    parent: Union[Connectable,
                                  Tuple[Connectable], None]) -> None:

        if element is None:
            return

        if parent:
            if isinstance(parent, list):
                element.set_sources(*parent)
            else:
                element.set_source(parent)

        if is_source(element):
            self._roots.append(element)

        self._nodes[key] = element

    def __getitem__(self, item: str) -> Connectable:
        return self._nodes[item]

    def __contains__(self, item: str) -> bool:
        return item in self._nodes

    def __len__(self):
        return len(self._nodes)

    @property
    def nodes(self) -> ValuesView:
        return self._nodes.values()

    def is_root(self, node: Connectable) -> bool:
        return node in self._roots

    @property
    def roots(self) -> Iterable:
        return iter(self._roots)

    def check_reachability(self, callback: Callable[[type(NodeReachabilityError), str], None]) -> None:

        def _check_path_to_source(node):

            if node.has_source:
                for source in node.sources:
                    _check_path_to_source(source)
            elif not is_source(node):
                callback(NodeReachabilityError, f'Transformation {node.name} unconnected to a source')
            else:
                pass

        def _check_path_to_target(node):
            if node.has_target:
                for child in node.targets:
                    _check_path_to_target(child)
            elif is_target(node):
                pass
            else:
                callback(NodeReachabilityError, f"Transformation {node.name} unconnected to a target")

        for _node in self.nodes:
            _check_path_to_source(_node)
            _check_path_to_target(_node)

    def bfs_traverse_links(self, callback: Callable[[Connectable, Connectable], None]) -> None:
        queue = []
        visited = set()
        for n in self.roots:
            queue.append(n)
        while len(queue):
            node = queue.pop(0)
            if node in visited:
                continue
            else:
                visited.add(node)
                if node.has_target:
                    for c in node.targets:
                        callback(node, c)
                        queue.append(c)

    def bfs_traverse(self, callback: Callable[[str, Any], None]) -> None:
        queue = []
        visited = set()
        for n in self.roots:
            queue.append(n)
        while len(queue):
            node = queue.pop(0)
            if node in visited:
                continue
            else:
                callback(node.name, node)
                visited.add(node)
                if node.has_target:
                    for c in node.targets:
                        queue.append(c)
