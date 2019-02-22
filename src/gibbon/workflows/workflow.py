import logging
from typing import *

from .exceptions import *
from .mixins import Transformation, Connectable
from .endpoints import Source, Target
from .endpoints import is_source, is_target
from ..utils.abstract import Namable, Visitable, VisitMode, Visitor


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

    def __getitem__(self, item: str) -> Transformation:
        return self._nodes[item]

    def __contains__(self, item: str) -> bool:
        return item in self._nodes

    def __len__(self):
        return len(self._nodes)

    @property
    def nodes(self) -> ValuesView:
        return self._nodes.values()

    def is_root(self, node: Transformation) -> bool:
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

    def bfs_traverse_links(self, callback: Callable[[Transformation, Transformation], None]) -> None:
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


class Workflow(Namable, Visitable):

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._dag = DirectedAcyclicGraph()

        self._requires_validation = True
        self._valid = False
        self._invalid_config = None

        if not Namable.check_valid_name(self.name):
            raise InvalidNameError(f'Object name is invalid: {self.name}')

    @staticmethod
    def _element_factory(cls, name, *args, **kwargs):
        if not Namable.check_valid_name(name):
            raise InvalidNameError(f'Object name is invalid: {name}')

        return cls(name, *args, **kwargs)

    def get_node_by_name(self, name: str) -> Union[Transformation, None]:
        if name not in self._dag:
            raise NodeNotFound(f"Node {name} not found")
        else:
            return self._dag[name]

    def add_source(self, name: str) -> None:
        element = self._element_factory(Source, name)
        self._dag.insert_node(name, element, None)
        self._requires_validation = True

    def add_target(self, name: str, source: str = None) -> None:
        if source:
            parent = self.get_node_by_name(source)
        else:
            parent = None
        element = self._element_factory(Target, name)
        self._dag.insert_node(name, element, parent)
        self._requires_validation = True

    def add_transformation(self, name: Text,
                           cls: Type[Transformation],
                           *args,
                           sources: Union[Tuple[str], str] = None,
                           targets: Tuple[str] = (),
                           **kwargs) -> None:

        parents = None
        if sources and isinstance(sources, tuple):
            parents = []
            for source in sources:
                parent = self.get_node_by_name(source)
                if parent:
                    parents.append(parent)
        elif sources is not None:
            source = str(sources)
            parents = self.get_node_by_name(source)

        element = self._element_factory(cls, name, *args, **kwargs)
        self._dag.insert_node(name, element, parents)

        for target in targets:
            if target:
                target = self.get_node_by_name(target)
                target.set_source(element)

        self._requires_validation = True

    def connect(self, source: str, *targets: str) -> None:
        if source:
            source = self.get_node_by_name(source)

        for target in targets:
            target = self.get_node_by_name(target)
            if target and source:
                self._requires_validation = True
                target.set_source(source)

    @property
    def is_valid(self) -> bool:
        if self._requires_validation:
            self.validate(silent=True)

        return self._valid

    def validate(self, verbose: bool = False, silent: bool = False) -> None:

        if not self._requires_validation:
            logging.info(f"Workflow {self.name} does not seem to require validation")

        logging.info(f"Validating workflow {self.name}")

        errors = []

        def on_error(exc_type, msg):
            errors.append(exc_type(msg))

        if len(self._dag.nodes) == 0:
            on_error(NodeNotFound, f'No transformation defined, workflow {self.name} is empty')
        else:
            self._dag.check_reachability(on_error)

        self._valid = len(errors) == 0

        if verbose and not self._valid:
            for error in errors:
                logging.error(str(error))

        if self._valid:
            logging.info(f"Workflow {self.name} is valid.")
            self._requires_validation = False
        else:
            logging.info(f"Workflow {self.name} is invalid.")
            self._requires_validation = True

        if len(errors) and not silent:
            raise errors.pop()

    def accept_visitor(self, visitor: Visitor, mode: VisitMode):
        if mode == VisitMode.elements_only:
            self._dag.bfs_traverse(visitor.visit_element)

        elif mode == VisitMode.elements_then_links:
            self._dag.bfs_traverse(visitor.visit_element)
            self._dag.bfs_traverse_links(visitor.visit_link)

        elif mode == VisitMode.links_only:
            self._dag.bfs_traverse_links(visitor.visit_link)

        elif mode == VisitMode.links_then_elements:
            self._dag.bfs_traverse_links(visitor.visit_link)
            self._dag.bfs_traverse(visitor.visit_element)
