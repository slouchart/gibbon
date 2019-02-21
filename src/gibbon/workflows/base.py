import logging
from typing import *

from .util import Namable

from .transformations.base import Transformation, Connectable
from .transformations.endpoints import Source, Target
from .transformations.endpoints import is_source, is_target
from .configuration import Configuration
from ..execution.base import BaseExecutor
from .exceptions import *


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

    def bfs_traverse(self, callback: Callable[[Transformation], None]) -> None:
        queue = []
        visited = set()
        for n in self.roots:
            queue.append(n)
        while len(queue):
            node = queue.pop(0)
            if node in visited:
                continue
            else:
                callback(node)
                visited.add(node)
                if node.has_target:
                    for c in node.targets:
                        queue.append(c)


class Workflow(Namable):

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

    def prepare(self, cfg_visitor: Configuration) -> None:
        if not self.is_valid:
            logging.error(f"{self.name}: invalid workflow cannot be configured")
        else:
            try:
                self._dag.bfs_traverse(cfg_visitor.set_configuration)
                self._invalid_config = False
            except ConfigurationError as e:
                logging.error(str(e))
                self._invalid_config = True
                raise

    def reset(self, cfg_visitor: Configuration) -> None:
        self._dag.bfs_traverse(cfg_visitor.reset_configuration)

    def _prepare_execution(self, exec_visitor: BaseExecutor) -> bool:
        if not self.is_valid:
            logging.error(f"{self.name}: invalid workflow cannot be run")
        elif self._invalid_config is None or self._invalid_config:
            logging.error(f"{self.name}: configuration is either missing or incomplete, workflow cannot be run")
            self._invalid_config = True
        else:
            self._dag.bfs_traverse(exec_visitor.complete_runtime_configuration)
            self._dag.bfs_traverse_links(exec_visitor.set_queues)
            self._dag.bfs_traverse(exec_visitor.create_job_from)
        return self.is_valid and not self._invalid_config

    async def schedule(self, exec_visitor: BaseExecutor, *args, **kwargs) -> None:
        if self._prepare_execution(exec_visitor):
            await exec_visitor.schedule(self.name, *args, **kwargs)

    def run(self, exec_visitor: BaseExecutor, *args, **kwargs) -> None:
        if self._prepare_execution(exec_visitor):
            exec_visitor.run(self.name, *args, **kwargs)
