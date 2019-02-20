import logging
import re
from typing import *

from .transformations.base import Transformation
from .transformations.endpoints import Source, Target
from .transformations.endpoints import is_source, is_target
from .configuration import Configuration
from ..execution.base import BaseExecutor
from .exceptions import *


class DirectedAcyclicGraph:
    def __init__(self) -> None:
        self._nodes = dict()
        self._roots = []

    def create_node(self,
                    cls: Type[Transformation], *args,
                    parent: Transformation = None, **kwargs) \
            -> Union[Transformation, None]:
        node = cls(*args, **kwargs)

        if node is None:
            return None

        if parent:
            if isinstance(parent, list):
                node.set_source(*parent)
            else:
                node.set_source(parent)

        if is_source(node):
            self._roots.append(node)

        self._nodes[node.id] = node

        return node

    def __getitem__(self, item: str) -> Transformation:
        return self._nodes[item]

    def __contains__(self, item: str) -> bool:
        return item in self._nodes

    @property
    def nodes(self) -> Iterable:
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


class Workflow:

    def __init__(self, name: str = None) -> None:
        self._dag = DirectedAcyclicGraph()
        self._warnings = []
        self._errors = []
        self._checked = False
        self._valid = False
        self.name = name
        self.check_valid_name(self.name)
        self._invalid_config = None

    def check_valid_name(self, name: str) -> None:
        if not name:
            self._add_error(InvalidNameError, f'Object name is mandatory but appears to be missing')
            return

        regexp = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]*$')
        if not regexp.match(name):
            self._add_error(InvalidNameError, f'Object name is invalid: {name}')

    def get_node_by_name(self, name: str) -> Union[Transformation, None]:
        if name not in self._dag:
            self._add_error(NodeNotFound, f"Node {name} not found")
            return None

        return self._dag[name]

    def _add_error(self, exc: Union[BaseException, Type[BaseBuildError]], msg: str = None) -> None:
        if isinstance(exc, BaseException):
            self._errors.append(exc)
        else:
            self._errors.append(exc(msg))

    def _add_warning(self, warn: Union[BaseException, Type[BaseBuildWarning]], msg: str = None) -> None:
        if isinstance(warn, BaseException):
            self._warnings.append(warn)
        else:
            self._warnings.append(warn(msg))

    def add_source(self, name: str) -> None:
        self._checked = False
        self.check_valid_name(name)

        try:
            self._dag.create_node(Source, name)
        except BaseBuildWarning as w:
            self._add_warning(w)
        except BaseException as e:
            self._add_error(e)

    def add_target(self, name: str, source: str = None) -> None:
        self._checked = False
        self.check_valid_name(name)

        try:
            if source:
                parent = self.get_node_by_name(source)
            else:
                parent = None
            self._dag.create_node(Target, name, parent=parent)
        except BaseBuildWarning as w:
            self._add_warning(w)
        except BaseException as e:
            self._add_error(e)

    def add_transformation(self, name: Text,
                           cls: Type[Transformation],
                           *args,
                           sources: Union[Tuple, Text] = None,
                           targets: Tuple = (),
                           **kwargs) -> None:

        self._checked = False
        self.check_valid_name(name)

        parents = None
        if sources and isinstance(sources, tuple) or isinstance(sources, list):
            parents = []
            for source in sources:
                parent = self.get_node_by_name(source)
                if parent:
                    parents.append(parent)
        elif sources is not None:
            parents = self.get_node_by_name(sources)

        try:
            node = self._dag.create_node(cls, name, *args, parent=parents, **kwargs)

            for target in targets:
                if target is None:
                    continue

                target = self.get_node_by_name(target)
                target.set_source(node)

        except BaseBuildWarning as w:
            self._add_warning(w)
        except BaseException as e:
            self._add_error(e)

    def connect(self, source: str, *targets: str) -> None:
        if source:
            source = self.get_node_by_name(source)

        for target in targets:
            target = self.get_node_by_name(target)
            if target and source:
                target.set_source(source)

    @property
    def is_valid(self) -> bool:
        if self._checked:
            pass
        else:
            self.validate()

        return self._valid

    def validate(self, verbose: bool = False) -> None:

        logging.info(f"Validating workflow {self.name}")

        # is there any previous build errors?
        if len(self._errors):
            self._valid = False
        else:
            # is the reachability predicate true?
            self._dag.check_reachability(self._add_error)

            if len(self._errors):
                self._valid = False
            else:
                self._valid = True

        self._checked = True

        if verbose:
            logging.info(self.get_all_errors())
            logging.info(self.get_all_warnings())
        if self.is_valid:
            logging.info(f"Workflow {self.name} is valid.")
        else:
            logging.info(f"Workflow {self.name} is invalid.")

    def get_all_warnings(self) -> str:
        s = ""
        for exc in self._warnings:
            s += f"warning: {exc}\n"

        if len(s) == 0:
            s = "No warning."

        return s

    def get_all_errors(self) -> str:
        s = ""
        for exc in self._errors:
            s += f"error: {repr(exc)}\n"

        if len(s) == 0:
            s = "No error."

        return s

    def raise_last(self) -> None:
        if len(self._errors):
            raise self._errors.pop()

    def prepare(self, cfg_visitor: Configuration) -> None:
        if not self.is_valid:
            logging.error(f"{self.name}: invalid workflow cannot be configured")
        else:
            try:
                self._dag.bfs_traverse(cfg_visitor.set_configuration)
                self._invalid_config = False
            except ConfigurationError as e:
                self._add_error(e)
                logging.error(str(e))
                self._invalid_config = True

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
