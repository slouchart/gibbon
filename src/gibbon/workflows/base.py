from .transformations.endpoints import Source, Target
from .transformations.endpoints import is_source, is_target
from .exceptions import *
import logging
import re


class DirectedAcyclicGraph:
    def __init__(self):
        self._nodes = dict()
        self._roots = []

    def create_node(self, parent, cls, *args, **kwargs):
        node = cls(*args, **kwargs)

        if node is None:
            return None

        if parent:
            node.set_source(parent)

        if not node.has_source:
            self._roots.append(node)

        self._nodes[node.id] = node

        return node

    def __getitem__(self, item):
        return self._nodes[item]

    def __contains__(self, item):
        return item in self._nodes

    @property
    def nodes(self):
        return self._nodes.values()

    def is_root(self, node):
        return node in self._roots

    @property
    def roots(self):
        return iter(self._roots)

    def check_reachability(self, callback):

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

        for node in self.nodes:
            _check_path_to_source(node)
            _check_path_to_target(node)

    def bfs_traverse_links(self, callback):
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

    def bfs_traverse(self, callback):
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

    def __init__(self, name=None):
        self._dag = DirectedAcyclicGraph()
        self._warnings = []
        self._errors = []
        self._checked = False
        self._valid = False
        self.name = name
        self.check_valid_name(self.name)
        self._invalid_config = None

    def check_valid_name(self, name):
        if not name:
            self._add_error(InvalidNameError, f'Object name is mandatory but appears to be missing')
            return

        regexp = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]*$')
        if not regexp.match(name):
            self._add_error(InvalidNameError, f'Object name is invalid: {name}')

    def get_node_by_name(self, name):
        if name not in self._dag:
            self._add_error(NodeNotFound, f"Node {name} not found")
            return None

        return self._dag[name]

    def _add_error(self, exc, msg=None):
        if isinstance(exc, BaseException):
            self._errors.append(exc)
        else:
            self._errors.append(exc(msg))

    def _add_warning(self, warn, msg=None):
        if isinstance(warn, BaseException):
            self._warnings.append(warn)
        else:
            self._warnings.append(warn(msg))

    def add_source(self, name):
        self._checked = False
        self.check_valid_name(name)

        try:
            self._dag.create_node(None, Source, name)
        except BaseBuildWarning as w:
            self._add_warning(w)
        except BaseException as e:
            self._add_error(e)

    def add_target(self, name, source):
        self._checked = False
        self.check_valid_name(name)

        try:
            if source:
                parent = self.get_node_by_name(source)
            else:
                parent = None
            self._dag.create_node(parent, Target, name)
        except BaseBuildWarning as w:
            self._add_warning(w)
        except BaseException as e:
            self._add_error(e)

    def add_transformation(self, name, type, source, targets=(), **kwargs):

        self._checked = False
        self.check_valid_name(name)

        parent = None
        if source:
            parent = self.get_node_by_name(source)

        try:
            node = self._dag.create_node(parent, type, name, **kwargs)

            for target in targets:
                if target is None:
                    continue
                try:
                    target = self.get_node_by_name(target)
                    target.connect_to_source(node)
                except BaseBuildWarning as w:
                    self._add_warning(w)
        except BaseBuildWarning as w:
            self._add_warning(w)
        except BaseException as e:
            self._add_error(e)


    def add_complex_transformation(self, name, type, sources, targets=(), **kwargs):
        self._checked = False
        self.check_valid_name(name)

        parents = []
        for source in sources:
            parent = self.get_node_by_name(source)
            if parent:
                parents.append(parent)

        node = self._dag.create_node(parents, type, name, **kwargs)

        for target in targets:
            if target is None:
                continue
            try:
                target = self.get_node_by_name(target)
                target.connect_to_source(node)
            except BaseBuildWarning as w:
                self._add_warning(w)

    @property
    def is_valid(self):
        if self._checked:
            pass
        else:
            self.validate()

        return self._valid

    def validate(self, verbose=False):

        logging.info(f"Validating mapping {self.name}")

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
            logging.info(f"Mapping {self.name} is valid.")
        else:
            logging.info(f"Mapping {self.name} is invalid.")

    def get_all_warnings(self):
        s = ""
        for exc in self._warnings:
            s += f"warning: {exc}\n"

        if len(s) == 0:
            s = "No warning."

        return s

    def get_all_errors(self):
        s = ""
        for exc in self._errors:
            s += f"error: {repr(exc)}\n"

        if len(s) == 0:
            s = "No error."

        return s

    def raise_last(self):
        if len(self._errors):
            raise self._errors.pop()

    def prepare(self, cfg_visitor):
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

    def reset(self, cfg_visitor):
        self._dag.bfs_traverse(cfg_visitor.reset_configuration)

    def _prepare_execution(self, exec_visitor):
        if not self.is_valid:
            logging.error(f"{self.name}: invalid workflow cannot be run")
        elif self._invalid_config is None or self._invalid_config:
            logging.error(f"{self.name}: configuration is either missing or incomplete, workflow cannot be run")
            self._invalid_config = True
        else:
            self._dag.bfs_traverse_links(exec_visitor.set_queues)
            self._dag.bfs_traverse(exec_visitor.create_job_from)
        return self.is_valid and not self._invalid_config

    async def schedule(self, exec_visitor, *args, **kwargs):
        if self._prepare_execution(exec_visitor):
            await exec_visitor.schedule(self.name, *args, **kwargs)

    def run(self, exec_visitor, *args, **kwargs):
        if self._prepare_execution(exec_visitor):
            exec_visitor.run(self.name, *args, **kwargs)
