from .transformations.endpoints import Source, Target
from .exceptions import *


class Mapping:

    def __init__(self, name=None):
        self._nodes = dict()
        self._roots = []
        self._warnings = []
        self._errors = []
        self._checked = False
        self._valid = False
        self.name = name if name else '<unamed>'

    def _create_node(self, parent, cls, *args, **kwargs):
        node = None
        try:
            node = cls(*args, **kwargs)
        except BaseBuildWarning as w:
            self._add_warning(w)
        except BaseBuildError as e:
            self._add_error(e)

        if node is None:
            return None

        if self._exists(node):
            self._add_error(NodeAlreadyExist(f'Duplicate name {node}'))

        if parent:
            node.set_source(parent)

        if not node.has_source:
            self._roots.append(node)

        self._nodes[node.id] = node

        self._ckecked = False

        return node

    def _get_node_by_name(self, name):
        if name not in self._nodes:
            self._add_error(NodeNotFound(f"Node {name} not found"))
            return None
        return self._nodes[name]

    def _exists(self, node):
        return node.id in self._nodes

    def _check_reachability(self):
        """reachability invariant #1: there must be at least one path connecting any root source to a target
           reachability invariant #2: there must be at least one path connecting any target to a root source"""

        def _check_path_to_source(node):

            if node.has_source:
                for source in node.sources:
                    _check_path_to_source(source)
            elif node not in self._roots:
                self._add_error(NodeReachabilityError(f'Source {node.name} unconnected'))
            else:
                pass

        def _check_path_to_target(node):
            if node.has_target:
                for child in node.targets:
                    _check_path_to_target(child)
            elif isinstance(node, Target):
                pass
            else:
                self._add_error(NodeReachabilityError(f"Transformation {node.name} unconnected to a target"))

        targets = [n for n in self._nodes.values() if n.has_target]
        for target in targets:
            _check_path_to_source(target)

        for source in self._roots:
            _check_path_to_target(source)

    def _add_error(self, exc):
        self._errors.append(exc)

    def _add_warning(self, warn):
        self._warnings.append(warn)

    def add_source(self, name, ports=1):
        self._checked = False
        node = self._create_node(None, Source, name)

    def add_target(self, name, source):
        self._checked = False
        parent = self._get_node_by_name(source)
        node = self._create_node(parent, Target, name)

    def add_transformation(self, name, type, source, targets=(), **kwargs):

        self._checked = False

        parent = None
        if source:
            parent = self._get_node_by_name(source)

        node = self._create_node(parent, type, name, **kwargs)

        for target in targets:
            if target is None:
                continue
            try:
                target = self._get_node_by_name(target)
                target.connect_to_source(node)
            except BaseBuildWarning as w:
                self._add_warning(w)

    def add_complex_transformation(self, name, type, sources, targets=(), **kwargs):
        self._checked = False
        parents = []
        for source in sources:
            parent = self._get_node_by_name(source)
            if parent:
                parents.append(parent)

        node = self._create_node(parents, type, name, **kwargs)

        for target in targets:
            if target is None:
                continue
            try:
                target = self._get_node_by_name(target)
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

        if verbose:
            print(f"Validating mapping {self.name}")

        # is there any previous build errors?
        if len(self._errors):
            self._valid = False
        else:
            # is the reachability predicate true?
            self._check_reachability()

            if len(self._errors):
                self._valid = False
            else:
                self._valid = True

        self._checked = True

        if verbose:
            print(self.get_all_errors())
            print(self.get_all_warnings())
            if self.is_valid:
                print(f"Mapping {self.name} is valid.")
            else:
                print(f"Mapping {self.name} is invalid.")

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
            s += f"error: {exc}\n"

        if len(s) == 0:
            s = "No error."

        return s

    def _bfs_traverse_links(self, callback):
        queue = []
        visited = set()
        for n in self._roots:
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

    def _bfs_traverse(self, callback):
        queue = []
        visited = set()
        for n in self._roots:
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

    def prepare(self, cfg_visitor):
        self._bfs_traverse(cfg_visitor.set_configuration)

    def reset(self, cfg_visitor):
        self._bfs_traverse(cfg_visitor.reset_configuration)

    def run(self, exec_visitor, *args, **kwargs):
        if not self.is_valid:
            raise RuntimeWarning(f"{self.name}: invalid mapping cannot be run")

        self._bfs_traverse_links(exec_visitor.set_queues)
        self._bfs_traverse(exec_visitor.create_job_from)
        exec_visitor.run(self.name, *args, **kwargs)
