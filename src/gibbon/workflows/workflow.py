import logging
from typing import *

from .endpoints import Source, Target
from .exceptions import *
from .graph import DirectedAcyclicGraph
from .mixins import Transformation, Connectable
from ..utils.abstract import Builder, Buildable, make_concrete_buildable
from ..utils.abstract import Namable, Validable, Visitable, VisitMode, Visitor


class WorkflowBuilder(Builder):

    @staticmethod
    def _element_factory(cls, name, *args, **kwargs):
        if not Namable.check_valid_name(name):
            raise InvalidNameError(f'Object name is invalid: {name}')

        return cls(name, *args, **kwargs)

    def get_node_by_name(self, name: str) -> Union[Connectable, None]:
        if name not in self.product:
            raise NodeNotFound(f"Node {name} not found")
        else:
            return self.product[name]

    def add_source(self, name: str) -> None:
        element = self._element_factory(Source, name)
        self.product.insert_node(name, element, None)
        self._buildee.may_require_validation()

    def add_target(self, name: str, source: str = None) -> None:
        if source:
            parent = self.get_node_by_name(source)
        else:
            parent = None
        element = self._element_factory(Target, name)
        self.product.insert_node(name, element, parent)
        self._buildee.may_require_validation()

    def add_transformation(self, name: Text,
                           cls: Type[Transformation],
                           *args,
                           sources: Union[Collection[str], str] = None,
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
        self.product.insert_node(name, element, parents)

        for target in targets:
            if target:
                target = self.get_node_by_name(target)
                target.set_source(element)

        self._buildee.may_require_validation()

    def connect(self, source: str, *targets: str) -> None:
        if source:
            source = self.get_node_by_name(source)

        for target in targets:
            target = self.get_node_by_name(target)
            if target and source:
                self._buildee.may_require_validation()
                target.set_source(source)


@make_concrete_buildable(WorkflowBuilder, product_factory=DirectedAcyclicGraph)
class Workflow(Buildable, Namable, Validable, Visitable):

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        if not Namable.check_valid_name(self.name):
            raise InvalidNameError(f'Object name is invalid: {self.name}')

    def validate(self, verbose: bool = False, silent: bool = False) -> None:

        if not self._requires_validation:
            logging.info(f"Workflow {self.name} does not seem to require validation")

        logging.info(f"Validating workflow {self.name}")

        errors = []

        def on_error(exc_type, msg):
            errors.append(exc_type(msg))

        if len(self._product) == 0:
            on_error(NodeNotFound, f'No transformation defined, workflow {self.name} is empty')
        else:
            self._product.check_reachability(on_error)

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
            self._product.bfs_traverse(visitor.visit_element)

        elif mode == VisitMode.elements_then_links:
            self._product.bfs_traverse(visitor.visit_element)
            self._product.bfs_traverse_links(visitor.visit_link)

        elif mode == VisitMode.links_only:
            self._product.bfs_traverse_links(visitor.visit_link)

        elif mode == VisitMode.links_then_elements:
            self._product.bfs_traverse_links(visitor.visit_link)
            self._product.bfs_traverse(visitor.visit_element)
