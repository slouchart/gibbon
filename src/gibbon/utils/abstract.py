import re

from abc import abstractmethod
from typing import Any
from contextlib import contextmanager
from enum import *


class Namable:
    __slots__ = ['_name']

    valid_name = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]*$')

    def __init__(self, name: str, *args: Any, **kwargs: Any) -> None:
        self._name = name
        super().__init__(*args, **kwargs)

    @property
    def name(self) -> str:
        return self._name

    @name.setter
    def name(self, value: str) -> None:
        self._name = value

    @staticmethod
    def check_valid_name(name: str) -> bool:
        return name is not None and Namable.valid_name.match(name) is not None


class Configurable:
    @abstractmethod
    def configure(self, **kwargs: Any) -> None:
        ...

    @abstractmethod
    def reset(self) -> None:
        ...


class Validable:
    def __init__(self, *args, **kwargs):
        self._requires_validation = True
        self._valid = False

    def may_require_validation(self):
        self._requires_validation = True

    @property
    def is_valid(self) -> bool:
        if self._requires_validation:
            self.validate(silent=True)

        return self._valid

    @abstractmethod
    def validate(self, verbose: bool = False, silent: bool = False) -> None:
        pass


class Visitor:
    @abstractmethod
    def visit_element(self, name: str, element: Any) -> None:
        ...

    @abstractmethod
    def visit_link(self, elem1: Any, elem2: Any) -> None:
        ...


class VisitMode(Enum):
    elements_only = 1
    links_only = 2
    links_then_elements = 3
    elements_then_links = 4


class Visitable:
    @abstractmethod
    def accept_visitor(self, visitor: Visitor, mode: VisitMode):
        ...


class Buildable:
    def start_build(self):
        pass

    def resume_build(self):
        pass

    def may_require_validation(self):
        pass


class Builder:
    def __init__(self, buildee: Buildable, product_factory, *args, **kwargs):
        self._buildee = buildee
        self._product = product_factory(*args, **kwargs)
        assert self._buildee is not None

    @property
    def product(self):
        return None # implemented by the decorator 'buildable' in derived classes

    def close(self):
        assert self._buildee._builder is self

        # destroy the relationship between the Builder and the Buildable
        # like so, accessing the builder outside of the scope of the with
        # block is still possible but any further invocation of add_component
        # raises an exception
        self._buildee._builder = None
        self._buildee = None
        self._product = None


def make_concrete_buildable(builder_cls: Builder, product_factory, *a_init_product, **kw_init_product):

    builder_out_of_scope = 'Attempt to add a component outside the scope of the builder'

    def inner_decorator(cclass):
        cclass.builder_factory = builder_cls

        def product(self):
            if self._product is None:
                raise RuntimeError(builder_out_of_scope)
            return self._product

        setattr(cclass.builder_factory, 'product', property(product))

        # decorate __init__
        def __initialize(fn):
            def __init__(self, *a, **k):
                self._product = None
                self._builder = None
                fn(self, *a, *k)  # should be <decorated>.__init__
            return __init__

        cclass.__init__ = __initialize(cclass.__init__)

        def start_build(self):
            self._builder = self.builder_factory(self, product_factory, *a_init_product, **kw_init_product)
            yield self._builder
            self._product = self._builder.product
            self._builder.close()
            assert self._builder is None

        def resume_build(self):
            self._builder = self.builder_factory(self, lambda o: o, self._product)
            yield self._builder
            self._product = self._builder.product
            self._builder.close()
            assert self._builder is None

        cclass.start_build = contextmanager(start_build)
        cclass.resume_build = contextmanager(resume_build)

        def _delegator(func):
            def decorated(self, *args, **kwargs):
                if self._builder is not None:
                    func(self._builder, *args, **kwargs)
                else:
                    raise RuntimeError(builder_out_of_scope)
            return decorated

        # design choice: not exploring MRO to search for methods
        # => do not subclass from a subclass of Builder
        for method in (m for m in cclass.builder_factory.__dict__.keys() if not m.startswith('_')):
            setattr(cclass, method, _delegator(getattr(cclass.builder_factory, method)))

        return cclass

    return inner_decorator
