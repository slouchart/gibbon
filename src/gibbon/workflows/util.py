from abc import abstractmethod
from typing import Any
import re
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
    def configure(self, **kwargs):
        ...

    @abstractmethod
    def reset(self):
        ...


class Visitor:
    @abstractmethod
    def visit_element(self, name, element):
        ...

    @abstractmethod
    def visit_link(self, elem1, elem2):
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
