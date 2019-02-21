from typing import Any
import re


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
