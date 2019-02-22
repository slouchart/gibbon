import logging

from .mixins import Configurable
from ..utils.abstract import Visitor


class Configuration(Visitor):
    class Element:
        def __init__(self):
            self.__dict__['_sub_dict'] = set()

        def __getattr__(self, item):
            return self.__dict__[item]

        def __setattr__(self, key, value):
            if key not in self.__dict__:
                self._sub_dict.add(key)

            self.__dict__[key] = value

        def to_dict(self):
            cfg = dict()
            for item in self._sub_dict:
                cfg[item] = self.__dict__[item]
            return cfg

    def __init__(self) -> None:
        self._cfg = dict()
        self.can_traverse_nodes = True
        self.can_traverse_links = False

    def __getitem__(self, item):
        if item in self._cfg:
            return self._cfg[item]
        else:
            self._cfg[item] = Configuration.Element()
            return self._cfg[item]

    def add_configuration(self, name: str, **kwargs) -> None:
        if name in self._cfg:
            logging.warning(f'Attempting to supersede configuration of {name}')

        for k, v in kwargs.items():
            self[name].__setattr__(k, v)

    def set_configuration(self, name, transformation: Configurable) -> None:
        try:
            if name in self._cfg:
                transformation.configure(**self._cfg[name].to_dict())
            else:
                try:
                    transformation.configure()
                except NotImplementedError or AttributeError:
                    logging.warning(f'Configuration failed for transformation {name}')
        except KeyError as e:
            logging.error(f'{str(e)} Probable missing argument from configuration of {name}')

    def reset_configuration(self, name, transformation: Configurable) -> None:
        if name in self._cfg:
            transformation.reset()
            del self._cfg[name]

    def visit_element(self, name, item):
        self.set_configuration(name, item)

    def visit_link(self, elem1, elem2):
        ...
