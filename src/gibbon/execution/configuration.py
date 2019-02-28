import logging
from src.gibbon.utils.abstract import Visitor


class Configuration(Visitor):
    class Element:

        def to_dict(self):
            _dict = {k: v for k, v in self.__dict__.items() if not k.startswith('_')}
            return _dict

        def __getitem__(self, item):
            return self.__dict__[item]

        def __setitem__(self, key, value):
            setattr(self, key, value)

        def using(self, **kwargs):
            self.__dict__.update(kwargs)

    def __init__(self) -> None:
        self._cfg = dict()

    def __getitem__(self, item):
        if item not in self._cfg:
            self._cfg[item] = Configuration.Element()
        return self._cfg[item]

    def __call__(self, item):
        return self.__getitem__(item)

    def configure(self, name):
        return self.__getitem__(name)

    def visit_element(self, name, item):
        try:
            if name in self._cfg:
                item.configure(**self._cfg[name].to_dict())
            else:
                try:
                    item.configure()
                except NotImplementedError or AttributeError:
                    logging.warning(f'Configuration failed for transformation {name}')
        except KeyError as e:
            logging.error(f'{str(e)} Probable missing argument from configuration of {name}')

    def visit_link(self, elem1, elem2):
        ...
