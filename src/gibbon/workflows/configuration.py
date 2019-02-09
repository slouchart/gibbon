import logging


class Configuration:
    def __init__(self):
        self._cfg = dict()

    def add_configuration(self, name, *args, **kwargs):
        if name in self._cfg:
            logging.warning(f'Attempting to supersede configuration of {name}')
        self._cfg[name] = (args, kwargs)

    def set_configuration(self, transformation):
        try:
            if transformation.name in self._cfg:
                transformation.configure(*self._cfg[transformation.name][0], **self._cfg[transformation.name][1])
            else:
                try:
                    transformation.configure()
                except NotImplementedError or AttributeError:
                    logging.warning(f'Configuration failed for transformation {transformation.name}')
        except KeyError as e:
            logging.error(f'{str(e)} Probable missing argument from configuration of {transformation.name}')

    def reset_configuration(self, transformation):
        if transformation.name in self._cfg:
            transformation.reset()

