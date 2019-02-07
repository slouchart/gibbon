from .exceptions import ConfigurationError


class Configuration:
    def __init__(self):
        self._cfg = dict()
        self._errors = []

    def add_configuration(self, name, *args, **kwargs):
        self._cfg[name] = (args, kwargs)

    def set_configuration(self, transformation):
        try:
            if transformation.name in self._cfg:
                transformation.configure(*self._cfg[transformation.name][0], **self._cfg[transformation.name][1])
            else:
                try:
                    transformation.configure()
                except NotImplementedError or AttributeError:
                    pass
        except ConfigurationError as e:
            self._errors.append(e)

    def reset_configuration(self, transformation):
        if transformation.name in self._cfg:
            transformation.reset()

    @property
    def has_errors(self):
        return len(self._errors)

    @property
    def errors(self):
        return iter(self._errors)
