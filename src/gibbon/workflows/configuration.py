

class Configuration:
    def __init__(self):
        self._cfg = dict()

    def add_configuration(self, name, *args, **kwargs):
        self._cfg[name] = (args, kwargs)

    def set_configuration(self, transformation):
        if transformation.name in self._cfg:
            transformation.configure(*self._cfg[transformation.name][0], **self._cfg[transformation.name][1])
        else:
            try:
                transformation.configure()
            except NotImplementedError or AttributeError:
                pass

    def reset_configuration(self, transformation):
        if transformation.name in self._cfg:
            transformation.reset()

