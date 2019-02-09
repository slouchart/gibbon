from .base import ManyToMany
from .base import OneToMany


class Concat(ManyToMany):
    def __init__(self, name, in_ports=2, out_ports=1):
        super().__init__(name, in_ports, out_ports)

    def get_async_job(self):
        pass


class Split(OneToMany):
    def __init__(self, name, out_ports=2):
        super().__init__(name, out_ports)

    def get_async_job(self):
        pass


class Restrict(OneToMany):
    def __init__(self, name, out_ports=1):
        super().__init__(name, out_ports)

    def get_async_job(self):
        pass
