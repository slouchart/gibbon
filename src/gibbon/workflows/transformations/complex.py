from .base import Transformation
from abc import abstractmethod


class ManyToMany(Transformation):
    def __init__(self, name, in_ports=1, out_ports=1):
        super().__init__(name, in_ports, out_ports)

    def _extend_input_ports(self):
        n_port = len(self.in_ports)
        self.in_ports[n_port] = None
        return n_port

    def _get_next_available_input_port(self):
        n_port = None
        for k, v in self.in_ports.items():
            n_port = k
            if v is None:
                break
            else:
                n_port = None
        else:
            if n_port is None:
                n_port = self._extend_input_ports()

        return n_port

    def set_source(self, parent_transfo):
        # expect a tuple of sources transformations
        for source in parent_transfo:
            n_port = self._get_next_available_input_port()
            self.in_ports[n_port] = source
            source.add_target(self)

    @abstractmethod
    def get_async_job(self):
        raise NotImplementedError


