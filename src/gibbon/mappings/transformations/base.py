

class AbstractMappingNode:
    def __init__(self, name):
        self.name = name

    @property
    def id(self):
        return self.name


class Transformation(AbstractMappingNode):
    def __init__(self, name, in_ports, out_ports):
        super().__init__(name)
        self.in_ports = dict()
        self.out_ports = dict()
        self._initialize_ports(in_ports, out_ports)
        self.in_queues = []
        self.out_queues = []

    def _initialize_ports(self, in_ports, out_ports):
        for p in range(in_ports):
            self.in_ports[p] = None

        for p in range(out_ports):
            self.out_ports[p] = None

    def _extend_output_ports(self):
        n_port = len(self.out_ports)
        self.out_ports[n_port] = None
        return n_port

    def _get_next_available_output_port(self):
        n_port = None
        for k, v in self.out_ports.items():
            n_port = k
            if v is None:
                break
            else:
                n_port = None
        else:
            if n_port is None:
                n_port = self._extend_output_ports()

        return n_port

    def set_source(self, parent_transfo):
        raise NotImplementedError

    @property
    def has_source(self):
        return len(self.sources)

    @property
    def sources(self):
        return [n for n in self.in_ports.values() if n is not None]

    @property
    def has_target(self):
        return len(self.targets)

    @property
    def targets(self):
        return [n for n in self.out_ports.values() if n is not None]

    def add_target(self, target_transfo):
        n_port = self._get_next_available_output_port()
        self.out_ports[n_port] = target_transfo

    def share_queue_with_target(self, target, queue):
        assert target in self.out_ports.values()
        self.out_queues.append(queue)
        target.in_queues.append(queue)

    def configure(self, *args, **kwargs):
        pass

    def reset(self):
        pass

    def get_async_job(self):
        raise NotImplementedError

    def get_sync_job(self):
        raise NotImplementedError









