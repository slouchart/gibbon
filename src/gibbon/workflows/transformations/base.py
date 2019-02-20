from abc import abstractmethod
from typing import *

from ..exceptions import TargetAssignmentError


class Transformation:
    def __init__(self, name: str, in_ports: int, out_ports: int, *args, **kwargs):
        self.name = name
        self.in_ports = dict()
        self.out_ports = dict()
        self._initialize_ports(in_ports, out_ports)
        super().__init__(*args, **kwargs)

    @property
    def id(self) -> str:
        return self.name

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

    @abstractmethod
    def set_source(self, parent):
        raise NotImplementedError

    @property
    def has_source(self) -> int:
        return len(self.sources)

    @property
    def sources(self) -> MutableSequence:
        return [n for n in self.in_ports.values() if n is not None]

    @property
    def has_target(self) -> int:
        return len(self.targets)

    @property
    def targets(self) -> MutableSequence:
        return [n for n in self.out_ports.values() if n is not None]

    def add_target(self, target):

        if target in self.out_ports.values():
            raise TargetAssignmentError(f'Source {self.name} already connected to target {target.name}')

        n_port = self._get_next_available_output_port()
        self.out_ports[n_port] = target

    def configure(self, *args, **kwargs):
        pass

    def reset(self):
        pass


class OneToMany(Transformation):
    def __init__(self, *args, out_ports: int = 1, **kwargs) -> None:
        super().__init__(*args, in_ports=1, out_ports=out_ports, **kwargs)

    def set_source(self, parent):
        assert self.in_ports[0] is None
        self.in_ports[0] = parent
        parent.add_target(self)


class ManyToMany(Transformation):
    def __init__(self, *args, in_ports: int = 1, out_ports: int = 1, **kwargs) -> None:
        super().__init__(*args, in_ports=in_ports, out_ports=out_ports, **kwargs)

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

    def set_source(self, *parents):
        # expect a tuple of sources transformations
        for source in parents:
            n_port = self._get_next_available_input_port()
            self.in_ports[n_port] = source
            source.add_target(self)


class StreamProcessor:
    """Mixin that deals with the runtime behaviour of a transformation"""
    def __init__(self, *args, **kwargs) -> None:
        self.in_queues = []
        self.out_queues = []

    def share_queue_with_target(self, target, queue):
        self.out_queues.append(queue)
        target.in_queues.append(queue)

    async def get_row(self):
        return await self.in_queues[0].get()

    def may_stop_process(self, row):
        return row is None

    async def emit_eof(self):
        for q in self.out_queues:
            await q.put(None)

    def process_row(self, row):
        return row

    def can_emit_row(self, row):
        return True

    async def emit_row(self, row):
        if self.can_emit_row(row):
            for q in self.out_queues:
                await q.put(row)

    async def process_rows(self):
        while True:
            row = await self.get_row()
            if self.may_stop_process(row):
                break
            row = self.process_row(row)
            await self.emit_row(row)

        await self.emit_eof()

    def get_async_job(self):
        return self.process_rows

