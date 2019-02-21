from typing import *
from abc import abstractmethod


from ..util import Namable
from ..exceptions import InvalidNameError


class Connectable:
    @abstractmethod
    def set_source(self, parent):
        raise NotImplementedError("Please implement 'set_source' in derived class")

    @abstractmethod
    def set_sources(self, *parents):
        raise NotImplementedError("Please implement 'set_sources' in derived class")

    @property
    @abstractmethod
    def has_source(self) -> int:
        raise NotImplementedError("Please implement 'has_source' in derived class")

    @property
    @abstractmethod
    def sources(self) -> ValuesView:
        raise NotImplementedError("Please implement 'sources' in derived class")

    @property
    @abstractmethod
    def has_target(self) -> int:
        raise NotImplementedError("Please implement 'has_target' in derived class")

    @property
    @abstractmethod
    def targets(self) -> ValuesView:
        raise NotImplementedError("Please implement 'targets' in derived class")

    @abstractmethod
    def add_target(self, target):
        raise NotImplementedError("Please implement 'add_target' in derived class")

    @abstractmethod
    def reset_target(self, target):
        raise NotImplementedError("Please implement 'reset_target' in derived class")


class UpStreamable(Connectable):
    def __init__(self, *args, **kwargs) -> None:
        self.out_ports = dict()
        super().__init__(*args, **kwargs)

    def set_source(self, parent):
        super().set_source(parent)

    def set_sources(self, *parents):
        super().set_sources(*parents)

    @property
    def has_source(self) -> int:
        return super().has_source

    @property
    def sources(self) -> ValuesView:
        return super().sources

    @property
    def has_target(self) -> int:
        return len(self.targets)

    @property
    def targets(self) -> MutableSequence:
        return [n for n in self.out_ports.values() if n is not None]

    def add_target(self, target):
        n_port = self._get_next_available_output_port()
        self.out_ports[n_port] = target

    def reset_target(self, target):
        for k, v in self.out_ports.items():
            if v is target:
                del self.out_ports[k]
                break

    def _initialize_output_ports(self, out_ports):
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


class NotUpStreamable(Connectable):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        if hasattr(self, 'output_ports'):
            raise TypeError("Cannot base class NotUpStreamable and UpStreamable")

    def set_source(self, parent):
        super().set_source(parent)

    def set_sources(self, *parents):
        super().set_sources(*parents)

    @property
    def has_source(self) -> int:
        return super().has_source

    @property
    def sources(self) -> ValuesView:
        return super().sources

    @property
    def has_target(self) -> int:
        # Invoking 'has_target' makes no sense
        return 0

    @property
    def targets(self) -> MutableSequence:
        # Invoking 'targets' makes no sense
        return []

    def add_target(self, target):
        # Invoking 'add_target' makes no sense
        ...

    def reset_target(self, target):
        # Invoking 'reset_target' makes no sense
        ...


class MultiDownStreamable(Connectable):
    def __init__(self, *args, **kwargs):
        self.in_ports = dict()
        super().__init__(*args, **kwargs)

    def set_source(self, parent):
        self.set_sources(parent)

    def set_sources(self, *parents):
        # expect a tuple of sources transformations
        for source in parents:
            n_port = self._get_next_available_input_port()
            self.in_ports[n_port] = source
            source.add_target(self)

    @property
    def has_source(self) -> int:
        return len(self.sources)

    @property
    def sources(self) -> ValuesView:
        return self.in_ports.values()

    @property
    def has_target(self) -> int:
        return super().has_target

    @property
    def targets(self) -> ValuesView:
        return super().targets

    def add_target(self, target):
        super().add_target(target)

    def reset_target(self, target):
        super().reset_target(target)

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


class MonoDownStreamable(Connectable):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.in_port = None

    def set_source(self, parent):
        if self.in_port is not None:
            self.in_port.reset_target(self)
        self.in_port = parent
        parent.add_target(self)

    def set_sources(self, *parents):
        raise TypeError("Cannot invoke 'set_sources' on a mono downstreamable object, use 'set_source' instead")

    @property
    def has_source(self) -> int:
        return self.in_port is not None

    @property
    def sources(self) -> ValuesView:
        if self.in_port is not None:
            return {None: self.in_port}.values()
        else:
            return {}.values()

    @property
    def has_target(self) -> int:
        return super().has_target

    @property
    def targets(self) -> ValuesView:
        return super().targets

    def add_target(self, target):
        super().add_target(target)

    def reset_target(self, target):
        super().reset_target(target)


class NotDownStreamable(Connectable):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        if hasattr(self, 'input_ports'):
            raise TypeError("Cannot base class NotDownStreamable and DownStreamable")

    def set_source(self, parent):
        raise TypeError("Cannot invoke 'set_source' on a NotDownStreamable object")

    def set_sources(self, *parents):
        raise TypeError("Cannot invoke 'set_sources' on a NotDownStreamable object")

    @property
    def has_source(self) -> int:
        return 0

    @property
    def sources(self) -> MutableSequence:
        return []

    @property
    def has_target(self) -> int:
        return len(self.targets)

    @property
    def targets(self) -> ValuesView:
        return super().targets

    def add_target(self, target):
        return super().add_target(target)

    def reset_target(self, target):
        return super().reset_target(target)


class AbstractTransformation(Namable):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        if not Namable.check_valid_name(self.name):
            raise InvalidNameError(f'Object name is invalid: {self.name}')

    @property
    def id(self) -> str:
        return self.name

    def configure(self, *args, **kwargs):
        ...

    def reset(self):
        ...


class StreamProcessor:
    """Mixin that deals with the runtime behaviour of a transformation"""
    def __init__(self, *args, **kwargs) -> None:
        self.in_queues = []
        self.out_queues = []
        super().__init__(*args, **kwargs)

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


class Transformation(AbstractTransformation, StreamProcessor):
    ...
