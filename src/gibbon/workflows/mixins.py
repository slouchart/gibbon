from typing import *
from abc import abstractmethod


from ..utils.abstract import Namable, Configurable
from .exceptions import NodeNotFound  # exceptions
from .exceptions import ParentNodeReset, DuplicatedSource  # warnings


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
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.out_ports = dict()
        super().__init__(*args, **kwargs)

    def set_source(self, parent: Connectable):
        super().set_source(parent)

    def set_sources(self, *parents: Connectable):
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

    def add_target(self, target: Connectable):
        n_port = self._get_next_available_output_port()
        self.out_ports[n_port] = target

    def reset_target(self, target: Connectable):
        for k, v in self.out_ports.items():
            if v is target:
                del self.out_ports[k]
                raise ParentNodeReset('Attempt made to reset the target')
        else:
            raise NodeNotFound('Target to reset was not found')

    def _initialize_output_ports(self, out_ports: int):
        for p in range(out_ports):
            self.out_ports[p] = None

    def _extend_output_ports(self) -> int:
        n_port = len(self.out_ports)
        self.out_ports[n_port] = None
        return n_port

    def _get_next_available_output_port(self) -> int:
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
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        if hasattr(self, 'output_ports'):
            raise TypeError("Cannot base class NotUpStreamable and UpStreamable")

    def set_source(self, parent: Connectable):
        super().set_source(parent)

    def set_sources(self, *parents: Connectable):
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

    def add_target(self, target: Connectable):
        # Invoking 'add_target' makes no sense
        raise TypeError("Cannot invoke 'add_target' on a NotUpStreamable object")

    def reset_target(self, target: Connectable):
        # Invoking 'reset_target' makes no sense
        raise TypeError("Cannot invoke 'reset_target' on a NotUpStreamable object")


class MultiDownStreamable(Connectable):
    def __init__(self, *args: Any, **kwargs: Any):
        self.in_ports = dict()
        super().__init__(*args, **kwargs)

    def set_source(self, parent: Connectable):
        self.set_sources(parent)

    def set_sources(self, *parents: Connectable):
        # expect a tuple of sources transformations
        for source in parents:

            if source in self.in_ports.values():
                raise DuplicatedSource(f'Duplicated source: {source.name}')

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

    def add_target(self, target: Connectable):
        super().add_target(target)

    def reset_target(self, target: Connectable):
        super().reset_target(target)

    def _extend_input_ports(self) -> int:
        n_port = len(self.in_ports)
        self.in_ports[n_port] = None
        return n_port

    def _get_next_available_input_port(self) -> int:
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
    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self.in_port = None

    def set_source(self, parent: Connectable):
        if self.in_port is not None:
            self.in_port.reset_target(self)
        self.in_port = parent
        parent.add_target(self)

    def set_sources(self, *parents: Connectable):
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

    def add_target(self, target: Connectable):
        super().add_target(target)

    def reset_target(self, target: Connectable):
        super().reset_target(target)


class NotDownStreamable(Connectable):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        if hasattr(self, 'input_ports'):
            raise TypeError("Cannot base class NotDownStreamable and DownStreamable")

    def set_source(self, parent: Connectable):
        raise TypeError("Cannot invoke 'set_source' on a NotDownStreamable object")

    def set_sources(self, *parents: Connectable):
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

    def add_target(self, target: Connectable):
        return super().add_target(target)

    def reset_target(self, target: Connectable):
        return super().reset_target(target)


class RowProcessor:
    """Mixin that deals with the runtime behaviour of a transformation"""
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.in_queues = []
        self.out_queues = []

    @abstractmethod
    def process_row(self, row):
        ...

    async def get_row(self):
        if len(self.in_queues) == 1:
            return await self.in_queues[0].get()
        else:
            raise NotImplementedError

    def may_stop_process(self, row):
        return row is None

    async def emit_eof(self):
        for q in self.out_queues:
            await q.put(None)


class StreamProcessor(RowProcessor):

    def share_queue_with_target(self, target, queue) -> None:
        self.out_queues.append(queue)
        target.in_queues.append(queue)

    def on_start_process_rows(self):
        ...

    def can_emit_row(self, row):
        return True

    async def emit_row(self, row):
        if self.can_emit_row(row):
            for q in self.out_queues:
                await q.put(row)

    def process_row(self, row):
        return row

    async def process_rows(self):
        self.on_start_process_rows()
        while True:
            row = await self.get_row()
            if self.may_stop_process(row):
                break
            row = self.process_row(row)
            await self.emit_row(row)

        await self.emit_eof()

    def get_async_job(self):
        return self.process_rows


class Bufferized(RowProcessor):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.buffer = []
        super().__init__(*args, **kwargs)

    def process_row(self, row):
        row = super().process_row(row)
        self.buffer.append(row)

    @abstractmethod
    def process_buffer(self):
        ...

    async def emit_buffer(self):
        for row in self.buffer:
            for oq in self.out_queues:
                await oq.put(row)

    async def process_rows(self):
        while True:
            row = await self.get_row()
            if self.may_stop_process(row):
                break

            self.process_row(row)

        self.process_buffer()
        await self.emit_buffer()

        await self.emit_eof()


class Transformation(Namable, Configurable, StreamProcessor):
    def configure(self, **kwargs: Any) -> None:
        ...

    def reset(self) -> None:
        ...
