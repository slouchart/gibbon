from .base import Transformation, StreamProcessor
from ..exceptions import TargetAssignmentError, MissingArgumentError
from abc import abstractmethod


class AbstractEndPoint:
    @abstractmethod
    def configure(self, *args, **kwargs):
        raise NotImplementedError


class Source(Transformation, AbstractEndPoint, StreamProcessor):
    """A near abstract source of data. THe actual stream generator is provided at runtime by the Configuration feature
    the actual source must expose a asynchronous interface for async iter and async context management"""
    def __init__(self, *args, out_ports=1, **kwargs):
        super().__init__(*args, in_ports=0, out_ports=out_ports, **kwargs)
        self.actual_source = None
        self.source_cfg = None

    @property
    def has_source(self):
        return False

    @property
    def sources(self):
        return []

    def set_source(self, parent_transfo):
        raise AttributeError(f"{self.name}: cannot set the source of a Source transformation")

    def configure(self, *args, **kwargs):
        if 'source' in kwargs:
            self.actual_source = kwargs.pop('source')
            self.source_cfg = kwargs
        else:
            if self.actual_source is None:
                raise MissingArgumentError(f"Argument 'source' is missing for configuring source {self.name}")
            else:
                self.source_cfg.update(kwargs)

    def reset(self):
        self.actual_source = None
        self.source_cfg = None

    async def process_rows(self):
        async with self.actual_source(**self.source_cfg) as src:
            async for row in src:
                await self.emit_row(row)

            await self.emit_eof()


class Target(Transformation, AbstractEndPoint, StreamProcessor):
    """A near abstract model of a downstream target whether a file or a database.
    The actual target is specified at runtime with the Configuration.
    The target will perform blocking operations unless it is defined as non-blocking.
    Therefore we have an implementation mismatch here :/"""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, in_ports=1, out_ports=0, **kwargs)
        self.actual_target = None
        self.target_cfg = None

    def set_source(self, parent_transfo):
        assert self.in_ports[0] is None
        self.in_ports[0] = parent_transfo
        parent_transfo.add_target(self)

    @property
    def has_target(self):
        return False

    @property
    def targets(self):
        return []

    def add_target(self, target):
        raise TargetAssignmentError(f"{self.name}: cannot assign a target to a Target")

    def configure(self, *args, **kwargs):
        if 'target' in kwargs:
            self.actual_target = kwargs.pop('target')
            self.target_cfg = kwargs
        else:
            if self.actual_target is None:
                raise MissingArgumentError(f"Argument 'target' is missing for configuring target {self.name}")
            else:
                self.target_cfg.update(kwargs)

    def reset(self):
        self.actual_target = None
        self.target_cfg = None

    async def process_rows(self):
        async with self.actual_target(**self.target_cfg) as tgt:
            while True:
                row = await self.get_row()
                if self.may_stop_process(row):
                    break
                await tgt.send(row)


def is_source(o):
    return isinstance(o, Source)


def is_target(o):
    return isinstance(o, Target)


def is_endpoint(o):
    return is_source(o) or is_target(0)
