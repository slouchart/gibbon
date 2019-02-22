from .base import *
from ..exceptions import MissingArgumentError


class EndPoint(Namable, Configurable, StreamProcessor):
    @abstractmethod
    def configure(self, *args, **kwargs):
        ...

    @abstractmethod
    def reset(self):
        ...


class Source(NotDownStreamable, UpStreamable, EndPoint):
    """A near abstract source of data. THe actual stream generator is provided at runtime by the Configuration feature
    the actual source must expose a asynchronous interface for async iter and async context management"""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.actual_source = None
        self.source_cfg = None

    def configure(self, **kwargs):
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


class Target(NotUpStreamable, MonoDownStreamable, EndPoint):
    """A near abstract model of a downstream target whether a file or a database.
    The actual target is specified at runtime with the Configuration.
    The target will perform blocking operations unless it is defined as non-blocking.
    Therefore we have an implementation mismatch here :/"""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.actual_target = None
        self.target_cfg = None

    def configure(self, **kwargs):
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
