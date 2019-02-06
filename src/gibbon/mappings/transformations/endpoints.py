from .base import Transformation
from ..exceptions import TargetAssignmentError


class AbstractEndPoint:
    pass


class Source(Transformation, AbstractEndPoint):

    def __init__(self, name, ports=1):
        super().__init__(name, in_ports=0, out_ports=ports)
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
        self.actual_source = kwargs['source']
        del kwargs['source']
        self.source_cfg = kwargs

    def reset(self):
        self.actual_source = None
        self.source_cfg = None

    def get_async_job(self):
        async def job():
            with self.actual_source(**self.source_cfg) as src:
                for row in src:
                    for q in self.out_queues:
                        await q.put(row)

                for q in self.out_queues:
                    await q.put(None)  # EOF
        return job

    def get_sync_job(self):
        def job():
            with self.actual_source(**self.source_cfg) as src:
                for row in src:
                    for q in self.out_queues:
                        q.put(row)

                for q in self.out_queues:
                    q.put(None)  # EOF
        return job


class Target(Transformation, AbstractEndPoint):
    def __init__(self, name):
        super().__init__(name, in_ports=1, out_ports=0)
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

    def add_target(self, target_transfo):
        raise TargetAssignmentError(f"{self.name}: cannot assign a target to a Target")

    def configure(self, *args, **kwargs):
        self.actual_target = kwargs['target']
        del kwargs['target']
        self.target_cfg = kwargs

    def reset(self):
        self.actual_target = None
        self.target_cfg = None

    def get_async_job(self):
        async def job():
            with self.actual_target(**self.target_cfg) as tgt:
                while True:
                    row = await self.in_queues[0].get()
                    if row is None:
                        break
                    tgt.send(row)
        return job

    def get_sync_job(self):
        def job():
            with self.actual_target(**self.target_cfg) as tgt:
                while True:
                    row = self.in_queues[0].get()
                    if row is None:
                        break
                    tgt.send(row)
        return job


def is_source(o):
    return isinstance(o, Source) or issubclass(o, Source)


def is_target(o):
    return isinstance(o, Target) or issubclass(o, Target)


def is_endpoint(o):
    return is_source(o) or is_target(0)
