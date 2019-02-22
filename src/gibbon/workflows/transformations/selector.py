from .base import *
from ..exceptions import BaseBuildWarning


class SelectorHasTooManyTargets(BaseBuildWarning):
    ...


class Selector(UpStreamable, MonoDownStreamable, Transformation):
    """Dispatch input rows according to some conditions"""
    def __init__(self, name: str, conditions: Collection, *args: Any, **kwargs: Any):
        self.conditions = conditions
        super().__init__(name, *args, **kwargs)
        assert len(self.targets) <= len(self.conditions)

    def add_target(self, target):
        super().add_target(target)
        if len(self.targets) > len(self.conditions)+1:
            raise SelectorHasTooManyTargets(f'Selector {self.name} has too many targets')

    async def process_rows(self):
        while True:
            row = await self.get_row()
            if self.may_stop_process(row):
                break

            row_is_emitted = False
            for (cond, oq) in zip(self.conditions, self.out_queues):
                if cond(row):
                    await oq.put(row)
                    row_is_emitted = True
            if not row_is_emitted:
                if len(self.out_queues) > len(self.conditions):
                    default_queue = self.out_queues[len(self.conditions)]
                    await default_queue.put(row)

        await self.emit_eof()
