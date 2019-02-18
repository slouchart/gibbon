from .base import OneToMany, StreamProcessor
from ..exceptions import BaseBuildWarning


class SelectorHasTooManyTargets(BaseBuildWarning):
    ...


class Selector(OneToMany, StreamProcessor):
    # TODO: add doc string
    def __init__(self, *args, conditions, **kwargs):
        self.conditions = conditions
        out_ports = len(self.conditions)
        super().__init__(*args, out_ports, **kwargs)

    def add_target(self, target_transfo):
        super().add_target(target_transfo)
        if len(self.out_ports) > len(self.conditions)+1:
            raise SelectorHasTooManyTargets(f'Selector {self.name} has too many targets')

    async def process_rows(self):
        while True:
            row = await self.get_row()
            if self.eof_signal(row):
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
