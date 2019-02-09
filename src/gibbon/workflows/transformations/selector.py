from .base import OneToMany
from ..exceptions import BaseBuildWarning


class SelectorHasTooManyTargets(BaseBuildWarning):
    ...


class Selector(OneToMany):
    # TODO: add doc string
    def __init__(self, name, conditions):
        self.conditions = conditions
        out_ports = len(self.conditions)
        super().__init__(name, out_ports)

    def add_target(self, target_transfo):
        super().add_target(target_transfo)
        if len(self.out_ports) > len(self.conditions)+1:
            raise SelectorHasTooManyTargets(f'Selector {self.name} has too many targets')

    def get_async_job(self):
        async def job():
            while True:
                row = await self.in_queues[0].get()
                if row is None:
                    # EOF propagated to default queue anyway
                    for q in self.out_queues:
                        await q.put(row)
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

        return job
