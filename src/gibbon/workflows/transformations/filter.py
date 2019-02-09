from .base import OneToMany


class Filter(OneToMany):
    # TODO: add doc string
    def __init__(self, name, condition=lambda r: True, out_ports=1):
        super().__init__(name, out_ports)
        self.condition = condition

    def get_async_job(self):
        async def job():
            while True:
                row = await self.in_queues[0].get()
                if row is None:
                    for q in self.out_queues:
                        await q.put(row)
                    break

                if self.condition(row):
                    for q in self.out_queues:
                        await q.put(row)
        return job
