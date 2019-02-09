from .base import OneToMany


class Expression(OneToMany):
    # TODO: add doc string
    def __init__(self, name, out_ports=1, func=lambda r: r):
        super().__init__(name, out_ports)
        self.func = func

    def get_async_job(self):
        async def job():
            while True:
                row = await self.in_queues[0].get()
                if row is None:
                    for q in self.out_queues:
                        await q.put(row)
                    break

                row = self.func(row)

                for q in self.out_queues:
                    await q.put(row)
        return job
