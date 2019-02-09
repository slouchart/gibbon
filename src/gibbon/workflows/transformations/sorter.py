from .base import OneToMany


class Sorter(OneToMany):
    # TODO: add doc string
    def __init__(self, name, key, reverse=False, out_ports=1):
        super().__init__(name, out_ports)
        self.key = key
        self.reverse = reverse

    def get_async_job(self):
        async def job():
            buffer = []
            while True:
                row = await self.in_queues[0].get()
                if row is None:
                    break
                else:
                    buffer.append(row)

            for row in sorted(buffer, key=self.key, reverse=self.reverse):
                for q in self.out_queues:
                    await q.put(row)

            for q in self.out_queues:
                await q.put(None)

        return job
