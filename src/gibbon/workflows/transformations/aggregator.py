from .base import OneToMany


def row_count():
    def _count(r, c):
        return (c+1, )
    return _count


def simple_sum(field_index):
    def _sum_on(r, s):
        return (s+r[field_index],)
    return _sum_on


class Aggregator(OneToMany):
    # TODO: add doc string
    def __init__(self, name, key, func, initializer=(0,), out_ports=1):
        super().__init__(name, out_ports)
        self.key = key
        self.func = func
        self.initializer = initializer

    def get_async_job(self):
        async def job():
            buffer = dict()
            while True:
                row = await self.in_queues[0].get()
                if row is None:
                    break

                if self.key(row) in buffer:
                    buffer[self.key(row)] = self.func(row, *buffer[self.key(row)])
                else:
                    buffer[self.key(row)] = self.func(row, *self.initializer)

            for key, value in buffer.items():
                for q in self.out_queues:
                    await q.put((*key, value[0]))

            for q in self.out_queues:
                await q.put(None)

        return job


