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


class Selector(OneToMany):
    # TODO: add doc string
    def __init__(self, name, conditions, out_ports=1):
        super().__init__(name, out_ports)
        self.conditions = conditions

    def get_async_job(self):
        async def job():
            while True:
                row = await self.in_queues[0].get()
                if row is None:
                    for q in self.out_queues:
                        await q.put(row)
                    break

                for (cond, oq) in zip(self.conditions, self.out_queues):
                    if cond(row):
                        await oq.put(row)
        return job


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


