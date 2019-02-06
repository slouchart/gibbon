from .base import Transformation


class OneToMany(Transformation):
    def __init__(self, name, out_ports=1):
        super().__init__(name, 1, out_ports)

    def set_source(self, parent_transfo):
        assert self.in_ports[0] is None
        self.in_ports[0] = parent_transfo
        parent_transfo.add_target(self)

    def get_async_job(self):
        raise NotImplementedError

    def get_sync_job(self):
        raise NotImplementedError


class Expression(OneToMany):
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

    def get_sync_job(self):
        def job():
            while True:
                row = self.in_queues[0].get()
                if row is None:
                    for q in self.out_queues:
                        q.put(row)
                    break

                row = self.func(row)

                for q in self.out_queues:
                    q.put(row)
        return job


class Filter(OneToMany):
    def __init__(self, name, condition, out_ports=1):
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

    def get_sync_job(self):
        def job():
            while True:
                row = self.in_queues[0].get()
                if row is None:
                    for q in self.out_queues:
                        q.put(row)
                    break

                if self.condition(row):
                    for q in self.out_queues:
                        q.put(row)
        return job


class Sorter(OneToMany):
    def __init__(self, name, key, out_ports=1):
        super().__init__(name, out_ports)
        self.key = key

    def get_async_job(self):
        async def job():
            buffer = []
            while True:
                row = await self.in_queues[0].get()
                if row is None:
                    break
                else:
                    buffer.append(row)

            for row in sorted(buffer, key=self.key):
                for q in self.out_queues:
                    await q.put(row)

            for q in self.out_queues:
                await q.put(None)

        return job

    def get_sync_job(self):
        def job():
            buffer = []
            while True:
                row = self.in_queues[0].get()
                if row is None:
                    break
                else:
                    buffer.append(row)

            for row in sorted(buffer, key=self.key):
                for q in self.out_queues:
                    q.put(row)

            for q in self.out_queues:
                q.put(None)

        return job


class Selector(OneToMany):
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

    def get_sync_job(self):
        def job():
            while True:
                row = self.in_queues[0].get()
                if row is None:
                    for q in self.out_queues:
                        q.put(row)
                    break

                for (cond, oq) in zip(self.conditions, self.out_queues):
                    if cond(row):
                        oq.put(row)
        return job


class Aggregator(OneToMany):
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

    def get_sync_job(self):
        def job():
            buffer = dict()
            while True:
                row = self.in_queues[0].get()
                if row is None:
                    break

                if self.key(row) in buffer:
                    buffer[self.key(row)] = self.func(row, *buffer[self.key(row)])
                else:
                    buffer[self.key(row)] = self.func(row, *self.initializer)

            for key, value in buffer.items():
                for q in self.out_queues:
                    q.put((*key, value[0]))

            for q in self.out_queues:
                q.put(None)

        return job
