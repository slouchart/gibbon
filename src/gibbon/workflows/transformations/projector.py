from .base import ManyToMany
from .base import OneToMany


class Concat(ManyToMany):
    def __init__(self, name, in_ports=2, out_ports=1):
        super().__init__(name, in_ports, out_ports)
        self.buffers = dict()

    def get_async_job(self):
        async def job():
            eof_signals = {q: False for q in self.in_queues}
            while True:
                buffer = []
                for iq in [iq for iq, sig in eof_signals.items() if not sig]:
                    row = await iq.get()
                    if row is None:
                        eof_signals[iq] = True
                    else:
                        buffer.append(row)

                if len(buffer):
                    # we can emit
                    concat_row = []
                    for row in buffer:
                        concat_row += list(row)
                    concat_row = tuple(concat_row)
                    for oq in self.out_queues:
                        await oq.put(concat_row)
                else:
                    for oq in self.out_queues:
                        await oq.put(None)
                    break

        return job


class Split(OneToMany):
    def __init__(self, name, func, out_ports=2):
        super().__init__(name, out_ports)
        self.func = func

    def get_async_job(self):
        async def job():
            while True:
                row = await self.in_queues[0].get()

                if row is None:
                    for oq in self.out_queues:
                        await oq.put(None)
                    break
                else:
                    rows = self.func(row)
                    for row, oq in zip(rows, self.out_queues):
                        await oq.put(row)

        return job


