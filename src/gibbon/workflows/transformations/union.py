from .base import ManyToMany


class Union(ManyToMany):
    def __init__(self, name, in_ports=2, out_ports=1):
        super().__init__(name, in_ports, out_ports)

    def get_async_job(self):
        eof_signals = {q: False for q in self.in_queues}

        async def job():
            while True:
                if all(eof_signals.values()):
                    for oq in self.out_queues:
                        await oq.put(None)
                    break

                for iq in self.in_queues:
                    if not eof_signals[iq]:
                        row = await iq.get()
                        if row is None:
                            eof_signals[iq] = True
                        else:
                            for oq in self.out_queues:
                                await oq.put(row)

        return job
