from .base import ManyToMany


class Union(ManyToMany):
    def __init__(self, name, in_ports=2, out_ports=1):
        super().__init__(name, in_ports, out_ports)

    def get_async_job(self):
        async def job():
            input_queues = [q for q in self.in_queues]
            while True:

                if len(input_queues) == 0:
                    for oq in self.out_queues:
                        await oq.put(None)
                    break

                for iq in input_queues:
                    if iq.empty():
                        continue

                    row = await iq.get()

                    if row is None:
                        input_queues.remove(iq)
                    else:
                        for oq in self.out_queues:
                            await oq.put(row)
        return job