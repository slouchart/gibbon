from .base import Transformation


class ManyToMany(Transformation):
    def __init__(self, name, in_ports=1, out_ports=1):
        super().__init__(name, in_ports, out_ports)

    def _extend_input_ports(self):
        n_port = len(self.in_ports)
        self.in_ports[n_port] = None
        return n_port

    def _get_next_available_input_port(self):
        n_port = None
        for k, v in self.in_ports.items():
            n_port = k
            if v is None:
                break
            else:
                n_port = None
        else:
            if n_port is None:
                n_port = self._extend_input_ports()

        return n_port

    def set_source(self, parent_transfo):
        # expect a tuple of sources transformations
        for source in parent_transfo:
            n_port = self._get_next_available_input_port()
            self.in_ports[n_port] = source
            source.add_target(self)

    def get_async_job(self):
        raise NotImplementedError

    def get_sync_job(self):
        raise NotImplementedError


class Union(ManyToMany):
    def __init__(self, name, in_ports=2, out_ports=1):
        super().__init__(name, in_ports, out_ports)

    def get_async_job(self):
        async def job():
            input_queues = [q for q in self.in_queues]
            while True:

                if len(input_queues) == 0:
                    for q in self.out_queues:
                        await q.put(None)
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

    def get_sync_job(self):
        def job():
            input_queues = [q for q in self.in_queues]
            while True:

                if len(input_queues) == 0:
                    for q in self.out_queues:
                        q.put(None)
                    break

                for iq in input_queues:
                    if iq.empty():
                        continue
                    row = iq.get()

                    if row is None:
                        input_queues.remove(iq)
                    else:
                        for oq in self.out_queues:
                            oq.put(row)
        return job

