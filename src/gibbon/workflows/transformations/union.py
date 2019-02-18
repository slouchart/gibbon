from .base import ManyToMany, StreamProcessor


class Union(ManyToMany, StreamProcessor):
    def __init__(self, *args, in_ports=2, out_ports=1, **kwargs):
        super().__init__(*args, in_ports=in_ports, out_ports=out_ports, **kwargs)
        self.eof_signals = None

    def may_stop_process(self, row=None):
        return all(self.eof_signals.values())

    async def process_rows(self):
        self.eof_signals = {q: False for q in self.in_queues}

        while True:
            if self.may_stop_process():
                break

            for iq in self.in_queues:
                if not self.eof_signals[iq]:
                    row = await iq.get()
                    if row is None:
                        self.eof_signals[iq] = True
                    else:
                        await self.emit_row(row)

        await self.emit_eof()

