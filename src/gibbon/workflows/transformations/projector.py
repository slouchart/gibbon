from .base import ManyToMany
from .base import OneToMany
from .base import StreamProcessor


class Concat(ManyToMany, StreamProcessor):
    def __init__(self, *args, in_ports=2, out_ports=1, **kwargs):
        super().__init__(*args, in_ports=in_ports, out_ports=out_ports, **kwargs)
        self.buffers = dict()

    async def process_rows(self):
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


class Split(OneToMany, StreamProcessor):
    def __init__(self, *args, func, **kwargs):
        super().__init__(*args, out_ports=2, **kwargs)
        self.func = func

    async def process_rows(self):
        while True:
            row = await self.get_row()

            if self.may_stop_process(row):
                break

            rows = self.func(row)
            for row, oq in zip(rows, self.out_queues):
                await oq.put(row)

        await self.emit_eof()



