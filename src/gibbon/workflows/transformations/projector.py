from .base import *


class Concat(UpStreamable, MultiDownStreamable, Transformation):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
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


class Split(UpStreamable, MonoDownStreamable, Transformation):
    def __init__(self, name: str, func: Callable[[Tuple], Collection[Tuple]], *args: Any, **kwargs: Any):
        self.func = func
        super().__init__(name, *args, **kwargs)

    async def process_rows(self):
        while True:
            row = await self.get_row()

            if self.may_stop_process(row):
                break

            rows = self.func(row)
            for row, oq in zip(rows, self.out_queues):
                await oq.put(row)

        await self.emit_eof()



