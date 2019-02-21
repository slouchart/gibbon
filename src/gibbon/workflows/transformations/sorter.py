from .base import *


class Sorter(UpStreamable, MonoDownStreamable, Transformation):
    """Take its input in a buffer, sort it and output its content as a downstream"""
    def __init__(self, name, key, *args, reverse=False, **kwargs):
        self.key = key
        self.reverse = reverse
        super().__init__(name, *args, **kwargs)

    async def process_rows(self):
        buffer = []
        while True:
            row = await self.get_row()
            if self.may_stop_process(row):
                break
            else:
                buffer.append(row)

        for row in sorted(buffer, key=self.key, reverse=self.reverse):
            await self.emit_row(row)

        await self.emit_eof()


