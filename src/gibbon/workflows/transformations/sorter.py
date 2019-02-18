from .base import OneToMany, StreamProcessor


class Sorter(OneToMany, StreamProcessor):
    """Take its input in a buffer, sort it and output its content as a downstream"""
    def __init__(self, *args, key, reverse=False, **kwargs):
        super().__init__(*args, **kwargs)
        self.key = key
        self.reverse = reverse

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


