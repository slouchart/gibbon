from .base import OneToMany, StreamProcessor


class Sorter(OneToMany, StreamProcessor):
    # TODO: add doc string
    def __init__(self, *args, key, reverse=False, **kwargs):
        super().__init__(*args, **kwargs)
        self.key = key
        self.reverse = reverse

    async def process_rows(self):
        buffer = []
        while True:
            row = await self.get_row()
            if self.eof_signal(row):
                break
            else:
                buffer.append(row)

        for row in sorted(buffer, key=self.key, reverse=self.reverse):
            await self.emit_row(row)

        await self.emit_eof()


