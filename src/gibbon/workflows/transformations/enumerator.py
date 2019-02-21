from .base import *


class Enumerator(UpStreamable, MonoDownStreamable, Transformation):

    def __init__(self, *args, start_with=0, reset_after=-1, **kwargs):
        super().__init__(*args, **kwargs)
        self.start_with = start_with
        self._index = self.start_with
        self.reset_after = reset_after

    def process_row(self, row):
        row = tuple([self._index] + [f for f in row])

        self._index += 1
        if self._index > self.reset_after > 0:
            self._index = self.start_with

        return row

    async def process_rows(self):
        self._index = self.start_with
        await super().process_rows()

