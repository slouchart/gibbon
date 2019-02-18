from .base import OneToMany, StreamProcessor


def row_count():
    def _count(r, c):
        return (c+1, )
    return _count


def simple_sum(field_index):
    def _sum_on(r, s):
        return (s+r[field_index],)
    return _sum_on


class Aggregator(OneToMany, StreamProcessor):
    '''This transformation accepts a stream of rows and compute some accumulators through a function
    the parameter :key accepts a row and return row that is a subset of the fields of the row
    the parameter :accumulator is a callable that accepts an input row and somme accumulator. It must output a row containing
    the values of each of the accumulator.
    the parameter :initializer is used to set the starting value of the accumulators as a tuple'''
    def __init__(self, *args, key, accumulator, initializer, **kwargs):
        super().__init__(*args, **kwargs)
        self.key = key
        self.func = accumulator
        self.initializer = initializer
        self.buffer = None

    def process_row(self, row):
        if self.key(row) in self.buffer:
            self.buffer[self.key(row)] = self.func(row, *self.buffer[self.key(row)])
        else:
            self.buffer[self.key(row)] = self.func(row, *self.initializer)

    async def process_rows(self):
        self.buffer = dict()
        while True:
            row = await self.get_row()
            if self.eof_signal(row):
                break

            self.process_row(row)

        for key, value in self.buffer.items():
            row = (*key, value[0],)
            await self.emit_row(row)

        await self.emit_eof()




