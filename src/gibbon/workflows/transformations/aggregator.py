from .base import OneToMany


def row_count():
    def _count(r, c):
        return (c+1, )
    return _count


def simple_sum(field_index):
    def _sum_on(r, s):
        return (s+r[field_index],)
    return _sum_on


class Aggregator(OneToMany):
    '''This transformation accepts a stream of rows and compute some accumulators through a function
    the parameter :key accepts a row and return row that is a subset of the fields of the row
    the parameter :accumulator is a callable that accepts an input row and somme accumulator. It must output a row containing
    the values of each of the accumulator.
    the parameter :initializer is used to set the starting value of the accumulators as a tuple'''
    def __init__(self, name, key, accumulator, initializer, out_ports=1):
        super().__init__(name, out_ports)
        self.key = key
        self.func = accumulator
        self.initializer = initializer

    def get_async_job(self):
        async def job():
            buffer = dict()
            while True:
                row = await self.in_queues[0].get()
                if row is None:
                    break

                if self.key(row) in buffer:
                    buffer[self.key(row)] = self.func(row, *buffer[self.key(row)])
                else:
                    buffer[self.key(row)] = self.func(row, *self.initializer)

            for key, value in buffer.items():
                for q in self.out_queues:
                    await q.put((*key, value[0]))

            for q in self.out_queues:
                await q.put(None)

        return job


