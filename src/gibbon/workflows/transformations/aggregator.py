from ..mixins import *


def row_count() -> Callable[[Tuple, int], Tuple[int]]:
    def inner_count(r: Tuple, c: int):
        return c + 1,
    return inner_count


def simple_sum(field_index: int) -> Callable[[Tuple, float], Tuple[float]]:
    def inner_sum(r: Tuple, s: float):
        return s + r[field_index],
    return inner_sum


class Aggregator(UpStreamable, MonoDownStreamable, Bufferized, Transformation):
    """This transformation accepts a stream of rows and compute some accumulators through a function
    the parameter :key accepts a row and return row that is a subset of the fields of the row.
    the parameter :accumulator is a callable that accepts an input row and somme accumulator. It must output a
    row containing the values of each of the accumulator.
    the parameter :initializer is used to set the starting value of the accumulators as a tuple"""

    def __init__(self, name: str, key: Callable[[Tuple], Tuple],
                 accumulator: Callable, initializer: Tuple, *args: Any, **kwargs: Any) -> None:
        self.key = key
        self.func = accumulator
        self.initializer = initializer
        self.dict = dict()
        super().__init__(name, *args, **kwargs)

    def process_row(self, row):
        if self.key(row) in self.dict:
            self.dict[self.key(row)] = self.func(row, *self.dict[self.key(row)])
        else:
            self.dict[self.key(row)] = self.func(row, *self.initializer)

    def process_buffer(self):
        for key, value in self.dict.items():
            row = (*key, value[0],)
            self.buffer.append(row)
