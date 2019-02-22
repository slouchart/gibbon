from ..mixins import *


class Expression(UpStreamable, MonoDownStreamable, Transformation):
    """Stream transformation that applies a function to each row
    That function is provided though the parameter :func at initialization
    and must accept a tuple and return a tuple"""
    def __init__(self, *args: Any, func: Callable[[Tuple], Tuple] = lambda r: r, **kwargs: Any):
        self.func = func
        super().__init__(*args, **kwargs)

    def process_row(self, row):
        return self.func(row)

