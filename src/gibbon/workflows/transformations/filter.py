from .base import *


class Filter(UpStreamable, MonoDownStreamable, Transformation):
    # TODO: add doc string
    def __init__(self, *args: Any, condition: Callable[[Tuple], bool] = lambda r: True, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self.condition = condition

    def can_emit_row(self, row):
        return self.condition(row)

