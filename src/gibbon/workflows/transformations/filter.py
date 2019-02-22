from ..mixins import *


class Filter(UpStreamable, MonoDownStreamable, Transformation):
    """Stream transformation that applies a gate function to each row
        That function is provided though the parameter :func at initialization
        and must accept a tuple and return True or False
        On True, the row is emitted downstream otherwise it is not"""
    def __init__(self, *args: Any, condition: Callable[[Tuple], bool] = lambda r: True, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self.condition = condition

    def can_emit_row(self, row):
        return self.condition(row)

