from .base import *


class Filter(UpStreamable, MonoDownStreamable, Transformation):
    # TODO: add doc string
    def __init__(self, *args, condition=lambda r: True, **kwargs):
        super().__init__(*args, **kwargs)
        self.condition = condition

    def can_emit_row(self, row):
        return self.condition(row)

