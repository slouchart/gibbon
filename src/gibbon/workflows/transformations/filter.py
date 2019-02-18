from .base import OneToMany, StreamProcessor


class Filter(OneToMany, StreamProcessor):
    # TODO: add doc string
    def __init__(self, name, out_ports=1, condition=lambda r: True, **kwargs):
        super().__init__(name, out_ports, **kwargs)
        self.condition = condition

    def can_emit_row(self, row):
        return self.condition(row)

