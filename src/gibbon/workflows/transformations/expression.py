from .base import OneToMany, StreamProcessor


class Expression(OneToMany, StreamProcessor):
    """Stream transformation that applies a function to each row
    That function is provided though the parameter :func at initialization
    and must accept a tuple and return a tuple"""
    def __init__(self, *args, func=lambda r: r, **kwargs):
        super().__init__(*args, **kwargs)
        self.func = func

    def process_row(self, row):
        return self.func(row)

