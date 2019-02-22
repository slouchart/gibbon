from ..mixins import *


class Sorter(UpStreamable, MonoDownStreamable, Bufferized, Transformation):
    """Take its input in a buffer, sort it and output its content as a downstream"""
    def __init__(self, name: str, key: Callable[[Tuple, Tuple], bool], *args: Any,
                 reverse: bool = False, **kwargs: Any):
        self.key = key
        self.reverse = reverse
        super().__init__(name, *args, **kwargs)

    def process_buffer(self):
        self.buffer = sorted(self.buffer, key=self.key, reverse=self.reverse)



