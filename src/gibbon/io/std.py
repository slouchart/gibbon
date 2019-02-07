from sys import stdout


class Sequence:
    def __init__(self, data=()):
        self._data = list(data)

    def __iter__(self):
        return iter(self._data)

    def __aiter__(self):
        self._iter = iter(self._data)
        return self

    async def __anext__(self):
        try:
            item = self._iter.__next__()
            return item
        except StopIteration:
            raise StopAsyncIteration

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass


class StdOut:
    def __init__(self):
        self._output = stdout

    def send(self, data):
        print(data, file=self._output)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass
