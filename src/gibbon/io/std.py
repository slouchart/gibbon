from .base import AsyncReaderInterface, AsyncWriterInterface


class SequenceWrapper(AsyncReaderInterface, AsyncWriterInterface):
    def __init__(self, iterable=(), container=None):
        self._iter = iter(iterable)
        self._container = container

    def __aiter__(self):
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

    async def send(self, data):
        if self._container is not None:
            self._container.append(data)


class StdOut(AsyncWriterInterface):
    def __init__(self, stdout):
        self._output = stdout

    async def send(self, data):
        print(data, file=self._output)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass

