from abc import abstractmethod


class AsyncReaderInterface:

    def __init__(self, *args, **kwargs):
        ...

    @abstractmethod
    def __aiter__(self):
        return self

    @abstractmethod
    async def __anext__(self):
        raise StopAsyncIteration

    @abstractmethod
    async def __aenter__(self):
        return self

    @abstractmethod
    async def __aexit__(self, *args):
        pass


class AsyncWriterInterface:

    def __init__(self, *args, **kwargs):
        ...

    @abstractmethod
    async def put(self, data):
        ...

    @abstractmethod
    async def __aenter__(self):
        return self

    @abstractmethod
    async def __aexit__(self, *args):
        pass
