from abc import abstractmethod


class AsyncReaderInterface:

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
    @abstractmethod
    async def send(self, data):
        ...

    @abstractmethod
    async def __aenter__(self):
        return self

    @abstractmethod
    async def __aexit__(self, *args):
        pass
