from sys import stdout


class Sequence:
    def __init__(self, data=()):
        self._data = list(data)
        self._iter = iter(data)

    '''Design note: the following implementation of the async iterator interface 
    only works with Python 3.7+ see Issue bpo-31709: Drop support for asynchronous __aiter__
    at https://github.com/python/cpython/pull/3903/files'''
    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return next(self._iter)
        except StopIteration:
            raise StopAsyncIteration

    def __enter__(self):
        return self

    def __exit__(self, *args):
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
