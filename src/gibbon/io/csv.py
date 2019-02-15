import csv

from .base import AsyncReaderInterface


# TODO: make it work using aiofiles and csvreader


def naive_tuple_maker(it):
    return tuple(it)


class CSVSourceFile(AsyncReaderInterface):
    def __init__(self, filename, loop, tuple_maker=naive_tuple_maker, **fmtopts):
        self._filename = filename
        self._fmt_options = fmtopts
        self._reader = None
        self._file_obj = None
        self._loop = loop
        self._to_tuple = tuple_maker

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            row = await self._loop.run_in_executor(None, self._reader.__next__)
            return self._to_tuple(row)
        except StopIteration:
            raise StopAsyncIteration

    async def __aenter__(self):
        self._file_obj = await self._loop.run_in_executor(None, open, self._filename, 'r')
        self._reader = await self._loop.run_in_executor(None, csv.reader, self._file_obj, **self._fmt_options)
        return self

    async def __aexit__(self, *args):
        self._reader = None
        await self._loop.run_in_executor(None, self._file_obj.close)


