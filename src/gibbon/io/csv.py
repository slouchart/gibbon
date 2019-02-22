import csv

from .base import AsyncReaderInterface, AsyncWriterInterface


def naive_tuple_maker(it):
    return tuple(it)


class CSVSourceFile(AsyncReaderInterface):
    def __init__(self, filename, loop, *, executor=None, tuple_maker=naive_tuple_maker, **fmtopts):
        self._filename = filename
        self._fmt_options = fmtopts
        self._reader = None
        self._file_obj = None
        self._loop = loop
        self._executor = executor
        self._to_tuple = tuple_maker
        super().__init__()

    def __aiter__(self):
        return self

    async def __anext__(self):
        def _wrap_next(sync_iter):
            try:
                row = next(sync_iter)
                return self._to_tuple(row)
            except StopIteration:
                raise StopAsyncIteration

        return await self._loop.run_in_executor(self._executor, _wrap_next, self._reader)

    async def __aenter__(self):
        try:
            self._file_obj = await self._loop.run_in_executor(self._executor, open, self._filename, 'r')
            self._reader = await self._loop.run_in_executor(self._executor, csv.reader,
                                                            self._file_obj, **self._fmt_options)
            return self
        except BaseException as exc:
            await self.__aexit__(type(exc), str(exc), exc.__traceback__)

    async def __aexit__(self, exc_type, exc_value, exc_traceback):
        self._reader = None
        if self._file_obj is not None:
            try:
                await self._loop.run_in_executor(self._executor, self._file_obj.close)
            finally:
                pass
        return exc_type is not None


class CSVTargetFile(AsyncWriterInterface):
    def __init__(self, filename, loop, *, executor=None, **fmtopts):
        self._filename = filename
        self._fmt_options = fmtopts
        self._writer = None
        self._file_obj = None
        self._loop = loop
        self._executor = executor
        super().__init__()

    async def put(self, data):
        await self._loop.run_in_executor(self._executor, self._writer.writerow, data)

    async def __aenter__(self):
        try:
            self._file_obj = await self._loop.run_in_executor(self._executor, open, self._filename, 'w')
            self._writer = await self._loop.run_in_executor(self._executor, csv.writer, self._file_obj,
                                                            **self._fmt_options)
            return self
        except BaseException:
            await self._safe_close()
            raise

    async def __aexit__(self, exc_type, exc_value, exc_traceback):
        await self._safe_close()
        return exc_type is None

    async def _safe_close(self):
        self._writer = None
        if self._file_obj is not None:
            try:
                await self._loop.run_in_executor(self._executor, self._file_obj.close)
            finally:
                pass
