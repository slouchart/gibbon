import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor as ThreadPoolExecutor
from typing import *

from .base import BaseExecutor
from ..workflows.transformations import StreamProcessor


def get_async_executor(loop: asyncio.AbstractEventLoop = None, shutdown: bool = False) -> BaseExecutor:
    if loop is not None and shutdown:
        logging.warning(f'The provided event loop will be shut down')

    if loop is None:
        loop = asyncio.new_event_loop()

    return AsyncExecutor(asyncio.Queue, loop=loop, shutdown=shutdown)


class AsyncExecutor(BaseExecutor):

    def __init__(self, queue_factory: Union[Type[asyncio.Queue], Callable[[asyncio.AbstractEventLoop], asyncio.Queue]],
                 loop: asyncio.AbstractEventLoop,
                 executor: ThreadPoolExecutor = None, shutdown: bool = True) -> None:
        super().__init__(queue_factory)
        self._tasks = []
        self.loop = loop
        self.shutdown = shutdown
        self.executor = executor
        assert self.loop is not None
        if self.executor is not None:
            self.loop.set_default_executor(self.executor)

    def create_queue(self) -> asyncio.Queue:
        return self._queue_factory(loop=self.loop)

    async def schedule(self, name: str, *args, **kwargs) -> bool:

        for coro_func, infos in self._jobs.items():
            logging.info(f'job {name}, starting transformation {infos[0]} ({infos[1]})')
            coro = coro_func()
            self._tasks.append(coro)

        done, pending = await asyncio.wait(self._tasks, loop=self.loop, return_when=asyncio.FIRST_EXCEPTION)

        exec_ok = True
        for future in done:
            if future.exception():
                logging.error(f'{future.exception()}')
                exec_ok = False
                break
            elif future.result():
                logging.info(f'Got result {future.result()}')

        for future in pending:
            future.cancel()

        return exec_ok

    def run(self, name: str, *args, **kwargs) -> None:

        logging.info(f'Start asynchronous job execution for workflow {name}')
        exec_ok = self.loop.run_until_complete(self.schedule(name))

        if exec_ok:
            status = 'SUCCESS'
        else:
            status = 'FAILURE'
        logging.info(f'Complete asynchronous job execution for workflow {name}, status {status}')

        if self.shutdown:
            self.loop.stop()
            self.loop.close()

    def create_job_from(self, transformation: StreamProcessor) -> None:
        coro_func = transformation.get_async_job()
        name = getattr(transformation, 'name', '<no name>')
        self._jobs[coro_func] = (name, type(transformation).__name__)

    def complete_runtime_configuration(self, transformation: StreamProcessor) -> None:
        def dummy(*_):
            ...
        callback = getattr(transformation, 'configure', dummy)
        callback(loop=self.loop, executor=self.executor)
