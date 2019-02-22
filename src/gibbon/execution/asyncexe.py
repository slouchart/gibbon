import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor as ThreadPoolExecutor
from typing import *

from .base import BaseExecutor
from src.gibbon.utils.abstract import Visitor, VisitMode
from ..workflows.transformations import StreamProcessor


def get_async_executor(loop: asyncio.AbstractEventLoop = None, shutdown: bool = False) -> BaseExecutor:
    if loop is not None and shutdown:
        logging.warning(f'The provided event loop will be shut down')

    if loop is None:
        loop = asyncio.new_event_loop()

    return AsyncExecutor(asyncio.Queue, loop=loop, shutdown=shutdown)


class AsyncExecutor(BaseExecutor, Visitor):

    def __init__(self, queue_factory: Union[Type[asyncio.Queue], Callable[[asyncio.AbstractEventLoop], asyncio.Queue]],
                 loop: asyncio.AbstractEventLoop,
                 executor: ThreadPoolExecutor = None, shutdown: bool = True) -> None:
        super().__init__()
        self._jobs = dict()
        self._queue_factory = queue_factory
        self._tasks = []
        self.loop = loop
        self.shutdown = shutdown
        self.executor = executor
        self.can_traverse_nodes = True
        self.can_traverse_links = False
        assert self.loop is not None
        if self.executor is not None:
            self.loop.set_default_executor(self.executor)

    def create_queue(self) -> asyncio.Queue:
        return self._queue_factory(loop=self.loop)

    def set_queues(self, source, target):
        assert source is not target
        queue = self.create_queue()
        source.share_queue_with_target(target, queue)

    async def _schedule_jobs(self, name: str, *args, **kwargs) -> bool:

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

    def create_job_from(self, name, transformation: StreamProcessor) -> None:
        coro_func = transformation.get_async_job()
        self._jobs[coro_func] = (name, type(transformation).__name__)

    def complete_runtime_configuration(self, name, transformation: StreamProcessor) -> None:
        def dummy(*_):
            ...
        callback = getattr(transformation, 'configure', dummy)
        callback(loop=self.loop, executor=self.executor)

    def run_workflow(self, name, workflow, configuration):

        if not workflow.is_valid:
            logging.error(f"{name}: invalid workflow cannot be run")
        else:
            workflow.accept_visitor(configuration, VisitMode.elements_only)
            workflow.accept_visitor(self, VisitMode.links_only)
            workflow.accept_visitor(self, VisitMode.elements_only)

            logging.info(f'Start asynchronous job execution for workflow {name}')
            exec_ok = self.loop.run_until_complete(self._schedule_jobs(name))

            if exec_ok:
                status = 'SUCCESS'
            else:
                status = 'FAILURE'
            logging.info(f'Complete asynchronous job execution for workflow {name}, status {status}')

        if self.shutdown:
            self.loop.stop()
            self.loop.close()

    def visit_link(self, elem1, elem2):
        self.set_queues(source=elem1, target=elem2)

    def visit_element(self, name, element):
        self.complete_runtime_configuration(name, element)
        self.create_job_from(name, element)

