from .base import BaseExecutor
import asyncio
import logging


def get_async_executor(loop=None, shutdown=False):
    if loop is not None and shutdown:
        logging.warning(f'The provided event loop will be shut down')

    if loop is None:
        loop = asyncio.new_event_loop()

    return AsyncExecutor(asyncio.Queue, loop=loop, shutdown=shutdown)


class AsyncExecutor(BaseExecutor):

    def __init__(self, queue_factory, loop, shutdown=False):
        super().__init__(queue_factory)
        self._tasks = []
        self.loop = loop
        self.shutdown = shutdown
        assert self.loop is not None

    def create_queue(self):
        return self._queue_factory(loop=self.loop)

    async def schedule(self, name):

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

    def run(self, name):

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

    def create_job_from(self, transformation):
        coro_func = transformation.get_async_job()
        self._jobs[coro_func] = (transformation.name, type(transformation).__name__)
