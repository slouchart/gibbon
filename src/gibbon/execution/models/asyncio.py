from ..asyncexe import AsyncExecutor
import asyncio


class AsyncIOKernel:
    def __init__(self, loop):
        self.loop = loop

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def run(self, coro):
        self.loop.run_until_complete(coro)


class AsyncIONursery:

    def __init__(self, loop):
        self.loop = loop
        self.tasks = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return asyncio.gather(*self.tasks, loop=self.loop)

    async def spawn(self, coro):
        self.tasks.append(self.loop.create_task(coro))


def _nursery_factory(loop):
    def _create_nursery():
        return AsyncIONursery(loop)
    return _create_nursery


def get_async_executor():
    loop = asyncio.get_event_loop()
    return AsyncExecutor(asyncio.Queue, _nursery_factory(loop), AsyncIOKernel(loop))
