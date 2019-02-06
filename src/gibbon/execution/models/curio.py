from ..asyncexe import AsyncExecutor
import curio


def get_async_executor():
    return AsyncExecutor(curio.Queue, curio.TaskGroup, curio.Kernel())
