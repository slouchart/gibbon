from ..syncexe import SyncExecutor
import threading
from queue import Queue


class ThreadNursery:
    def __init__(self):
        self._threads = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        for thread in self._threads:
            thread.start()

        for thread in self._threads:
            thread.join()

    def spawn(self, func):
        thread = threading.Thread(target=func)
        self._threads.append(thread)


class MainThread:
    def __init__(self):
        self.thread = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def run(self, target):
        self.thread = threading.Thread(target=target)
        self.thread.start()
        self.thread.join()


def get_sync_executor():
    return SyncExecutor(Queue, ThreadNursery, MainThread())
