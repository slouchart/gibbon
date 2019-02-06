from abc import abstractmethod


class BaseExecutor:

    def __init__(self, queue_factory):
        self._jobs = dict()
        self._queue_factory = queue_factory
        self.verbose = False

    def create_queue(self):
        return self._queue_factory()

    def set_queues(self, source, target):
        queue = self.create_queue()
        source.share_queue_with_target(target, queue)

    @abstractmethod
    def create_job_from(self, transformation):
        raise NotImplementedError

    @abstractmethod
    def run(self, name, verbose=False):
        raise NotImplementedError


