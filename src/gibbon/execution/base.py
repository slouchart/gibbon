class BaseExecutor:

    def __init__(self, queue_factory, nursery_factory, kernel):
        self._jobs = dict()
        self._queue_factory = queue_factory
        self._nursery_factory = nursery_factory
        self._kernel = kernel
        self.verbose = False

    def create_queue(self):
        return self._queue_factory()

    def set_queues(self, source, target):
        queue = self.create_queue()
        source.share_queue_with_target(target, queue)

    def create_job_from(self, transformation):
        func = self._create_job_from(transformation)
        self._jobs[func] = (transformation.name, type(transformation).__name__)

    def exec_msg(self, msg):
        if self.verbose:
            print(msg)

    def run(self, name, verbose=False):
        raise NotImplementedError

    def _create_job_from(self, transformation):
        raise NotImplementedError
