from .base import BaseExecutor


class AsyncExecutor(BaseExecutor):

    def run(self, name, verbose=False):
        self.verbose = verbose

        async def job():
            async with self._nursery_factory() as task_group:
                for coro, infos in self._jobs.items():
                    self.exec_msg(f'job {name}, starting transformation {infos[0]} ({infos[1]})')
                    await task_group.spawn(coro())

        self.exec_msg(f'Start asynchronous job execution for mapping {name}')
        try:
            with self._kernel as kern:
                kern.run(job())
        except BaseException as e:
            raise e
        self.exec_msg(f'Complete asynchronous job execution for mapping {name}')

    def _create_job_from(self, transformation):
        return transformation.get_async_job()
