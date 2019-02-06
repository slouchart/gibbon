from .base import BaseExecutor


class SyncExecutor(BaseExecutor):

    def run(self, name, verbose=False):
        self.verbose = verbose

        def job():
            with self._nursery_factory() as task_group:
                for func, infos in self._jobs.items():
                    self.exec_msg(f'job {name}, starting transformation {infos[0]} ({infos[1]})')
                    task_group.spawn(func)

        self.exec_msg(f'Start synchronous job execution for mapping {name}')
        try:
            with self._kernel as kern:
                kern.run(job)
        except BaseException as e:
            raise e
        self.exec_msg(f'Complete synchronous job execution for mapping {name}')

    def _create_job_from(self, transformation):
        return transformation.get_sync_job()
