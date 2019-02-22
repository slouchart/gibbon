from abc import abstractmethod


class BaseExecutor:

    @abstractmethod
    def run_workflow(self, name, workflow, configuration):
        ...


