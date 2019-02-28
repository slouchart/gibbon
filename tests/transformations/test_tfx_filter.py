import unittest
import logging

from src import gibbon


class TestFilter(unittest.TestCase):

    @staticmethod
    def only_positive(row):
        return row[0] > 0

    def setUp(self):
        self.data = [(0,), (1,), (-1,)]
        self.results = []
        self.wk_all_rows = gibbon.Workflow('all_rows')
        self.wk_only_pos = gibbon.Workflow('only_positive')

        with self.wk_all_rows.start_build():
            self.wk_all_rows.add_source('src')
            self.wk_all_rows.add_transformation('filter', gibbon.Filter, sources='src')
            self.wk_all_rows.add_target('tgt', source='filter')

        with self.wk_only_pos.start_build():
            self.wk_only_pos.add_source('src')
            self.wk_only_pos.add_transformation('filter', gibbon.Filter, sources='src', condition=self.only_positive)
            self.wk_only_pos.add_target('tgt', source='filter')

        self.cfg = gibbon.Configuration()

    def testAllRows(self):
        self.wk_only_pos.validate(verbose=True)
        self.cfg.configure('src').using(source=gibbon.SequenceWrapper, iterable=self.data)
        self.cfg.configure('tgt').using(target=gibbon.SequenceWrapper, container=self.results)

        executor = gibbon.get_async_executor(shutdown=True)
        executor.run_workflow(self.wk_all_rows.name, self.wk_all_rows, self.cfg)

        self.assertSequenceEqual(self.results, self.data)

    def testOnlyPositive(self):
        self.wk_only_pos.validate(verbose=True)
        self.results = []

        self.cfg.configure('src').using(source=gibbon.SequenceWrapper, iterable=self.data)
        self.cfg.configure('tgt').using(target=gibbon.SequenceWrapper, container=self.results)

        executor = gibbon.get_async_executor(shutdown=True)
        executor.run_workflow(self.wk_only_pos.name, self.wk_only_pos, self.cfg)

        self.assertSequenceEqual(self.results, [(1,)])


def len_over_three(row):
    return len(row[0]) > 3


class TestFilterString(unittest.TestCase):

    def setUp(self):
        self.data = ['22', '333', '4444']
        self.wk = gibbon.Workflow('all_rows')

        with self.wk.start_build():
            self.wk.add_source('src')
            self.wk.add_transformation('filter', gibbon.Filter, sources='src', condition=len_over_three)
            self.wk.add_target('tgt', source='filter')

        self.cfg = gibbon.Configuration()

    def testLenOverThree(self):
        self.wk.validate(verbose=True)
        sink = []
        self.cfg.configure('src').using(source=gibbon.SequenceWrapper, iterable=zip(self.data))
        self.cfg.configure('tgt').using(target=gibbon.SequenceWrapper, container=sink)

        executor = gibbon.get_async_executor(shutdown=True)
        executor.run_workflow(self.wk.name, self.wk, self.cfg)

        self.assertSequenceEqual(sink, [('4444',)])


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.DEBUG)
    unittest.main()
