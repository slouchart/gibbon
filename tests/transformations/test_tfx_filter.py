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
        self.wk_all_rows.add_source('src')
        self.wk_all_rows.add_transformation('filter', gibbon.Filter, source='src')
        self.wk_all_rows.add_target('tgt', source='filter')

        self.wk_only_pos = gibbon.Workflow('only_positive')
        self.wk_only_pos.add_source('src')
        self.wk_only_pos.add_transformation('filter', gibbon.Filter, source='src', condition=self.only_positive)
        self.wk_only_pos.add_target('tgt', source='filter')

        self.cfg = gibbon.Configuration()

    def testAllRows(self):
        self.wk_only_pos.validate(verbose=True)
        self.cfg.add_configuration('src', source=gibbon.SequenceWrapper, iterable=self.data)
        self.cfg.add_configuration('tgt', target=gibbon.SequenceWrapper, container=self.results)

        executor = gibbon.get_async_executor(shutdown=True)
        self.wk_all_rows.prepare(self.cfg)
        self.wk_all_rows.run(executor)
        self.assertSequenceEqual(self.results, self.data)

    def testOnlyPositive(self):
        self.wk_only_pos.validate(verbose=True)
        self.results = []

        self.cfg.add_configuration('src', source=gibbon.SequenceWrapper, iterable=self.data)
        self.cfg.add_configuration('tgt', target=gibbon.SequenceWrapper, container=self.results)

        executor = gibbon.get_async_executor(shutdown=True)
        self.wk_only_pos.prepare(self.cfg)
        self.wk_only_pos.run(executor)
        self.assertSequenceEqual(self.results, [(1,)])


def len_over_three(row):
    return len(row[0]) > 3


class TestFilterString(unittest.TestCase):

    def setUp(self):
        self.data = ['22', '333', '4444']
        self.wk = gibbon.Workflow('all_rows')
        self.wk.add_source('src')
        self.wk.add_transformation('filter', gibbon.Filter, source='src', condition=len_over_three)
        self.wk.add_target('tgt', source='filter')

        self.cfg = gibbon.Configuration()

    def testLenOverThree(self):
        self.wk.validate(verbose=True)
        sink = []
        self.cfg.add_configuration('src', source=gibbon.SequenceWrapper, iterable=zip(self.data))
        self.cfg.add_configuration('tgt', target=gibbon.SequenceWrapper, container=sink)

        executor = gibbon.get_async_executor(shutdown=True)
        self.wk.prepare(self.cfg)
        self.wk.run(executor)
        self.assertSequenceEqual(sink, [('4444',)])


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.DEBUG)
    unittest.main()
