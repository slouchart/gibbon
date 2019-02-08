import unittest

from src import gibbon


class TestFilter(unittest.TestCase):

    @staticmethod
    def all_rows(row):
        return row

    @staticmethod
    def only_positive(row):
        if row[0] > 0:
            return row

    def setUp(self):
        self.data = [(0,), (1,), (-1,)]
        self.results = []
        self.wk_all_rows = gibbon.Workflow('all_rows')
        self.wk_all_rows.add_source('src')
        self.wk_all_rows.add_transformation('filter', gibbon.Filter, source='src', condition=self.all_rows)
        self.wk_all_rows.add_target('tgt', source='filter')

        self.wk_only_pos = gibbon.Workflow('only_positive')
        self.wk_only_pos.add_source('src')
        self.wk_only_pos.add_transformation('filter', gibbon.Filter, source='src', condition=self.only_positive)
        self.wk_only_pos.add_target('tgt', source='filter')

        self.cfg = gibbon.Configuration()
        self.cfg.add_configuration('src', source=gibbon.SequenceWrapper, data=self.data)
        self.cfg.add_configuration('tgt', target=gibbon.SequenceWrapper, data=self.results)

    def testAllRows(self):
        self.cfg = gibbon.Configuration()
        self.cfg.add_configuration('src', source=gibbon.SequenceWrapper, data=self.data)
        self.cfg.add_configuration('tgt', target=gibbon.SequenceWrapper, data=self.results)

        executor = gibbon.get_async_executor()
        self.wk_all_rows.prepare(self.cfg)
        self.wk_all_rows.run(executor)
        self.assertEqual(self.results, self.data)

    def testOnlyPositive(self):
        self.results = []

        self.cfg = gibbon.Configuration()
        self.cfg.add_configuration('src', source=gibbon.SequenceWrapper, data=self.data)
        self.cfg.add_configuration('tgt', target=gibbon.SequenceWrapper, data=self.results)

        executor = gibbon.get_async_executor()
        self.wk_only_pos.prepare(self.cfg)
        self.wk_only_pos.run(executor)
        self.assertEqual(self.results, [v for v in self.data if self.only_positive(v)])


if __name__ == '__main__':
    unittest.main()
