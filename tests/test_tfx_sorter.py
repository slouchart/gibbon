import unittest
from src import gibbon


class TestSorter(unittest.TestCase):
    def setUp(self):
        self.data = [(0,), (1,), (-1,)]
        self.results = []
        self.wk_sort_asc = gibbon.Workflow('sort_asc')
        self.wk_sort_asc.add_source('src')
        self.wk_sort_asc.add_transformation('sort_asc', gibbon.Sorter, source='src', key=lambda r: r[0])
        self.wk_sort_asc.add_target('tgt', source='sort_asc')

        self.wk_sort_desc = gibbon.Workflow('sort_desc')
        self.wk_sort_desc.add_source('src')
        self.wk_sort_desc.add_transformation('sort_desc', gibbon.Sorter, source='src', key=lambda r: r[0], reverse=True)
        self.wk_sort_desc.add_target('tgt', source='sort_desc')

        self.cfg = gibbon.Configuration()

    def test_sort_asc(self):

        self.cfg.add_configuration('src', source=gibbon.SequenceWrapper, data=self.data)
        self.cfg.add_configuration('tgt', target=gibbon.SequenceWrapper, data=self.results)

        executor = gibbon.get_async_executor()
        self.wk_sort_asc.prepare(self.cfg)
        self.wk_sort_asc.run(executor)
        self.assertEqual(self.results, [(-1,), (0,), (1,)])

    def test_sort_desc(self):
        self.results = []

        self.cfg.add_configuration('src', source=gibbon.SequenceWrapper, data=self.data)
        self.cfg.add_configuration('tgt', target=gibbon.SequenceWrapper, data=self.results)

        executor = gibbon.get_async_executor()
        self.wk_sort_desc.prepare(self.cfg)
        self.wk_sort_desc.run(executor)
        self.assertEqual(self.results, [(1,), (0,), (-1,)])

if __name__ == '__main__':
    unittest.main()
