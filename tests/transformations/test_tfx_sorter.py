import unittest
import logging

from src import gibbon


class TestSorter(unittest.TestCase):
    def setUp(self):
        self.data = [(0,), (1,), (-1,)]
        self.results = []

        self.wk_sort_asc = gibbon.Workflow('sort_asc')
        with self.wk_sort_asc.start_build():
            self.wk_sort_asc.add_source('src')
            self.wk_sort_asc.add_transformation('sort_asc', gibbon.Sorter, lambda r: r[0],
                                                sources='src')
            self.wk_sort_asc.add_target('tgt', source='sort_asc')

        self.wk_sort_desc = gibbon.Workflow('sort_desc')
        with self.wk_sort_desc.start_build():
            self.wk_sort_desc.add_source('src')
            self.wk_sort_desc.add_transformation('sort_desc', gibbon.Sorter, lambda r: r[0], reverse=True,
                                                 sources='src')
            self.wk_sort_desc.add_target('tgt', source='sort_desc')

        self.cfg = gibbon.Configuration()

    def test_sort_asc(self):

        self.wk_sort_asc.validate(verbose=True)

        self.cfg.configure('src').using(source=gibbon.SequenceWrapper, iterable=self.data)
        self.cfg.configure('tgt').using(target=gibbon.SequenceWrapper, container=self.results)

        executor = gibbon.get_async_executor(shutdown=True)
        executor.run_workflow(self.wk_sort_asc.name, self.wk_sort_asc, self.cfg)

        self.assertSequenceEqual(self.results, [(-1,), (0,), (1,)])

    def test_sort_desc(self):
        self.results = []

        self.cfg.configure('src').using(source=gibbon.SequenceWrapper, iterable=self.data)
        self.cfg.configure('tgt').using(target=gibbon.SequenceWrapper, container=self.results)

        executor = gibbon.get_async_executor(shutdown=True)
        executor.run_workflow(self.wk_sort_desc.name, self.wk_sort_desc, self.cfg)

        self.assertSequenceEqual(self.results, [(1,), (0,), (-1,)])


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.DEBUG)
    unittest.main()
