import unittest
import logging

from src import gibbon


def row_split(r):
    return [(r[0],), (r[1],)]


def row_split_unbalanced_1(r):  # expect 2 targets, get 1
    return [r]


def row_split_unbalanced_2(r):  # expect 2 targets, get 3
    return [(r[0],), (r[1],), (r[2],)]


class TestConcat(unittest.TestCase):
    def setUp(self):
        self.w = gibbon.Workflow('concat')
        with self.w.start_build():
            self.w.add_source('src1')
            self.w.add_source('src2')
            self.w.add_transformation('concat', gibbon.Concat, sources=('src1', 'src2'))
            self.w.add_target('tgt', source='concat')

    def test_concat(self):
        self.w.validate(verbose=True)
        self.assertTrue(self.w.is_valid)

        src1 = [('row_id',)]
        src2 = [('row_data',)]
        sink = []

        cfg = gibbon.Configuration()
        cfg.configure('src1').using(source=gibbon.SequenceWrapper, iterable=src1)
        cfg.configure('src2').using(source=gibbon.SequenceWrapper, iterable=src2)
        cfg.configure('tgt').using(target=gibbon.SequenceWrapper, container=sink)

        executor = gibbon.get_async_executor(shutdown=True)
        executor.run_workflow(self.w.name, self.w, cfg)

        self.assertTrue(len(sink))
        self.assertEqual(sink[0], ('row_id', 'row_data'))

    def test_concat_unbalanced(self):
        self.assertTrue(self.w.is_valid)

        src1 = [('row_id',), ('another_id',)]
        src2 = [('row_data',)]
        sink = []

        cfg = gibbon.Configuration()
        cfg.configure('src1').using(source=gibbon.SequenceWrapper, iterable=src1)
        cfg.configure('src2').using(source=gibbon.SequenceWrapper, iterable=src2)
        cfg.configure('tgt').using(target=gibbon.SequenceWrapper, container=sink)

        executor = gibbon.get_async_executor(shutdown=True)
        executor.run_workflow(self.w.name, self.w, cfg)

        self.assertTrue(len(sink))
        self.assertEqual(len(sink), 2)
        self.assertEqual(sink[0], ('row_id', 'row_data'))
        self.assertEqual(sink[1], ('another_id',))


class TestSplit(unittest.TestCase):

    def setUp(self):
        self.w = gibbon.Workflow('split')
        with self.w.start_build():
            self.w.add_source('src')
            self.w.add_transformation('splitter', gibbon.Split, func=row_split, sources='src')
            self.w.add_target('tgt1', source='splitter')
            self.w.add_target('tgt2', source='splitter')

    def test_split(self):
        self.w.validate(verbose=True)
        self.assertTrue(self.w.is_valid)
        src = [('row_id', 'row_data')]
        sink1 = []
        sink2 = []

        cfg = gibbon.Configuration()
        cfg.configure('src').using(source=gibbon.SequenceWrapper, iterable=src)
        cfg.configure('tgt1').using(target=gibbon.SequenceWrapper, container=sink1)
        cfg.configure('tgt2').using(target=gibbon.SequenceWrapper, container=sink2)

        executor = gibbon.get_async_executor(shutdown=True)
        executor.run_workflow(self.w.name, self.w, cfg)

        self.assertEqual(len(sink1), 1)
        self.assertEqual(len(sink2), 1)
        self.assertEqual(sink1[0], ('row_id',))
        self.assertEqual(sink2[0], ('row_data',))


class TestSplitUnbalancedOne(unittest.TestCase):
    def setUp(self):
        self.w = gibbon.Workflow('split_unbalanced_1')
        with self.w.start_build():
            self.w.add_source('src')
            self.w.add_transformation('splitter', gibbon.Split, func=row_split_unbalanced_1, sources='src')
            self.w.add_target('tgt1', source='splitter')
            self.w.add_target('tgt2', source='splitter')

    def test_split_unbalanced(self):
        self.w.validate(verbose=True)
        self.assertTrue(self.w.is_valid)
        src = [('row_id', 'row_data')]
        sink1 = []
        sink2 = []

        cfg = gibbon.Configuration()
        cfg.configure('src').using(source=gibbon.SequenceWrapper, iterable=src)
        cfg.configure('tgt1').using(target=gibbon.SequenceWrapper, container=sink1)
        cfg.configure('tgt2').using(target=gibbon.SequenceWrapper, container=sink2)

        executor = gibbon.get_async_executor(shutdown=True)
        executor.run_workflow(self.w.name, self.w, cfg)

        self.assertEqual(len(sink1), 1)
        self.assertEqual(len(sink2), 0)
        self.assertEqual(sink1[0], src[0])


class TestSplitUnbalancedTwo(unittest.TestCase):
    def setUp(self):
        self.w = gibbon.Workflow('split_unbalanced_2')
        with self.w.start_build():
            self.w.add_source('src')
            self.w.add_transformation('splitter', gibbon.Split, func=row_split_unbalanced_2, sources='src')
            self.w.add_target('tgt1', source='splitter')
            self.w.add_target('tgt2', source='splitter')

    def test_split_unbalanced(self):
        self.w.validate(verbose=True)
        self.assertTrue(self.w.is_valid)
        src = [('row_id', 'row_data', 'row_info')]
        sink1 = []
        sink2 = []

        cfg = gibbon.Configuration()
        cfg.configure('src').using(source=gibbon.SequenceWrapper, iterable=src)
        cfg.configure('tgt1').using(target=gibbon.SequenceWrapper, container=sink1)
        cfg.configure('tgt2').using(target=gibbon.SequenceWrapper, container=sink2)

        executor = gibbon.get_async_executor(shutdown=True)
        executor.run_workflow(self.w.name, self.w, cfg)

        self.assertEqual(len(sink1), 1)
        self.assertEqual(len(sink2), 1)
        self.assertEqual(sink1[0], ('row_id',))
        self.assertEqual(sink2[0], ('row_data',))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.DEBUG)
    unittest.main()
