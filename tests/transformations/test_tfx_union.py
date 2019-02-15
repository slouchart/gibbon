import unittest

from src import gibbon


class TestUnionCreate(unittest.TestCase):
    def setUp(self):
        self.w = gibbon.Workflow('test_union')
        self.w.add_source('src1')
        self.w.add_source('src2')
        self.w.add_target('tgt')

    def test_create(self):
        self.w.add_complex_transformation('union', gibbon.Union)
        self.w.connect('src1', 'union')
        self.w.connect('src2', 'union')
        self.w.connect('union', 'tgt')
        self.w.validate()
        self.assertTrue(self.w.is_valid)


class TestUnionRun(unittest.TestCase):
    def setUp(self):
        self.w = gibbon.Workflow('test_union')
        self.w.add_source('src1')
        self.w.add_source('src2')
        self.w.add_complex_transformation('union', gibbon.Union, sources=('src1', 'src2',))
        self.w.add_target('tgt', source='union')

    def test_union(self):
        self.w.validate()
        self.assertTrue(self.w.is_valid)

        data_src_1 = list(zip(['a', 'b', 'c'], [1, 2, 3]))
        data_src_2 = list(zip(['e', 'f', 'g'], [4, 5, 6]))
        sink = []

        cfg = gibbon.Configuration()
        cfg.add_configuration('src1', source=gibbon.SequenceWrapper, iterable=data_src_1)
        cfg.add_configuration('src2', source=gibbon.SequenceWrapper, iterable=data_src_2)
        cfg.add_configuration('tgt', target=gibbon.SequenceWrapper, container=sink)

        self.w.prepare(cfg)
        self.w.run(gibbon.get_async_executor(shutdown=True))

        self.assertGreater(len(sink), 0)
        self.assertEqual(len(sink), len(data_src_1)+len(data_src_2))
        dict_for_assert = dict(sink)
        ref_for_assert = dict(data_src_1+data_src_2)
        self.assertSequenceEqual(sorted(list(dict_for_assert.keys())), sorted(list(ref_for_assert.keys())))
        self.assertSequenceEqual(sorted(list(dict_for_assert.values())), sorted(list(ref_for_assert.values())))


if __name__ == '__main__':
    unittest.main()
