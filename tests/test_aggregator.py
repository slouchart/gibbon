import unittest

from src import gibbon


class TestAggCreate(unittest.TestCase):
    def setUp(self):
        self.w = gibbon.Workflow('test_agg')
        self.w.add_source('src')
        self.w.add_target('tgt')

    def test_create(self):
        self.w.add_transformation('agg', gibbon.Aggregator, key=lambda r: r, func=lambda r: 0)
        self.w.connect('src', 'agg')
        self.w.connect('agg', 'tgt')
        self.w.validate(verbose=True)
        self.assertTrue(self.w.is_valid)


class TestAggCounter(unittest.TestCase):
    def setUp(self):
        self.w = gibbon.Workflow('test_count')
        self.w.add_source('src')
        self.w.add_target('tgt')

        self.w.add_transformation('counter', gibbon.Aggregator, key=lambda r: ('count:',), func=lambda r, c: (c+1,))

        self.w.connect('src', 'counter')
        self.w.connect('counter', 'tgt')

    def test_row_count_func(self):
        func = gibbon.row_count()
        r = None
        c = 0
        self.assertEqual(func(r, c), (1,))

    def test_simple_sum_func(self):
        func = gibbon.simple_sum(0)
        r = (4,)
        s = 5
        self.assertEqual(func(r, s), (9,))

    def test_count(self):
        cfg = gibbon.Configuration()
        data = list(zip(['a', 'b', 'c']))
        sink = []
        cfg.add_configuration('src', source=gibbon.SequenceWrapper, data=data)
        cfg.add_configuration('tgt', target=gibbon.SequenceWrapper, data=sink)
        self.w.prepare(cfg)
        self.w.run(gibbon.get_async_executor(shutdown=True))
        self.assertSequenceEqual(sink, [('count:', 3,)])


class TestAggSum(unittest.TestCase):
    def setUp(self):
        self.w = gibbon.Workflow('test_sum')
        self.w.add_source('src')
        self.w.add_target('tgt')

        self.w.add_transformation('sum', gibbon.Aggregator, key=lambda r: ('sum:',), func=lambda r, s: (s+r[0],))

        self.w.connect('src', 'sum')
        self.w.connect('sum', 'tgt')

    def test_sum(self):
        cfg = gibbon.Configuration()
        data = list(zip([1, 2, 3]))
        sink = []
        cfg.add_configuration('src', source=gibbon.SequenceWrapper, data=data)
        cfg.add_configuration('tgt', target=gibbon.SequenceWrapper, data=sink)
        self.w.prepare(cfg)
        self.w.run(gibbon.get_async_executor(shutdown=True))
        self.assertSequenceEqual(sink, [('sum:', 6,)])


class TestAggProduct(unittest.TestCase):
    def setUp(self):
        self.w = gibbon.Workflow('test_prd')
        self.w.add_source('src')
        self.w.add_target('tgt')

        self.w.add_transformation('prd', gibbon.Aggregator, key=lambda r: ('product:',),
                                  func=lambda r, p: (p*r[0],), initializer=(1,))

        self.w.connect('src', 'prd')
        self.w.connect('prd', 'tgt')

    def test_product(self):
        cfg = gibbon.Configuration()
        data = list(zip([1, 2, 3]))
        sink = []
        cfg.add_configuration('src', source=gibbon.SequenceWrapper, data=data)
        cfg.add_configuration('tgt', target=gibbon.SequenceWrapper, data=sink)
        self.w.prepare(cfg)
        self.w.run(gibbon.get_async_executor(shutdown=True))
        self.assertSequenceEqual(sink, [('product:', 6,)])


class TestAggStringConcat(unittest.TestCase):
    def setUp(self):
        self.w = gibbon.Workflow('test_str_concat')
        self.w.add_source('src')
        self.w.add_target('tgt')

        self.w.add_transformation('concat', gibbon.Aggregator, key=lambda r: ('concat:',),
                                  func=lambda r, s: (s+r[0],), initializer=('',))

        self.w.connect('src', 'concat')
        self.w.connect('concat', 'tgt')

    def test_count(self):
        cfg = gibbon.Configuration()
        data = list(zip(['a', 'b', 'c']))
        sink = []
        cfg.add_configuration('src', source=gibbon.SequenceWrapper, data=data)
        cfg.add_configuration('tgt', target=gibbon.SequenceWrapper, data=sink)
        self.w.prepare(cfg)
        self.w.run(gibbon.get_async_executor(shutdown=True))
        self.assertSequenceEqual(sink, [('concat:', 'abc',)])


class TestAggGroupBySum(unittest.TestCase):
    def setUp(self):
        self.w = gibbon.Workflow('test_group_by')
        self.w.add_source('src')
        self.w.add_target('tgt')

        self.w.add_transformation('group', gibbon.Aggregator, key=lambda r: r[0], func=lambda r, s: (s+r[1],))

        self.w.connect('src', 'group')
        self.w.connect('group', 'tgt')

    def test_group_by(self):
        cfg = gibbon.Configuration()
        data = list(zip(['a', 'b', 'a', 'b'], [1, 2, 3, 0]))
        sink = []
        cfg.add_configuration('src', source=gibbon.SequenceWrapper, data=data)
        cfg.add_configuration('tgt', target=gibbon.SequenceWrapper, data=sink)
        self.w.prepare(cfg)
        self.w.run(gibbon.get_async_executor(shutdown=True))
        self.assertEqual(len(sink), 2)
        self.assertTrue('a' in dict(sink))
        self.assertTrue('b' in dict(sink))
        self.assertEqual(dict(sink)['a'], 4)
        self.assertEqual(dict(sink)['b'], 2)


if __name__ == '__main__':
    unittest.main()
