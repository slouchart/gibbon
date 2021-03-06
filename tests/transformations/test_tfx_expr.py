import unittest
from src import gibbon


def compute(r):
    return r[0]*2,


class TestExpr(unittest.TestCase):

    def setUp(self):
        self.data = [(0,), (1,), (-1,)]
        self.results = []
        self.wk_default_expr = gibbon.Workflow('default')
        self.wk_default_expr.add_source('src')
        self.wk_default_expr.add_transformation('expression', gibbon.Expression, source='src')
        self.wk_default_expr.add_target('tgt', source='expression')

        self.wk_compute_expr = gibbon.Workflow('compute')
        self.wk_compute_expr.add_source('src')
        self.wk_compute_expr.add_transformation('expression', gibbon.Expression, source='src', func=compute)
        self.wk_compute_expr.add_target('tgt', source='expression')

        self.cfg = gibbon.Configuration()

    def test_default(self):

        self.cfg.add_configuration('src', source=gibbon.SequenceWrapper, iterable=self.data)
        self.cfg.add_configuration('tgt', target=gibbon.SequenceWrapper, container=self.results)

        executor = gibbon.get_async_executor(shutdown=True)
        self.wk_default_expr.prepare(self.cfg)
        self.wk_default_expr.run(executor)
        self.assertSequenceEqual(self.results, self.data)

    def test_check_function(self):
        data = (1,)
        result = compute(data)
        self.assertIsInstance(result, tuple)
        self.assertEqual(result[0], 2)

    def test_compute(self):
        self.results = []

        self.cfg.add_configuration('src', source=gibbon.SequenceWrapper, iterable=self.data)
        self.cfg.add_configuration('tgt', target=gibbon.SequenceWrapper, container=self.results)

        executor = gibbon.get_async_executor(shutdown=True)
        self.wk_compute_expr.prepare(self.cfg)
        self.wk_compute_expr.run(executor)
        self.assertSequenceEqual(self.results, [(0,), (2,), (-2,)])


if __name__ == '__main__':
    unittest.main()
