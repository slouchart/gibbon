import unittest
from src import gibbon


class TestReach1(unittest.TestCase):
    def setUp(self):
        self.wks = []
        for inx in range(3):
            self.wks.append(gibbon.Workflow(f'test_{inx}'))

    def test_reach_1(self):
        self.wks[0].add_source('src1')
        self.assertFalse(self.wks[0].is_valid)

    def test_reach_2(self):
        self.wks[1].add_target('tgt1', source=None)
        self.assertFalse(self.wks[1].is_valid)

    def test_reach_3(self):
        self.wks[0].add_target('tgt1', source='unknown')
        self.assertFalse(self.wks[0].is_valid)

    def test_reach_4(self):
        self.wks[2].add_source('src')
        self.wks[2].add_target('tgt', source='src')
        self.assertTrue(self.wks[2].is_valid)


class TestReach2(unittest.TestCase):
    def setUp(self):
        self.wks = []
        for inx in range(3):
            self.wks.append(gibbon.Workflow(f'test_{inx}'))

    def test_reach_1(self):
        self.wks[0].add_source('src')
        self.wks[0].add_target('tgt1', source='src')
        self.wks[0].add_target('tgt2', source='src')
        self.assertTrue(self.wks[0].is_valid)

    def test_reach_2(self):
        self.wks[1].add_source('src1')
        self.wks[1].add_target('tgt1', source=None)
        self.wks[1].add_target('tgt2', source='src1')
        self.assertFalse(self.wks[1].is_valid)
        with self.assertRaises(gibbon.NodeReachabilityError) as ctx:
            self.wks[1].raise_last()

        exc = ctx.exception
        self.assertRegex(str(exc), r'^.* unconnected to a source$')

    def test_reach_3(self):
        self.wks[2].add_source('src')
        self.wks[2].add_transformation('exp1', gibbon.Expression, source='src')
        self.wks[2].add_transformation('exp2', gibbon.Expression, source='src')
        self.wks[2].add_target('tgt1', source='exp2')

        self.assertFalse(self.wks[2].is_valid)
        with self.assertRaises(gibbon.NodeReachabilityError) as ctx:
            self.wks[2].raise_last()

        exc = ctx.exception
        self.assertRegex(str(exc), r'^.* unconnected to a target$')


if __name__ == '__main__':
    import logging
    logging.getLogger().setLevel(logging.CRITICAL)
    unittest.main()
