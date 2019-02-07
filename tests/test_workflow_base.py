from src import gibbon
import unittest


class TestCreate(unittest.TestCase):

    def setUp(self):
        self.wk = gibbon.Workflow('unamed')

    def test_create(self):
        self.assertIsNotNone(self.wk)
        self.assertIsInstance(self.wk, gibbon.Workflow)

    def test_no_name(self):
        with self.assertRaises(gibbon.InvalidNameError) as ctx:
            wk = gibbon.Workflow()
            wk.raise_last()
        exc = ctx.exception
        self.assertEqual(str(exc), 'Object name is mandatory but appears to be missing')

    def test_invalid_name(self):
        with self.assertRaises(gibbon.InvalidNameError) as ctx:
            wk = gibbon.Workflow('0uuuu')
            wk.raise_last()
        exc = ctx.exception
        self.assertRegex(str(exc), r'^Object name is invalid:')


if __name__ == '__main__':
    import logging
    logging.getLogger().setLevel(logging.CRITICAL)
    unittest.main()
