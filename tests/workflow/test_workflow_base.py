import unittest

from src import gibbon


class TestCreate(unittest.TestCase):

    def setUp(self):
        self.name = u'unamed'
        self.invalid_name = u'0uuuu'
        self.wk = gibbon.Workflow(self.name)

        self.expected_attrs = ('add_source',
                               'add_target',
                               'add_transformation',
                               'prepare', 'schedule', 'run')

    def test_create(self):
        self.assertIsNotNone(self.wk)
        self.assertIsInstance(self.wk, gibbon.Workflow)
        self.assertTrue(hasattr(self.wk, 'name'))
        self.assertEqual(self.wk.name, self.name)

    def test_iface(self):
        for attr in self.expected_attrs:
            self.assertTrue(hasattr(self.wk, attr))

    def test_no_name(self):
        with self.assertRaises(gibbon.InvalidNameError) as ctx:
            wk = gibbon.Workflow()
            wk.raise_last()
        exc = ctx.exception
        self.assertEqual(str(exc), 'Object name is mandatory but appears to be missing')

    def test_invalid_name(self):
        with self.assertRaises(gibbon.InvalidNameError) as ctx:
            wk = gibbon.Workflow(self.invalid_name)
            wk.raise_last()
        exc = ctx.exception
        self.assertRegex(str(exc), r'^Object name is invalid:')
        self.assertEqual(wk.name, self.invalid_name)


if __name__ == '__main__':
    import logging
    logging.getLogger().setLevel(logging.CRITICAL)
    unittest.main()
