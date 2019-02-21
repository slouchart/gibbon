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
        with self.assertRaises(TypeError) as ctx:
            gibbon.Workflow()

        exc = ctx.exception
        self.assertEqual(str(exc), "__init__() missing 1 required positional argument: 'name'")

    def test_invalid_name(self):
        with self.assertRaises(gibbon.InvalidNameError) as ctx:
            gibbon.Workflow(self.invalid_name)

        exc = ctx.exception
        self.assertRegex(str(exc), r'^Object name is invalid:')


if __name__ == '__main__':
    import logging
    logging.getLogger().setLevel(logging.CRITICAL)
    unittest.main()
