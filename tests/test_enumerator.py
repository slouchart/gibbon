import unittest

from src import gibbon


class TestEnumerator1(unittest.TestCase):

    def setUp(self):

        self.w = gibbon.Workflow('test_enumerator')
        self.w.add_source('src')
        self.w.add_target('tgt')

    def test_create(self):
        self.w.add_transformation('enum', gibbon.Enumerator)
        self.w.connect('src', 'enum')
        self.w.connect('enum', 'tgt')
        self.w.validate(verbose=True)
        self.assertTrue(self.w.is_valid)


class TestEnumerator2(unittest.TestCase):

    def setUp(self):
        self.data = ['Henry', 'Jane', 'Willy']

        self.w = gibbon.Workflow('test_enumerator')
        self.w.add_source('src')
        self.w.add_transformation('enum', gibbon.Enumerator, source='src')
        self.w.add_target('tgt', source='enum')

        self.cfg = gibbon.Configuration()
        tuples = list(zip(self.data))
        self.cfg.add_configuration('src', source=gibbon.SequenceWrapper, data=tuples)
        self.w.validate(verbose=True)

    def test_run(self):
        sink = []
        self.cfg.add_configuration('tgt', target=gibbon.SequenceWrapper, data=sink)
        executor = gibbon.get_async_executor(shutdown=True)
        self.w.prepare(self.cfg)
        self.w.run(executor)
        self.assertSequenceEqual(sink, list(enumerate(self.data)))


if __name__ == '__main__':
    unittest.main()
