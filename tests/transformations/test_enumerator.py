import unittest

from src import gibbon


class TestEnumerator1(unittest.TestCase):

    def setUp(self):

        self.w = gibbon.Workflow('test_enumerator')
        with self.w.start_build():
            self.w.add_source('src')
            self.w.add_target('tgt')

    def test_create(self):
        with self.w.resume_build():
            self.w.add_transformation('enum', gibbon.Enumerator)
            self.w.connect('src', 'enum')
            self.w.connect('enum', 'tgt')

        self.w.validate(verbose=True)
        self.assertTrue(self.w.is_valid)


class TestEnumerator2(unittest.TestCase):

    def setUp(self):
        self.data = ['Henry', 'Jane', 'Willy']

        self.w = gibbon.Workflow('test_enumerator')
        with self.w.start_build():
            self.w.add_source('src')
            self.w.add_transformation('enum', gibbon.Enumerator, sources='src')
            self.w.add_target('tgt', source='enum')

        self.cfg = gibbon.Configuration()
        tuples = list(zip(self.data))
        self.cfg.configure('src').using(source=gibbon.SequenceWrapper, iterable=tuples)
        self.w.validate(verbose=True)

    def test_run(self):
        sink = []
        self.cfg.configure('tgt').using(target=gibbon.SequenceWrapper, container=sink)
        executor = gibbon.get_async_executor(shutdown=True)

        executor.run_workflow(self.w.name, self.w, self.cfg)
        self.assertSequenceEqual(sink, list(enumerate(self.data)))


class TestEnumerator3(unittest.TestCase):

    def setUp(self):
        self.data = ['Henry', 'Jane', 'Willy']
        self.w = gibbon.Workflow('test_enumerator')

        with self.w.start_build():
            self.w.add_source('src')
            self.w.add_transformation('enum', gibbon.Enumerator, sources='src', start_with=1, reset_after=1)
            self.w.add_target('tgt', source='enum')

        self.cfg = gibbon.Configuration()
        tuples = list(zip(self.data))
        self.cfg.configure('src').using(source=gibbon.SequenceWrapper, iterable=tuples)
        self.w.validate(verbose=True)

    def test_run(self):
        sink = []
        self.cfg.configure('tgt').using(target=gibbon.SequenceWrapper, container=sink)
        executor = gibbon.get_async_executor(shutdown=True)
        executor.run_workflow(self.w.name, self.w, self.cfg)
        self.assertSequenceEqual(sink, list(zip([1]*len(self.data), self.data)))


if __name__ == '__main__':
    unittest.main()
