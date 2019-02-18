import unittest
import logging

from src import gibbon


class TestNormalizerOne(unittest.TestCase):
    def setUp(self):
        self.w = gibbon.Workflow('test_normalizer')
        self.w.add_source('src')
        self.w.add_target('tgt')

    def test_connect_normalizer(self):
        self.w.add_transformation('normalize', gibbon.Normalizer, source='src', key=0, entries=[])
        self.w.connect('normalize', 'tgt')
        self.assertTrue(self.w.is_valid)


class TestNormalizerTwo(unittest.TestCase):
    def setUp(self):
        self.w = gibbon.Workflow('test_normalizer')
        self.w.add_source('src')
        self.w.add_target('tgt')

        self.w.add_transformation('normalize', gibbon.Normalizer, source='src',
                                  key=2, entries=['Maths', 'CS', 'Physics'])
        self.w.connect('normalize', 'tgt')

        self.w.validate(verbose=True)

    def test_normalizer(self):
        cfg = gibbon.Configuration()

        data = [('Norman', 'MSc.', '50', '70', '25')]
        sink = []
        cfg.add_configuration('src', source=gibbon.SequenceWrapper, iterable=data)
        cfg.add_configuration('tgt', target=gibbon.SequenceWrapper, container=sink)

        self.w.prepare(cfg)
        self.w.run(gibbon.get_async_executor(shutdown=True))

        self.assertEqual(len(sink), 3)
        logging.info(sink)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.DEBUG)
    unittest.main()
