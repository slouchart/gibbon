import unittest
from src import gibbon


class TestUnconnectedWorkflow(unittest.TestCase):

    def test_unconnected(self):
        w = gibbon.Workflow('unconnected')
        w.add_source('a_source')
        w.add_target('a_target')
        w.add_transformation('a_filter', gibbon.Filter)
        w.add_complex_transformation('union', gibbon.Union)

        self.assertFalse(w.is_valid)

    def test_reconnect(self):
        w = gibbon.Workflow('unconnected')
        w.add_source('a_source')
        w.add_target('a_target')
        w.add_transformation('a_filter', gibbon.Filter)
        w.add_complex_transformation('union', gibbon.Union)

        w.connect('a_source', 'a_filter', 'union')
        w.connect('a_filter', 'union')
        w.connect('union', 'a_target')

        self.assertTrue(w.is_valid)

    def test_connect_on_create(self):
        w = gibbon.Workflow('unconnected')
        w.add_source('a_source')
        w.add_target('a_target')
        w.add_transformation('a_filter', gibbon.Filter, source='a_source', targets=('a_target',))

        self.assertTrue(w.is_valid)

    def test_connect_on_create_multi_sources(self):
        w = gibbon.Workflow('unconnected')
        w.add_source('a_source')
        w.add_source('another_one')
        w.add_target('a_target')
        w.add_complex_transformation('union', gibbon.Union,
                                     sources=('a_source', 'another_one'), targets=('a_target',))

        self.assertTrue(w.is_valid)


if __name__ == '__main__':
    unittest.main()
