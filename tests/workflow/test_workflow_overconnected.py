import unittest
import logging

from src import gibbon


class TestTargetToSource(unittest.TestCase):
    def test_tgt2src(self):
        w = gibbon.Workflow('tgt2src')
        w.add_target('tgt')
        w.add_source('src')
        with self.assertRaises(TypeError) as ctx:
            w.connect('tgt', 'src')
        exc = ctx.exception
        self.assertRegex(str(exc), r"Cannot invoke 'set_source' on a NotDownStreamable object$")


class TestSourceToSource(unittest.TestCase):
    def test_src2src(self):
        w = gibbon.Workflow('src2src')
        w.add_source('src_a')
        w.add_source('src_b')
        with self.assertRaises(TypeError) as ctx:
            w.connect('src_a', 'src_b')
        exc = ctx.exception
        self.assertRegex(str(exc), r"Cannot invoke 'set_source' on a NotDownStreamable object$")


class TestTargetToTarget(unittest.TestCase):
    def test_tgt2tgt(self):
        w = gibbon.Workflow('tgt2tgt')
        w.add_target('tgt_a')
        with self.assertRaises(TypeError) as ctx:
            w.add_target('tgt_b', source='tgt_a')
        exc = ctx.exception
        self.assertRegex(str(exc), r"Cannot invoke 'add_target' on a NotUpStreamable object$")

        w.add_target('tgt_b')
        with self.assertRaises(TypeError) as ctx:
            w.connect('tgt_a', 'tgt_b')
        exc = ctx.exception
        self.assertRegex(str(exc), r"Cannot invoke 'add_target' on a NotUpStreamable object$")


class TestTooManyInputs(unittest.TestCase):
    def setUp(self):
        self.cls = type('MonoTfx', (gibbon.MonoDownStreamable, gibbon.UpStreamable, gibbon.Namable), {'id': 'mono_tfx'})

    def test_too_many_inputs(self):
        w = gibbon.Workflow('too_many_inputs')
        w.add_transformation('mono_tfx', self.cls)
        w.add_source('src1')
        w.add_source('src2')
        w.connect('src1', 'mono_tfx')
        w.add_target('tgt', source='mono_tfx')

        with self.assertRaises(gibbon.ParentNodeReset) as ctx:
            w.connect('src2', 'mono_tfx')
        exc = ctx.exception
        self.assertEqual(str(exc), 'Attempt made to reset the target')

        self.assertFalse(w.is_valid)


class TestTooManyOutputs(unittest.TestCase):
    def setUp(self):
        self.cls = type('MultiTfx', (gibbon.MultiDownStreamable, gibbon.UpStreamable, gibbon.Namable),
                        {'id': 'multi_tfx'})

    def test_too_many_outputs(self):
        w = gibbon.Workflow('too_many_outputs')
        w.add_source('src')
        w.add_transformation('multi_tfx', self.cls, sources='src')
        w.add_target('tgt', source='multi_tfx')

        self.assertTrue(w.is_valid)

        with self.assertRaises(gibbon.DuplicatedSource) as ctx:
            w.connect('src', 'multi_tfx')

        exc = ctx.exception
        self.assertEqual(str(exc), 'Source duplicated')

        self.assertTrue(w.is_valid)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.DEBUG)
    unittest.main()
