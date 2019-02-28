import unittest
import logging

from src import gibbon


def is_positive_or_zero(r):
    return r[0] >= 0


def is_negative(r):
    return r[0] < 0


def is_positive(r):
    return r[0] > 0


def is_zero(r):
    return r[0] == 0


class TestSelector(unittest.TestCase):
    def setUp(self):
        self.data = [(0,), (1,), (-1,)]

        self.wk_sel_bin = gibbon.Workflow('binary_selector')
        with self.wk_sel_bin.start_build():
            self.wk_sel_bin.add_source('src')

            conditions = (is_positive_or_zero, is_negative)
            self.wk_sel_bin.add_transformation('sel', gibbon.Selector, conditions, sources='src',)
            self.wk_sel_bin.add_target('tgt1', source='sel')
            self.wk_sel_bin.add_target('tgt2', source='sel')

        self.wk_sel_mul = gibbon.Workflow('multiple_selector')
        with self.wk_sel_mul.start_build():
            self.wk_sel_mul.add_source('src')

            conditions = (is_positive, is_negative, is_zero)
            self.wk_sel_mul.add_transformation('sel', gibbon.Selector, conditions, sources='src')

            self.wk_sel_mul.add_target('tgt1', source='sel')
            self.wk_sel_mul.add_target('tgt2', source='sel')
            self.wk_sel_mul.add_target('tgt3', source='sel')

        self.wk_sel_shc = gibbon.Workflow('short_circuit_selector')
        with self.wk_sel_shc.start_build():
            conditions = (is_positive_or_zero, is_zero)
            self.wk_sel_shc.add_source('src')
            self.wk_sel_shc.add_transformation('sel', gibbon.Selector, conditions, sources='src')

            self.wk_sel_shc.add_target('tgt1', source='sel')
            self.wk_sel_shc.add_target('tgt2', source='sel')

        self.wk_sel_def = gibbon.Workflow('default_selector')
        with self.wk_sel_def.start_build():
            conditions = (is_positive, is_negative)

            self.wk_sel_def.add_source('src')
            self.wk_sel_def.add_transformation('sel', gibbon.Selector,
                                               sources='src', conditions=conditions)
            self.wk_sel_def.add_target('tgt1', source='sel')
            self.wk_sel_def.add_target('tgt2', source='sel')

        self.wk_sel_osp1 = gibbon.Workflow('out_port_specified')
        with self.wk_sel_osp1.start_build():
            conditions = (is_positive, is_negative)

            self.wk_sel_osp1.add_source('src')
            self.wk_sel_osp1.add_transformation('sel', gibbon.Selector, conditions, sources='src')

            self.wk_sel_osp1.add_target('tgt1', source='sel')
            self.wk_sel_osp1.add_target('tgt2', source='sel')

        self.wk_sel_osp2 = gibbon.Workflow('no_out_ports_specified')
        with self.wk_sel_osp2.start_build():
            conditions = (is_positive, is_negative)

            self.wk_sel_osp2.add_source('src')
            self.wk_sel_osp2.add_transformation('sel', gibbon.Selector, conditions, sources='src')
            self.wk_sel_osp2.add_target('tgt1', source='sel')
            self.wk_sel_osp2.add_target('tgt2', source='sel')

        self.wk_sel_wd = gibbon.Workflow('with_default')
        with self.wk_sel_wd.start_build():
            self.wk_sel_wd.add_source('src')
            self.wk_sel_wd.add_transformation('sel', gibbon.Selector, conditions, sources='src')
            self.wk_sel_wd.add_target('tgt1', source='sel')
            self.wk_sel_wd.add_target('tgt2', source='sel')

        self.cfg = gibbon.Configuration()

    def testBinarySelection(self):
        self.wk_sel_bin.validate(verbose=True)

        sinks = ([], [])

        self.cfg.configure('src').using(source=gibbon.SequenceWrapper, iterable=self.data)
        self.cfg.configure('tgt1').using(target=gibbon.SequenceWrapper, container=sinks[0])
        self.cfg.configure('tgt2').using(target=gibbon.SequenceWrapper, container=sinks[1])

        executor = gibbon.get_async_executor(shutdown=True)
        executor.run_workflow(self.wk_sel_bin.name, self.wk_sel_bin, self.cfg)

        self.assertSequenceEqual(sinks[0], [(0,), (1,)])
        self.assertSequenceEqual(sinks[1], [(-1,)])

    def testMultipleSelection(self):
        sinks = ([], [], [])

        self.cfg.configure('src').using(source=gibbon.SequenceWrapper, iterable=self.data)
        self.cfg.configure('tgt1').using(target=gibbon.SequenceWrapper, container=sinks[0])
        self.cfg.configure('tgt2').using(target=gibbon.SequenceWrapper, container=sinks[1])
        self.cfg.configure('tgt3').using(target=gibbon.SequenceWrapper, container=sinks[2])

        executor = gibbon.get_async_executor(shutdown=True)
        executor.run_workflow(self.wk_sel_mul.name, self.wk_sel_mul, self.cfg)

        self.assertSequenceEqual(sinks[0], [(1,)])
        self.assertSequenceEqual(sinks[1], [(-1,)])
        self.assertSequenceEqual(sinks[2], [(0,)])

    def testShortCircuit(self):
        """Tip: should not short-circuit, rows are sent wherever it fits even if it means to send it twice or more"""
        data = [(0,)]
        sinks = ([], [])

        self.cfg.configure('src').using(source=gibbon.SequenceWrapper, iterable=data)
        self.cfg.configure('tgt1').using(target=gibbon.SequenceWrapper, container=sinks[0])
        self.cfg.configure('tgt2').using(target=gibbon.SequenceWrapper, container=sinks[1])

        executor = gibbon.get_async_executor(shutdown=True)
        executor.run_workflow(self.wk_sel_shc.name, self.wk_sel_shc, self.cfg)

        self.assertSequenceEqual(sinks[0], [(0,)])
        self.assertSequenceEqual(sinks[1], [(0,)])

    def testDefault(self):
        """Tip: without a default queue, rows that don't match any condition are discarded
        by not being pushed downstream"""
        data = [(1,), (-1,), (0,)]
        sinks = ([], [])

        self.cfg.configure('src').using(source=gibbon.SequenceWrapper, iterable=data)
        self.cfg.configure('tgt1').using(target=gibbon.SequenceWrapper, container=sinks[0])
        self.cfg.configure('tgt2').using(target=gibbon.SequenceWrapper, container=sinks[1])

        executor = gibbon.get_async_executor(shutdown=True)
        executor.run_workflow(self.wk_sel_def.name, self.wk_sel_def, self.cfg)

        self.assertSequenceEqual(sinks[0], [(1,)])
        self.assertSequenceEqual(sinks[1], [(-1,)])

    @unittest.skip  # dubious test following the addition of StreamProcessor mixin
    def testMoreConditions(self):
        """Tip: Selectors are tricky
           Creation fails because Selector don't require argument 'out_ports'"""
        self.assertFalse(self.wk_sel_osp1.is_valid)

    def testNotEnoughConditions(self):
        """Tip: Seriously, use them with care
           Creation passes because 'out_ports' wasn't specified"""
        self.assertTrue(self.wk_sel_osp2.is_valid)

    def testWithDefault1(self):
        """Tip: should pass because a target was added after all gated outports were connected,
           this additional target is used as a default destination for rows that don't
           meet any of the conditions"""
        self.assertTrue(self.wk_sel_wd.is_valid)
        with self.wk_sel_wd.resume_build():
            self.wk_sel_wd.add_target('tgt_default', source='sel')

        self.assertTrue(self.wk_sel_wd.is_valid)

        data = [(0,), (1,), (-1,), (0,)]
        sinks = ([], [], [])

        self.cfg.configure('src').using(source=gibbon.SequenceWrapper, iterable=data)
        self.cfg.configure('tgt1').using(target=gibbon.SequenceWrapper, container=sinks[0])
        self.cfg.configure('tgt2').using(target=gibbon.SequenceWrapper, container=sinks[1])
        self.cfg.configure('tgt_default').using(target=gibbon.SequenceWrapper, container=sinks[2])

        executor = gibbon.get_async_executor(shutdown=True)
        executor.run_workflow(self.wk_sel_wd.name, self.wk_sel_wd, self.cfg)

        self.assertSequenceEqual(sinks[0], [(1,)])
        self.assertSequenceEqual(sinks[1], [(-1,)])
        self.assertSequenceEqual(sinks[2], [(0,), (0,)])

    def testWithDefault2(self):
        """Adding a useless target don't fail"""
        with self.wk_sel_wd.resume_build():
            self.wk_sel_wd.add_target('tgt_default', source='sel')

            # this test show the workflow is valid
            # any way, the Selector raised a build warning saying that a useless target was connected
            with self.assertRaises(gibbon.SelectorHasTooManyTargets) as ctx:
                self.wk_sel_wd.add_target('useless_target', source='sel')

            exc = ctx.exception
            self.assertRegex(str(exc), r'Selector sel has too many targets$')

        self.assertTrue(self.wk_sel_wd.is_valid)

        data = [(0,), (1,), (-1,), (0,)]
        empty = []
        sinks = ([], [], [])

        self.cfg.configure('src').using(source=gibbon.SequenceWrapper, iterable=data)
        self.cfg.configure('tgt1').using(target=gibbon.SequenceWrapper, container=sinks[0])
        self.cfg.configure('tgt2').using(target=gibbon.SequenceWrapper, container=sinks[1])
        self.cfg.configure('tgt_default').using(target=gibbon.SequenceWrapper, container=sinks[2])
        self.cfg.configure('useless_target').using(target=gibbon.SequenceWrapper, container=empty)

        executor = gibbon.get_async_executor(shutdown=True)
        executor.run_workflow(self.wk_sel_wd.name, self.wk_sel_wd, self.cfg)

        self.assertSequenceEqual(sinks[0], [(1,)])
        self.assertSequenceEqual(sinks[1], [(-1,)])
        self.assertSequenceEqual(sinks[2], [(0,), (0,)])
        self.assertSequenceEqual(empty, [])


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.DEBUG)
    unittest.main()
