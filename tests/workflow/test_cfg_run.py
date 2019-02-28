import unittest

from src import gibbon


class TestCfgRun(unittest.TestCase):

    def test_config_run(self):
        data = [(0,), (1,), (-1,)]
        results = []
        w = gibbon.Workflow('default')
        with w.start_build():
            w.add_source('src')
            w.add_target('tgt', source='src')

        cfg = gibbon.Configuration()
        cfg['src'].source = gibbon.SequenceWrapper
        cfg['src'].iterable = data
        cfg['tgt'].target = gibbon.SequenceWrapper
        cfg['tgt'].container = results

        cfg.configure('truc').using(param1=2)

        executor = gibbon.get_async_executor(shutdown=True)
        executor.run_workflow(w.name, workflow=w, configuration=cfg)
        self.assertSequenceEqual(results, data)


if __name__ == '__main__':
    unittest.main()
