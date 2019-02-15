import unittest
import asyncio
from pathlib import Path

from src import gibbon


class TestCSVOpen(unittest.TestCase):
    def setUp(self):
        self._loop = asyncio.new_event_loop()

        p = Path('.')
        p = Path(p.absolute())
        p = Path(p.parents[1])
        p = Path.joinpath(p, 'tests/sample.csv')

        self.assertFalse(not p.exists(), f"Expected file {p.absolute()} doesn't exist")
        self._filename = p.absolute()

    def test_open_async(self):
        io = gibbon.CSVSourceFile(filename=self._filename, loop=self._loop)

        async def open_file():
            async with io:
                self.assertFalse(io._file_obj.closed)
            self.assertTrue(io._file_obj.closed)

        self._loop.run_until_complete(open_file())

    def test_read_async(self):
        io = gibbon.CSVSourceFile(filename=self._filename, loop=self._loop)

        async def read_file():
            results = []
            async with io:
                async for line in io:
                    results.append(line)
            return results

        results = self._loop.run_until_complete((read_file()))
        self.assertTrue(len(results) > 0)

    def tearDown(self):
        self._loop.stop()
        self._loop.close()


class TestCSVSource(unittest.TestCase):
    def setUp(self):
        self.w = gibbon.Workflow('csv_read')
        self.w.add_source('csv')
        self.w.add_target('list', source='csv')

        p = Path('.')
        p = Path(p.absolute())
        p = Path(p.parents[1])
        p = Path.joinpath(p, 'tests/sample.csv')

        self.assertFalse(not p.exists(), f"Expected file {p.absolute()} doesn't exist")
        self._filename = p.absolute()

    def test_csv_source(self):
        self.assertTrue(self.w.is_valid)

        results = []
        cfg = gibbon.Configuration()
        cfg.add_configuration('csv', source=gibbon.CSVSourceFile, filename=self._filename)
        cfg.add_configuration('list', target=gibbon.SequenceWrapper, container=results)

        self.w.prepare(cfg)
        self.w.run(gibbon.get_async_executor(shutdown=True))
        self.assertTrue(len(results) > 0)
        self.assertIsInstance(results[0], tuple)


class TestCSVTarget(unittest.TestCase):
    def setUp(self):
        self.w = gibbon.Workflow('csv_read')
        self.w.add_source('csv')
        self.w.add_target('list', source='csv')

        p = Path('.')
        p = Path(p.absolute())
        p = Path(p.parents[1])
        p = Path.joinpath(p, 'tests/sample.csv')

        self.assertFalse(not p.exists(), f"Expected file {p.absolute()} doesn't exist")
        self._filename = p.absolute()


if __name__ == '__main__':
    unittest.main()
