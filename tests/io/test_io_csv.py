import unittest
import asyncio
from pathlib import Path
import os

from src import gibbon


def get_src_path():
    p = Path(__file__)
    p = Path(p.absolute())
    p = Path(p.parents[1])
    p = Path.joinpath(p, 'sample.csv')
    return p


def get_tgt_path():
    p = Path(__file__)
    p = Path(p.absolute())
    p = Path(p.parents[1])
    p = Path.joinpath(p, 'output.csv')
    return p


class TestCSVOpen(unittest.TestCase):
    def setUp(self):
        self._loop = asyncio.new_event_loop()

        p = get_src_path()

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
            lines = []
            async with io:
                async for line in io:
                    lines.append(line)
            return lines

        results = self._loop.run_until_complete((read_file()))
        self.assertTrue(len(results) > 0)

    def tearDown(self):
        self._loop.stop()
        self._loop.close()


class TestCSVSource(unittest.TestCase):
    def setUp(self):
        self.w = gibbon.Workflow('csv_read')
        with self.w.start_build():
            self.w.add_source('csv')
            self.w.add_target('list', source='csv')

        p = get_src_path()

        self.assertFalse(not p.exists(), f"Expected file {p.absolute()} does not exist")
        self._filename = p.absolute()

    def test_csv_source(self):
        self.assertTrue(self.w.is_valid)

        results = []
        cfg = gibbon.Configuration()
        cfg.configure('csv').using(source=gibbon.CSVSourceFile, filename=self._filename)
        cfg.configure('list').using(target=gibbon.SequenceWrapper, container=results)

        executor = gibbon.get_async_executor(shutdown=True)
        executor.run_workflow(self.w.name, workflow=self.w, configuration=cfg)

        self.assertTrue(len(results) > 0)
        self.assertIsInstance(results[0], tuple)


class TestCSVTarget(unittest.TestCase):
    def setUp(self):
        self.w = gibbon.Workflow('csv_write')
        with self.w.start_build():
            self.w.add_source('src')
            self.w.add_target('csv', source='src')

        p = get_tgt_path()
        self._filename = p.absolute()
        if p.exists():
            os.remove(p.absolute())

    def test_csv_target(self):
        self.assertTrue(self.w.is_valid)

        input_data = [
            ('Brian', 23),
            ('Joe', 'ERROR'),
            ('Mary', 40),
            ('Alice', 25),
            ('Billy', 15),
        ]
        cfg = gibbon.Configuration()
        cfg.configure('src').using(source=gibbon.SequenceWrapper, iterable=input_data)
        cfg.configure('csv').using(target=gibbon.CSVTargetFile, filename=self._filename)

        gibbon.get_async_executor(shutdown=True).run_workflow(self.w.name, self.w, configuration=cfg)
        self.assertFileContent()

    def assertFileContent(self):

        with open(self._filename, mode='r') as f:
            content = f.readlines()

        self.assertTrue(content is not None)

    def tearDown(self):
        p = Path(self._filename)
        if p.exists():
            os.remove(p.absolute())


if __name__ == '__main__':
    unittest.main()
