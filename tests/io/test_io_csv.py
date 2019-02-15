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

    def tearDown(self):
        self._loop.stop()
        self._loop.close()


if __name__ == '__main__':
    unittest.main()
