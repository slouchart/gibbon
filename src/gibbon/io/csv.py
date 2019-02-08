import csv


# TODO: make it work using aiofiles and csvreader


def naive_tuple_maker(it):
    return tuple(it)


class CSVSourceFile:
    def __init__(self, filename, tuple_maker=naive_tuple_maker, **fmtopts):
        self._filename = filename
        self._fmt_options = fmtopts
        self.reader = None
        self._to_tuple = tuple_maker

    def __iter__(self):
        return self

    def __next__(self):
        return self._to_tuple(self.reader.__next__())

    def __enter__(self):
        self.file = open(self._filename)
        self.reader = csv.reader(self.file, **self._fmt_options)
        return self

    def __exit__(self, *args):
        self.reader = None
        self.file.close()

