from sys import stdout


class Sequence:
    def __init__(self, data=()):
        self._data = list(data)

    def __iter__(self):
        return iter(self._data)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass


class StdOut:
    def __init__(self):
        self._output = stdout

    def send(self, data):
        print(data, file=self._output)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass
