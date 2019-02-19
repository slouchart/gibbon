from .base import OneToMany, StreamProcessor


class Normalizer(OneToMany, StreamProcessor):
    """
    Given a row of the form: 'Norman', 'MSc.', '50', '70', '25')
    outputs a stream of rows of the form:
    [('Norman', 'MSc.', 'Maths', '50'),
     ('Norman', 'MSc.', 'CS', '70'),
     ('Norman', 'MSc.', 'Physics', '25')]

     where the :key parameter governs how many head fields compose the key
     and the :entries parameter defines the expected missing values
    """
    def __init__(self, *args, key, entries, **kwargs):
        super().__init__(*args, **kwargs)
        self.key = key
        self.entries = entries

    def process_row(self, row):
        rows = []
        values = list(row)
        head, tail = values[:self.key], values[self.key:]
        for key, value in zip(self.entries, tail):
            rows.append(tuple(head + [key, value]))
        return rows

    async def process_rows(self):
        while True:
            row = await self.get_row()
            if self.may_stop_process(row):
                break

            rows = self.process_row(row)

            for row in rows:
                await self.emit_row(row)

        await self.emit_eof()
