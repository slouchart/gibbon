from .base import OneToMany


class Enumerator(OneToMany):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._index = None

    def get_async_job(self):
        async def job():
            self._index = 0
            while True:
                row = await self.in_queues[0].get()

                if row is None:
                    for oq in self.out_queues:
                        await oq.put(None)
                    break

                else:
                    row = tuple([self._index] + [f for f in row])
                    for oq in self.out_queues:
                        await oq.put(row)
                    self._index += 1

        return job

