from .base import OneToMany


class Enumerator(OneToMany):

    def __init__(self, *args, start_with=0, reset_after=-1, **kwargs):
        super().__init__(*args, **kwargs)
        self.start_with = start_with
        self._index = self.start_with
        self.reset_after = reset_after

    def get_async_job(self):
        async def job():
            self._index = self.start_with
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
                    if self._index > self.reset_after > 0:
                        self._index = self.start_with

        return job

