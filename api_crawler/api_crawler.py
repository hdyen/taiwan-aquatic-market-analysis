"""A simple web crawler -- class implementing crawling logic."""

import asyncio
import time
import datetime

try:
    # Python 3.4.
    from asyncio import JoinableQueue as Queue
except ImportError:
    # Python 3.5.
    from asyncio import Queue

import aiohttp

one_day = datetime.timedelta(days=1)


def date_gen(start_date, end_date, delta=one_day):
    current_date = start_date
    while current_date <= end_date:
        yield current_date
        current_date += delta


class Crawler:
    def __init__(self,
                 root,
                 start_date,
                 end_date,
                 max_tasks=10,
                 max_tries=4,
                 loop=None):
        """
        :param start_date: Python date object
        :param end_date: Python date object
        """
        self.root = root
        self.start_date = start_date
        self.end_date = end_date
        self.max_tasks = max_tasks
        self.max_tries = max_tries
        self.loop = loop or asyncio.get_event_loop()

        self.t0 = time.time()
        self.t1 = None

        self.queue = Queue(loop=self.loop)
        self.done_urls = []
        self.seen_url = []
        self.session = aiohttp.ClientSession(loop=self.loop)

    def add_dates_to_queue(self):
        pass

    def fetch(self, url):
        pass

    def parse_json(self):
        pass

    async def work(self):
        """Run the crawler until all finished."""
        workers = [asyncio.Task(self.work(), loop=self.loop)
                   for _ in range(self.max_tasks)]
        self.t0 = time.time()
        await self.queue.join()
        self.t1 = time.time()
        for w in workers:
            w.cancel()

    def store_data(self):
        pass

    def close(self):
        """Close resources."""
        self.session.close()
