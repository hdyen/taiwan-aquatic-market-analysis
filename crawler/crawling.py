import asyncio

try:
    # Python 3.4.
    from asyncio import JoinableQueue as Queue
except ImportError:
    # Python 3.5.
    from asyncio import Queue

import aiohttp
import time
from datetime import timedelta
import logging
import cgi
import sqlite3

LOGGER = logging.getLogger(__name__)

# from urllib.parse import urlparse

BASE_URL = 'http://m.coa.gov.tw/OpenData/AquaticTransData.aspx?StartDate={}&EndDate={}'


def dates_gen_fn(start_date, end_date, delta=timedelta(days=1)):
    current_date = start_date
    while current_date <= end_date:
        yield current_date
        current_date += delta


DATABASE_PATH = 'tw-aquaculture-market-lab.sqlite'
DATABASE_TABLE = 'aquatic_trans_'

# Database connection
conn = sqlite3.connect(DATABASE_PATH)
cur = conn.cursor()

# Table setup
sql = '''
DROP TABLE IF EXISTS {};
CREATE TABLE {} (
    id           INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
    type_name    TEXT NOT NULL,
    type_code    INTEGER NOT NULL,
    market_name  TEXT NOT NULL,
    high_price   REAL NOT NULL,
    low_price    REAL NOT NULL,
    mid_price    REAL NOT NULL,
    avg_price    REAL NOT NULL,
    date         TEXT NOT NULL,
    trans_amount REAL NOT NULL
)
'''.format(DATABASE_TABLE, DATABASE_TABLE)
cur.executescript(sql)
conn.commit()
# cur.close()


class Crawler:
    """Crawl the aquatic market data of a specific date interval.
    """

    def __init__(self, start_date, end_date, max_tasks=10, max_tries=10, loop=None):
        self.start_date = start_date
        self.end_date = end_date
        self.max_tasks = max_tasks
        self.max_tries = max_tries

        self.loop = loop or asyncio.get_event_loop()
        self.session = aiohttp.ClientSession(loop=self.loop)

        self.q = Queue(loop=self.loop)

        self.t0 = time.time()
        self.t1 = None

        self.make_url_queue()

    def add_url(self, url):
        self.q.put_nowait(url)

    def make_url_queue(self):
        dates = dates_gen_fn(self.start_date, self.end_date)
        for date in dates:
            roc_year = int(date.strftime('%Y')) - 1911
            query_date = '{:3d}{}'.format(roc_year, date.strftime('%m%d')).replace(' ', '0')
            url = BASE_URL.format(query_date, query_date)
            self.add_url(url)

    def close(self):
        self.session.close()

    @asyncio.coroutine
    def parse(self, response):
        # print(response)
        if response.status == 200:
            content_type = response.headers.get('content-type')

            if content_type:
                content_type, pdict = cgi.parse_header(content_type)

            if content_type in ('text/html', 'application/xml'):
                json = yield from response.json(content_type=content_type)
                if json:
                    # print(len(json))
                    for item in json:
                        # print(item)
                        type_name = item['魚貨名稱']
                        type_code = item['品種代碼']
                        market_name = item['市場名稱']
                        high_price = item['上價']
                        low_price = item['下價']
                        mid_price = item['中價']
                        avg_price = item['平均價']
                        date = item['交易日期']
                        trans_amount = item['交易量']

                        sql = '''
                        INSERT INTO {}
                        (type_name, type_code, market_name, high_price, low_price, mid_price, avg_price, date, trans_amount)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)'''.format(DATABASE_TABLE)
                        cur.execute(sql, (
                            type_name, type_code, market_name, high_price, low_price, mid_price, avg_price, date,
                            trans_amount))

                    conn.commit()

        return

    @asyncio.coroutine
    def fetch(self, url):
        """Fetch one URL."""
        tries = 0
        while tries < self.max_tries:
            try:
                response = yield from self.session.get(url, allow_redirects=False)

                if tries > 1:
                    LOGGER.info('try %r for %r success', tries, url)

                break
            except aiohttp.ClientError as client_error:
                LOGGER.info('try %r for %r raised %r', tries, url, client_error)
                # exception = client_error

            tries += 1
        else:
            # We never broke out of the loop: all tries failed.
            return

        try:
            yield from self.parse(response)

        finally:
            yield from response.release()

        print('{} done'.format(url))

    @asyncio.coroutine
    def work(self):

        """Process queue items forever."""
        try:
            while True:
                url = yield from self.q.get()
                yield from self.fetch(url)
                self.q.task_done()
        except asyncio.CancelledError:
            pass

    @asyncio.coroutine
    def crawl(self):
        """Run the crawler until all finished."""
        workers = [asyncio.Task(self.work(), loop=self.loop)
                   for _ in range(self.max_tasks)]
        self.t0 = time.time()
        yield from self.q.join()
        self.t1 = time.time()
        for w in workers:
            w.cancel()

        conn.close()

        dt = self.t1 - self.t0
        print('elapsed time: {}'.format(dt))
