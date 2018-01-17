import asyncio

try:
    # Python 3.4.
    from asyncio import JoinableQueue as Queue
except ImportError:
    # Python 3.5.
    from asyncio import Queue

import aiohttp
from urllib.parse import urlparse

BASE_URL = 'http://m.coa.gov.tw/OpenData/AquaticTransData.aspx?StartDate={}&EndDate={}'


class Crawler:
    """Crawl the aquatic market data of a specific date.
    """

    def __init__(self, date):
        self.date = date
        self.response = b''  # Empty array of bytes.

        roc_year = int(date.strftime('%Y')) - 1911
        query_date = '{:3d}{}'.format(roc_year, date.strftime('%m%d')).replace(' ', '0')
        url = BASE_URL.format(query_date, query_date)

        self.url = urlparse(url)
