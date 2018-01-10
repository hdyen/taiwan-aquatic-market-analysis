import socket
from urllib.parse import urlparse
from selectors import DefaultSelector, EVENT_WRITE, EVENT_READ

import sqlite3
import json
from datetime import datetime, timedelta

DATABASE_PATH = 'tw-aquaculture-market.sqlite'
DATABASE_TABLE = 'aquatic_trans'
START_DATE = '2009-01-01'
END_DATE = '2018-01-09'
NUM_FETCHER = 10


def datetime_gen_fn(start_date, end_date, delta=timedelta(days=1)):
    current_date = start_date
    while current_date <= end_date:
        yield current_date
        current_date += delta


class Fetcher:
    def __init__(self, date, sql_conn):
        self.date = date
        self.conn = sql_conn

        self.cur = sql_conn.cursor()

        self.response = b''  # Empty array of bytes.
        self.sock = None

        roc_year = int(date.strftime('%Y')) - 1911
        query_date = '{:3d}{}'.format(roc_year, date.strftime('%m%d')).replace(' ', '0')
        url = base_url.format(query_date, query_date)

        self.url = urlparse(url)

    def read_response(self, key, mask):
        chunk = self.sock.recv(4096)  # 4k chunk size.
        if chunk:
            self.response += chunk
        else:
            selector.unregister(key.fd)  # Done reading.
            self.sock.close()

            try:
                next_date = next(datetime_gen)
                next_fetcher = Fetcher(next_date, self.conn)
                next_fetcher.fetch()
            except StopIteration:
                pass

            json_str = self.response.decode('utf-8').split('\r\n')[-1]
            json_parsed = json.loads(json_str)

            for item in json_parsed:
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
                self.cur.execute(sql, (
                    type_name, type_code, market_name, high_price, low_price, mid_price, avg_price, date, trans_amount))

            self.conn.commit()
            self.cur.close()

            print('{} done'.format(self.date))

    def connected(self, key, mask):
        selector.unregister(key.fd)

        url = self.url.geturl()
        host = self.url.netloc

        request = 'GET {} HTTP/1.0\r\nHost: {}\r\n\r\n'.format(url, host)
        self.sock.send(request.encode('ascii'))

        # Register the next callback.
        selector.register(key.fd,
                          EVENT_READ,
                          self.read_response)

    def fetch(self):
        self.sock = socket.socket()
        self.sock.setblocking(False)

        host = self.url.netloc
        port = self.url.port

        if not port:
            port = 80

        try:
            self.sock.connect((host, port))
        except BlockingIOError:
            pass

        # Register next callback.
        selector.register(self.sock.fileno(),
                          EVENT_WRITE,
                          self.connected)


if __name__ == '__main__':
    selector = DefaultSelector()

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
    cur.close()

    # datetime object generator
    start_date = datetime.strptime(START_DATE, '%Y-%m-%d')
    end_date = datetime.strptime(END_DATE, '%Y-%m-%d')
    datetime_gen = datetime_gen_fn(start_date, end_date)

    base_url = 'http://m.coa.gov.tw/OpenData/AquaticTransData.aspx?StartDate={}&EndDate={}'

    # Start fetchers
    for i in range(NUM_FETCHER):
        try:
            date = next(datetime_gen)
            fetcher = Fetcher(date, conn)
            fetcher.fetch()
        except StopIteration:
            pass

    # Event loop
    while True:
        events = selector.select(timeout=10)

        for event_key, event_mask in events:
            callback = event_key.data
            callback(event_key, event_mask)

    # conn.close()
