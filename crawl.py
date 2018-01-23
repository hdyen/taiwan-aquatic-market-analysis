#!/usr/bin/env python3.6

"""A simple Taiwan aquatic market crawler -- main driver program."""

import argparse
from datetime import datetime
import asyncio
import logging
import sys

import crawler.crawling as crawling

ARGS = argparse.ArgumentParser(description='Taiwan aquatic market crawler')
ARGS.add_argument('--max_tasks', action='store', type=int, metavar='N', default=10, help='Limit concurrent connections')
ARGS.add_argument('--max_tries', action='store', type=int, metavar='N', default=4,
                  help='Limit retries on network errors')
ARGS.add_argument('start_date', action='store')
ARGS.add_argument('end_date', action='store')

log_level_group = ARGS.add_mutually_exclusive_group()
log_level_group.add_argument('-d', '--debug', action='store_const', const=3, dest='level', default=2,
                             help='Log debug messages')
log_level_group.add_argument('-v', '--verbose', action='store_const', const=2, dest='level', default=2,
                             help='Verbose logging (repeat for more verbose)')
log_level_group.add_argument('-q', '--quiet', action='store_const', const=0, dest='level', default=2,
                             help='Only log errors')


def str_to_datetime(date_str):
    return datetime.strptime(date_str, '%Y-%m-%d')


def main():
    """Main program.

    Parse arguments, set up event loop, run crawler, print report.
    """

    args = ARGS.parse_args()

    levels = [logging.ERROR, logging.WARN, logging.INFO, logging.DEBUG]
    logging.basicConfig(level=levels[min(args.level, len(levels) - 1)])

    loop = asyncio.get_event_loop()

    start = str_to_datetime(args.start_date)
    end = str_to_datetime(args.end_date)
    crawler = crawling.Crawler(start, end,
                               max_tasks=args.max_tasks,
                               loop=loop)
    try:
        loop.run_until_complete(crawler.crawl())
    except KeyboardInterrupt:
        sys.stderr.flush()
        print('\nInterrupted\n')
    finally:
        crawler.close()

        # next two lines are required for actual aiohttp resource cleanup
        loop.stop()
        loop.run_forever()

        loop.close()


if __name__ == '__main__':
    main()
