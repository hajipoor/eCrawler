import warnings
from urllib.parse import urlparse

from utilities import get_full_path

warnings.simplefilter("ignore")

import argparse
import logging
import multiprocessing
import time
from datetime import datetime
from multiprocessing.pool import ThreadPool
from pathlib import Path
import json
import os
from dateparser.search import search_dates
from redis_set import redis_set

shared_date_extractor_pool = redis_set('pool_date_extractor')


def initial():
    # create saved folder
    Path(SAVED_PATH).mkdir(parents=True, exist_ok=True)

    log_file = f"date_extractor_{datetime.today().isoformat(sep='_')}.log"
    log_file = os.path.join(ROOT_PATH, 'logs', log_file)
    logging.basicConfig(level=logging.INFO, filename=log_file, filemode='w+',
                        format='%(levelname)s:%(asctime)s: %(message)s', datefmt='%d-%b %H:%M')

    print(f"log file is created....\n{log_file}")


def find_publication_date(item):
    """
    find dates in a document and consider the first one as a publication date of a document
    :param item: a item that is prepared by the m_text_extractor module
    :return: save results in json format in disk:
    """
    global global_counter
    dates = search_dates(item['text'])
    if dates is not None and len(dates) > 0:
        publication_date = dates[0][1].strftime("%d/%m/%Y")
    else:
        publication_date = "[UNKNOWN]"

    item['publication_date'] = publication_date

    file_path = os.path.join(SAVED_PATH, item['doc_id'])

    with open(file_path + ".json", '+w') as fp:
        json.dump(item, fp)


def worker(_):
    """
    run a worker in multi-threads to extract date from text documents
    :return:
    """
    items = shared_date_extractor_pool.get_items(count=50)
    if len(items) > 0:
        pool = ThreadPool(min(MAX_THREADS, len(items)))
        results = pool.map(find_publication_date, items)
        logging.info(f'{len(results)} JSON files are saved')
        pool.close()
        pool.join()


def run_workers():
    """
    run a worker over each processor
    :return:
    """
    processes = min(multiprocessing.cpu_count() - 1, MAX_PROCESSORS)
    pool = multiprocessing.Pool(processes)
    pool.map(worker, range(processes))

    pool.close()
    pool.join()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='eCrawler - Date Extractor')

    # Add an argument
    parser.add_argument('-timeout', type=int, default=0, help="max run time in minutes, 0 for infinity running")
    parser.add_argument('--saved_path', type=get_full_path, default="json/", required=True,
                        help="path of a folder to save json files")
    parser.add_argument('--threads', type=int, default=2, help="number of maximum concurrent on each processor")
    parser.add_argument('--processors', type=int, default=2, help="number of maximum processors")

    # Parse the argument
    args = parser.parse_args()

    MAX_TIMEOUT = args.timeout
    MAX_THREADS = args.threads
    MAX_PROCESSORS = args.threads
    SAVED_PATH = args.saved_path
    ROOT_PATH = os.getcwd()

    initial()

    st = time.time()
    while shared_date_extractor_pool.size()>0:
        run_workers()

        running_time = (time.time() - st) / 60
        if running_time > MAX_TIMEOUT != 0:
            logging.info("Exit - time over")
            break

    logging.info("Exit - Done")
