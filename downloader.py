import argparse
import asyncio
import logging
import os
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

import aiofiles
import aiohttp
from spider import user_agent_rotator, get_doc_id, links2hosts, run_in_multithreads
from redis_set import redis_set
from utilities import create_log_file, get_full_path

shared_pdf_pool = redis_set('pool_pdf')
shared_text_extractor_pool = redis_set('pool_text_extractor')


def initial():
    # create download and log folder
    Path(DOWNLOAD_PATH).mkdir(parents=True, exist_ok=True)
    Path(ROOT_PATH, 'logs').mkdir(parents=True, exist_ok=True)

    log_file = f"downloader_{datetime.today().isoformat(sep='_')}.log"

    create_log_file(ROOT_PATH,log_file)


def update_pdf_link_pool(links):
    if len(links) > 0:
        shared_pdf_pool.add_items(links)


def update_text_extractor_pool(links):
    for idx, link in enumerate(links):
        record = {'doc_id': link['doc_id'],
                  'locations': [{'name': link['name'], 'uid': link['code'], 'url': link['website']}],
                  'origin_url': link['url']}
        links[idx] = record

    shared_text_extractor_pool.add_items(links)


async def download_batch_pdf(hosts_queue):
    """
    download PDFs concurrently
    :param hosts_queue: a queue of hosts while each item is a list of pdf-links for the same host
    :return: add results into text_extractor_pool
    """

    pdf_links = hosts_queue.get()

    if len(pdf_links) < 1: return
    headers = {'User-Agent': user_agent_rotator.get_random_user_agent()}
    hostname = urlparse(pdf_links[0]['url']).netloc

    logging.info(f"Starting download {len(pdf_links)} PDFs from {hostname} host")

    tasks = []
    connector = aiohttp.TCPConnector(limit_per_host=LIMIT_PER_HOST)
    async with aiohttp.ClientSession(connector=connector) as session:
        for link in pdf_links:
            tasks.append(download_single_pdf(session, headers, link))
        result_downloaded_links = await asyncio.gather(*tasks)

    failed_links = []
    downloaded_links = []
    for status, link in result_downloaded_links:
        if status is None:
            failed_links.append(link)
        else:
            downloaded_links.append(link)

    update_pdf_link_pool(failed_links)
    update_text_extractor_pool(downloaded_links)

    logging.info(f"Downloaded {len(downloaded_links)} PDFs ({len(failed_links)} failed) from {hostname} host")

    hosts_queue.task_done()


async def download_single_pdf(session, headers, pdf_link):
    """
    Download one PDF
    :return: doc_id and the pdf_link
    """
    pdf_link['doc_id'] = get_doc_id(pdf_link['url'])
    file_path = os.path.join(DOWNLOAD_PATH, pdf_link['doc_id'] + '.pdf')
    logging.info(f"downloading {pdf_link['url']}")

    async with session.get(pdf_link['url'], headers=headers) as response:
        content = await response.read()

    # in case that url was redirected to another place
    pdf_link['url'] = str(response.url)

    # TODO limit_per_host can be adjusted dynamically for each host based on a status code, for example. Code 503 is
    #  a good signal to decrease limit_per_host
    if response.status != 200:
        logging.error(f"Failed to downloading : {pdf_link['url']} status-code: {response.status}")
        pdf_link['attempt'] = pdf_link['attempt'] + 1

        return None, pdf_link

    async with aiofiles.open(file_path, "+wb") as f:
        await f.write(content)

    return True, pdf_link


def run_downloaders():
    st = time.time()
    while shared_pdf_pool.size() > 0:
        pdf_links = shared_pdf_pool.get_items()

        hosts = links2hosts(pdf_links)

        # process each host by one thread
        run_in_multithreads(hosts, download_batch_pdf, is_async=True, max_threads=MAX_THREADS)

        running_time = (time.time() - st) / 60
        if running_time > MAX_TIMEOUT != 0:
            logging.info("Exit - time over")
            break

    logging.info("Exit - Done")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='eCrawler')

    # Add arguments
    parser.add_argument('-timeout', type=int, default=0, help="max run time in minutes, 0 for infinity running")
    parser.add_argument('--download_path', type=get_full_path, default="downloaded/", required=True,
                        help="path of a folder for saving downloaded pdfs")

    parser.add_argument('--limit_per_host', type=int, default=3,
                        help="number of maximum concurrent connections per host")
    parser.add_argument('--threads', type=int, default=3, help="number of maximum threads")
    parser.add_argument('--attempt', type=int, default=3,
                        help="max number of times to try opening a link before ignoring it")
    parser.add_argument('--seeds_path', default="", type=str,
                        help="path of a file including seed URLs in the JSON format")

    # Parse the argument
    args = parser.parse_args()

    MAX_TIMEOUT = args.timeout
    MAX_THREADS = args.threads
    LIMIT_PER_HOST = args.limit_per_host
    DOWNLOAD_PATH = args.download_path
    SEEDS_PATH = args.seeds_path
    MAX_ATTEMPT = args.attempt
    ROOT_PATH = os.getcwd()

    initial()
    run_downloaders()
