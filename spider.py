import aiohttp
import argparse
import asyncio
import base64
import json
import logging
import os
import queue
import re
import requests
import threading
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin, urlparse
import redis
from bs4 import BeautifulSoup
from random_user_agent.params import SoftwareName, OperatingSystem
from random_user_agent.user_agent import UserAgent

from redis_set import redis_set

# create random user-agent profiles
from utilities import get_full_path

software_names = [SoftwareName.CHROME.value]
operating_systems = [OperatingSystem.WINDOWS.value, OperatingSystem.LINUX.value]
user_agent_rotator = UserAgent(software_names=software_names, operating_systems=operating_systems, limit=20)

# shared pools on redis
shared_link_pool = redis_set('pool_pages')
shared_pdf_pool = redis_set('pool_pdf')
shared_text_extractor_pool = redis_set('pool_text_extractor')
shared_visited_links = redis.Redis()


def initial():
    # create log folder
    Path(ROOT_PATH, 'logs').mkdir(parents=True, exist_ok=True)

    log_file = f"spider_{datetime.today().isoformat(sep='_')}.log"
    log_file = os.path.join(ROOT_PATH, 'logs', log_file)
    logging.basicConfig(level=logging.INFO, filename=log_file, filemode='w+',
                        format='%(levelname)s:%(asctime)s: %(message)s', datefmt='%d-%b %H:%M')

    load_seeds()

    print(f"log file is created....\n{log_file}")


def reset():
    # loop over all keys
    for key in shared_visited_links.scan_iter("*"):
        shared_visited_links.delete(key)

    # delete log files
    for f in os.listdir(os.path.join(ROOT_PATH, 'logs')):
        os.remove(os.path.join(ROOT_PATH, 'logs', f))


def load_seeds():
    if os.path.isfile(SEEDS_PATH):
        total_seeds = []
        with open(SEEDS_PATH) as f:
            seeds = json.load(f)
            for idx, site in enumerate(seeds):
                # add home URLs into the crawler queue with depth 0
                site['depth'] = 0
                site['attempt'] = 0
                site['url'] = site['website']
                site['type'] = 'page'
                total_seeds.append(site)

        update_link_pool(total_seeds)
        update_visited_pages(total_seeds)
        logging.info(f"{len(total_seeds)} seeds are added to link-pool....")

    else:
        print(f"seeds file is not available \n {SEEDS_PATH}")


def get_doc_id(url):
    # TODO to improve, we can use hash functions and convert URL to a shorter equivalent one, for example, Hostname/link_number instead
    #  of using the original link

    doc_id = base64.urlsafe_b64encode(str.encode(url)).decode()
    # due to limitations for length of a file's name in OS
    doc_id = doc_id[:min(200, len(doc_id))]

    return doc_id


def is_url(string):
    # TODO improve it by more complex regex
    return "http" in string


def links2hosts(links):
    """
    categorize links based their hostnames
    :param links: list of links
    :return: dictionary of hosts
    """
    hosts = {}
    for link in links:
        hostname = urlparse(link['url']).netloc
        if hostname not in hosts:
            hosts[hostname] = []
        hosts[hostname].append(link)

    return hosts


def update_link_pool(links):
    if len(links) > 0:
        shared_link_pool.add_items(links)


def update_pdf_link_pool(links):
    if len(links) > 0:
        shared_pdf_pool.add_items(links)


def update_visited_pages(visited_links):
    for link in visited_links:
        doc_id = get_doc_id(link['url'])
        shared_visited_links.set(doc_id, 1)


def is_visited(url):
    doc_id = get_doc_id(url)
    if shared_visited_links.get(doc_id) is not None:
        return True
    return False


def run_in_multithreads(hosts, function, is_async=False, max_threads=2):
    """
        run the function in multi-threads, one thread for each host
    :param hosts: dictionary of hosts
    :param function: target method
    :param is_async: True if function is a async function
    :param max_threads: maximum number of threads
    :return:
    """
    hosts_queue = queue.Queue()

    for host in hosts:
        hosts_queue.put(hosts[host])

    # process each host by one thread
    for i in range(min(max_threads, len(hosts))):
        if is_async:
            t = threading.Thread(target=asyncio.run, args=(function(hosts_queue),), daemon=True)
        else:
            t = threading.Thread(target=function, args=(hosts_queue,), daemon=True)
        t.start()

    hosts_queue.join()


def extract_links_from_html(page_link, html):
    pattern = re.compile("^(/)")
    extracted_links = {}
    soup = BeautifulSoup(html, "html.parser")
    for new_link in soup.find_all("a", href=pattern):
        if "href" in new_link.attrs:
            new_link = new_link.get("href")
            new_link = urljoin(page_link['website'], new_link)

            if new_link not in extracted_links and is_url(new_link) and not is_visited(new_link):
                extracted_links[new_link] = {'name': page_link['name'], 'code': page_link['code'],
                                             'website': page_link['website'],
                                             'url': new_link, 'depth': page_link['depth'] + 1, 'attempt': 0}
    return extracted_links.values()


def extract_links_from_url(page_link):
    """
    extract all unvisited internal and external links
    :param page_link: a link comes from link-pool
    :return: list of links
    """

    if page_link['depth'] >= MAX_DEPTH:
        return None
    elif page_link['attempt'] > MAX_ATTEMPT:
        logging.error(f"Dead link: {str(page_link['url'])}")
        return None

    try:
        html = requests.get(page_link['url']).text
        return extract_links_from_html(page_link, html)

    except BaseException as e:
        logging.error(f"Failed to open: {str(e)}")
        page_link['attempt'] = page_link['attempt'] + 1
        update_link_pool([page_link])
        return None


async def link_analysis(links):
    """
    check availability and determine whether link is pdf-link or page-link and also extract internal links of page-links
    :param links: list of links that are extracted from a web page
    :return: list of pdf-link and failed_link, extracted_links
    """
    tasks = []
    connector = aiohttp.TCPConnector(limit_per_host=LIMIT_PER_HOST)
    async with aiohttp.ClientSession(connector=connector) as session:
        for link in links:
            tasks.append(asyncio.ensure_future(get_link_type(session, link)))
        results = await asyncio.gather(*tasks)

    pdf_links = []
    failed_links = []
    total_extracted_links = []
    for link_type, link, extracted_links in results:

        if link_type is None:
            if link['attempt'] < MAX_ATTEMPT:
                failed_links.append(link)
        elif "application/pdf" in link_type:
            pdf_links.append(link)
        elif "text/html" in link_type:
            total_extracted_links.extend(extracted_links)

    return pdf_links, failed_links, total_extracted_links


async def get_link_type(session, link):
    """
    extract the type of link and internal links of page-links
    :param link: a link come from link-pool
    :return:
    """
    hostname = urlparse(link['url']).netloc
    headers = {'User-Agent': user_agent_rotator.get_random_user_agent()}
    extracted_links = None
    try:
        async with session.get(link['url'], headers=headers) as response:
            link_type = response.headers["Content-Type"]
            if "application/octet-stream" in link_type and 'Content-Disposition' in response.headers and '.pdf' in \
                    response.headers["Content-Disposition"]:
                link_type = "application/pdf"

            elif "text/html" in link_type and hostname in link['url']:
                html = await response.text()
                extracted_links = extract_links_from_html(link, html)

    except BaseException as e:
        logging.error(f"Failed to get link type: {link['url']} \t {str(e)}")
        link['attempt'] = link['attempt'] + 1
        return None, link, None

    return link_type, link, extracted_links


def spider(hosts_queue):
    """
        extract links from a web page and type of links
    :param hosts_queue: a queue of hosts while each item is a list of links for the same host
    :return results add to link-pool and pdf-pool
    """
    links = hosts_queue.get()
    hostname = urlparse(links[0]['url']).netloc

    # analysis links
    pdf_links, failed_links, extracted_links = asyncio.run(link_analysis(links))

    update_pdf_link_pool(pdf_links)

    page_links_count = len(links) - (len(pdf_links) + len(failed_links))
    logging.info(
        f"Processing {page_links_count} page-links, {len(pdf_links)} pdf-links and {len(failed_links)} failed"
        f"-links from {hostname}")

    # extracted links will be processed later with parallelism approach, to overcome the number of concurrent
    # connections for each host, Spider will try to fetch random links of different hosts from link-pool
    if len(extracted_links) > 0:
        update_visited_pages(extracted_links)
        update_link_pool(extracted_links + failed_links)
        logging.info(f'Extracted {len(extracted_links)} links from {hostname} host')
    else:
        update_link_pool(failed_links)

    hosts_queue.task_done()


def run_spiders():
    st = time.time()
    while shared_link_pool.size() > 0:
        # fetch links from different hosts to increase concurrency
        new_site_links = shared_link_pool.get_items()
        hosts = links2hosts(new_site_links)
        run_in_multithreads(hosts, spider, max_threads=MAX_THREADS)

        running_time = (time.time() - st) / 60
        if running_time > MAX_TIMEOUT != 0:
            logging.info("Exit - time over")
            break

    logging.info("Exit - Done")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='eCrawler')

    # TODO we can improve parallelism by adding proxy feature
    # parser.add_argument('-PROXY', type=str, default="", help="a proxy address to run spider over different IP")

    parser.add_argument('-timeout', type=int, default=0, help="max run time in minutes, 0 for infinity running")
    parser.add_argument('--limit_per_host', type=int, default=3,
                        help="number of maximum concurrent connections per host")
    parser.add_argument('--threads', type=int, default=3, help="number of maximum threads")
    parser.add_argument('--depth', type=int, default=3, help="max depth to crawl")
    parser.add_argument('--attempt', type=int, default=3,
                        help="max number of times to try opening a link before ignoring it")
    parser.add_argument('--seeds_path', default="", type=get_full_path,
                        help="path of a file including seed URLs in the JSON format")
    parser.add_argument('--reset', action='store_true', help="delete all data")

    # Parse the argument
    args = parser.parse_args()

    MAX_DEPTH = args.depth
    MAX_TIMEOUT = args.timeout
    MAX_THREADS = args.threads
    LIMIT_PER_HOST = args.limit_per_host
    SEEDS_PATH = args.seeds_path
    MAX_ATTEMPT = args.attempt
    ROOT_PATH = os.getcwd()

    if args.reset:
        reset()

    initial()

    run_spiders()
