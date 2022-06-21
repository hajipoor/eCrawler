import argparse
import logging
import multiprocessing
import time
from datetime import datetime
from multiprocessing.pool import ThreadPool
import os
import fitz

from redis_set import redis_set
from utilities import get_full_path

shared_text_extractor_pool = redis_set('pool_text_extractor')
shared_date_extractor_pool = redis_set('pool_date_extractor')


def initial():
    log_file = f"text_extractor_{datetime.today().isoformat(sep='_')}.log"
    log_file = os.path.join(ROOT_PATH, 'logs', log_file)
    logging.basicConfig(level=logging.INFO, filename=log_file, filemode='w+',
                        format='%(levelname)s:%(asctime)s: %(message)s', datefmt='%d-%b %H:%M')

    print(f"log file is created....\n{log_file}")


def update_date_extractor_pool(items):
    shared_date_extractor_pool.add_items(items)


def get_text_percentage(file_path):
    """
    Calculate the percentage of document that is covered by searchable text.

    If the returned percentage of text is very low, the document has a high chance to be a scanned PDF
    """
    total_page_area = 0.0
    total_text_area = 0.0
    try:
        doc = fitz.open(file_path)
    except BaseException as e:
        logging.error(f'Failed to read : {str(e)}')
        return None

    for page_num, page in enumerate(doc):
        total_page_area = total_page_area + abs(page.rect)
        text_area = 0.0
        for b in page.getTextBlocks():
            r = fitz.Rect(b[:4])  # rectangle where block text appears
            text_area = text_area + abs(r)
        total_text_area = total_text_area + text_area
    doc.close()

    return total_text_area / total_page_area


def get_pdf_text(file_path):
    """
    convert normal PDFs to text
    :param file_path: path of a PDF file
    :return: text of PDF
    """
    try:
        doc = fitz.open(file_path)
        pages_text = []
        for page in doc:  # iterate the document pages
            pages_text.append(page.getText())  # get plain text

        return '\n'.join(pages_text)

    except BaseException as e:
        logging.error(f'Failed to convert : {str(e)}')
        return None


def pdf2text(item):
    """
    convert both scanned and normal PDFs to text
    :param item: a item that is prepared by the m_crawler module
    :return: text of PDF
    """
    file_name = item['doc_id']
    file_path = os.path.join(DOWNLOAD_PATH, file_name + '.pdf')
    text_perc = get_text_percentage(file_path)
    if text_perc is not None:
        if text_perc < 0.01:
            # fully scanned PDF - no relevant text
            # TODO OCR
            pdf_text = "scanned PDF"
        else:
            pdf_text = get_pdf_text(file_path)

        if pdf_text is not None:
            item['text'] = pdf_text

            return item


def worker(_):
    """
    run multi-threads to process PDFs
    :return add results in m_date_extractor module:
    """
    items = shared_text_extractor_pool.get_items(count=50)
    if len(items) > 0:
        logging.info(f'converting {len(items)} PDFs')

        mypool = ThreadPool(min(MAX_THREADS, len(items)))
        results = mypool.map(pdf2text, items)
        update_date_extractor_pool(results)

        logging.info(f'added {len(results)} documents to date_extractor pool')

        mypool.close()
        mypool.join()


def run_workers():
    """
    run workers over each processor
    :return:
    """
    processes = min(multiprocessing.cpu_count() - 1, MAX_PROCESSORS)
    mypool = multiprocessing.Pool(processes)
    mypool.map(worker, range(processes))

    mypool.close()
    mypool.join()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='eCrawler - Text Extractor')

    # Add an argument
    parser.add_argument('-timeout', type=int, default=0, help="max run time in minutes, 0 for infinity running")
    parser.add_argument('--download_path', type=get_full_path, default="downloaded/", required=True,
                        help="path of a PDFs location")
    parser.add_argument('--threads', type=int, default=2, help="number of maximum concurrent on each processor")
    parser.add_argument('--processors', type=int, default=2, help="number of maximum processors")

    # Parse the argument
    args = parser.parse_args()

    MAX_TIMEOUT = args.timeout
    MAX_THREADS = args.threads
    MAX_PROCESSORS = args.threads
    DOWNLOAD_PATH = args.download_path
    ROOT_PATH = os.getcwd()

    initial()

    st = time.time()
    while shared_text_extractor_pool.size() > 0:
        run_workers()

        running_time = (time.time() - st) / 60
        if running_time > MAX_TIMEOUT != 0:
            logging.info("Exit - time over")
            break

    logging.info("Exit - Done")
