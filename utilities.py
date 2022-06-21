import logging
import os


def get_full_path(string):
    script_dir = os.path.dirname(__file__)
    return  os.path.normpath(os.path.join(script_dir, string))

def create_log_file(ROOT_PATH,log_file):
    log_file = os.path.join(ROOT_PATH, 'logs', log_file)
    logging.basicConfig(level=logging.INFO, filename=log_file, filemode='w+',
                        format='%(levelname)s:%(asctime)s: %(message)s', datefmt='%d-%b %H:%M')

    print(f"log file is created....\n{log_file}")