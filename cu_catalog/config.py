import configparser
import logging
import os
from os.path import dirname, abspath

IN_TEST = os.environ.get('CU_TESTENV')

# Read defaults.cfg and optional config.cfg
conf_parser = configparser.ConfigParser()
this_dir = dirname(abspath(__file__))
config_files = [this_dir + '/defaults.cfg']
if not IN_TEST:
    config_files += [this_dir + '/config.cfg']
conf_parser.read(config_files)
config = conf_parser['DEFAULT']
config['project'] = dirname(dirname(abspath(__file__)))


# read config values
DATA_DIR = config['DATA_DIR']
HTTPCACHE_DIR = config['HTTPCACHE_DIR']
LOG_LEVEL = config['LOG_LEVEL']
LOG_DIR = config['LOG_DIR']
os.makedirs(LOG_DIR, exist_ok=True)


def get_logger(logger_name: str, log_filename: str):
    logger = logging.getLogger(logger_name)
    log_format = '[%(threadName)s] %(levelname)s %(asctime)s - %(message)s'
    logging.basicConfig(level=LOG_LEVEL,
                        format=log_format)
    log_file_handler = logging.FileHandler(LOG_DIR + '/' + log_filename)
    log_file_handler.setFormatter(logging.Formatter(log_format))
    logger.addHandler(log_file_handler)
    logger.setLevel(LOG_LEVEL)
    return logger


if IN_TEST:
    # create temporary data directory for test
    import atexit
    import tempfile
    import shutil

    DATA_DIR_REAL = DATA_DIR
    DATA_DIR_TEST = tempfile.mkdtemp()
    atexit.register(lambda: shutil.rmtree(DATA_DIR_TEST))

    # copy files needed in data directory for tests
    shutil.copytree(DATA_DIR + "/wiki-search", DATA_DIR_TEST + '/wiki-search')
    DATA_DIR = DATA_DIR_TEST
    config['DATA_DIR'] = DATA_DIR

HTTP_CACHE_ENABLED = config.getboolean('HTTP_CACHE_ENABLED')

# data directories
DATA_RAW_DIR = config['DATA_RAW_DIR']
DATA_WIKI_DIR = config['DATA_WIKI_DIR']

# wikipedia search classifier
DATA_WIKI_SEARCH_FILENAME = config['DATA_WIKI_SEARCH_FILENAME']
DATA_WIKI_SEARCH_TRAIN_FILENAME = config['DATA_WIKI_SEARCH_TRAIN_FILENAME']
DATA_WIKI_SEARCH_MODEL_FILENAME = config['DATA_WIKI_SEARCH_MODEL_FILENAME']

# wikipedia articles classifier
DATA_WIKI_ARTICLE_FILENAME = config['DATA_WIKI_ARTICLE_FILENAME']
DATA_WIKI_ARTICLE_TRAIN_FILENAME = config['DATA_WIKI_ARTICLE_TRAIN_FILENAME']
DATA_WIKI_ARTICLE_MODEL_FILENAME = config['DATA_WIKI_ARTICLE_MODEL_FILENAME']

# scraped classes data files
DATA_CLASSES_DIR = config['DATA_CLASSES_DIR']
DATA_CLASSES_ENROLLMENT_DIR = config['DATA_CLASSES_ENROLLMENT_DIR']
DATA_INSTRUCTORS_DIR = config['DATA_INSTRUCTORS_DIR']
DATA_INSTRUCTORS_JSON = DATA_INSTRUCTORS_DIR + '/instructors.json'
DATA_INSTRUCTORS_CSV = DATA_INSTRUCTORS_DIR + '/instructors.csv'

# internal database for keeping track e.g. when instructor was last searched in wikipedia
DATA_INTERNAL_DB_DIR = config['DATA_INTERNAL_DB_DIR']
DATA_INSTRUCTORS_INTERNAL_INFO_JSON = config['DATA_INTERNAL_DB_DIR'] + "/instructors-internal.json"

# unit tests related data
TEST_DATA_DIR = dirname(dirname(abspath(__file__))) + "/test-data"


def get_testcase_dir(testcase: str):
    assert len(testcase) > 0
    return TEST_DATA_DIR + "/" + testcase
