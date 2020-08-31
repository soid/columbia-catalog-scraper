import configparser
import os
from os.path import dirname, abspath

IN_TEST = os.environ.get('CU_TESTENV')

# Read defaults.cfg and optional config.cfg
conf_parser = configparser.ConfigParser()
this_dir = dirname(abspath(__file__))
config_files = [this_dir + '/defaults.cfg',
                this_dir + '/config.cfg']
conf_parser.read(config_files)
config = conf_parser['DEFAULT']
config['project'] = dirname(dirname(abspath(__file__)))


# read config values
DATA_DIR = config['DATA_DIR']

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
DATA_INSTRUCTORS_DIR = config['DATA_INSTRUCTORS_DIR']

# unit tests related data
TEST_DATA_DIR = dirname(dirname(abspath(__file__))) + "/test-data"


def get_testcase_dir(testcase: str):
    assert len(testcase) > 0
    return TEST_DATA_DIR + "/" + testcase
