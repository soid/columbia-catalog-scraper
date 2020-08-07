import os
from os.path import dirname, abspath

DATA_DIR = dirname(dirname(dirname(abspath(__file__)))) + "/data"
DATA_RAW_DIR = DATA_DIR + "/raw"

DATA_WIKI_DIR = DATA_DIR + "/wiki-search"
DATA_WIKI_FILENAME = DATA_WIKI_DIR + '/instructor-search-results.json'
DATA_WIKI_TRAIN_FILENAME = DATA_WIKI_DIR + '/instructor-search-results.train.json'

TEST_DATA_DIR = dirname(dirname(dirname(abspath(__file__)))) + "/test-data"

IN_TEST = os.environ.get('CU_TESTENV')


def get_testcase_dir(testcase: str):
    assert len(testcase) > 0
    return TEST_DATA_DIR + "/" + testcase
