import os
from os.path import dirname, abspath

IN_TEST = os.environ.get('CU_TESTENV')

DATA_DIR = dirname(dirname(dirname(abspath(__file__)))) + "/data"
DATA_RAW_DIR = DATA_DIR + "/raw"

DATA_WIKI_DIR = DATA_DIR + "/wiki-search"

# wikipedia search classifier
DATA_WIKI_SEARCH_FILENAME = DATA_WIKI_DIR + '/instructor-search-results.json'
DATA_WIKI_SEARCH_TRAIN_FILENAME = DATA_WIKI_DIR + '/instructor-search-results.train.json'
DATA_WIKI_SEARCH_MODEL_FILENAME = DATA_WIKI_DIR + '/instructor-search-results.model'

# wikipedia articles classifier
DATA_WIKI_ARTICLE_FILENAME = DATA_WIKI_DIR + '/instructor-article.json'
DATA_WIKI_ARTICLE_TRAIN_FILENAME = DATA_WIKI_DIR + '/instructor-article.train.json'
DATA_WIKI_ARTICLE_MODEL_FILENAME = DATA_WIKI_DIR + '/instructor-article.model'

TEST_DATA_DIR = dirname(dirname(dirname(abspath(__file__)))) + "/test-data"


def get_testcase_dir(testcase: str):
    assert len(testcase) > 0
    return TEST_DATA_DIR + "/" + testcase
