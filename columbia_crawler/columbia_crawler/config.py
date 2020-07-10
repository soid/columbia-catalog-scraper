from os.path import dirname, abspath

DATA_DIR = dirname(dirname(dirname(abspath(__file__)))) + "/data"
DATA_RAW_DIR = DATA_DIR + "/raw"

TEST_DATA_DIR = dirname(dirname(dirname(abspath(__file__)))) + "/test-data"


def get_test_data_dir(testcase: str):
    assert len(testcase) > 0
    return TEST_DATA_DIR + "/" + testcase
