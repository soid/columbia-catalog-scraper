#!/bin/bash -e -x

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

export CU_TESTENV=true

cd $DIR/columbia_crawler
python3 -m doctest -v columbia_crawler/pipelines.py
python3 -m doctest -v columbia_crawler/util.py

python3 -m unittest columbia_crawler/spiders/test_catalog.py

scrapy check
