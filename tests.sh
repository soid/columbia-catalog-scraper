#!/bin/bash
set -e -x

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

export CU_TESTENV=true
export PYTHONPATH=$PYTHONPATH:$DIR:$DIR/columbia_crawler

# crawler tests
pushd $DIR/columbia_crawler
scrapy check
python3 -m doctest -v columbia_crawler/pipelines.py
python3 -m doctest -v columbia_crawler/util.py

python3 -m unittest columbia_crawler/spiders/test_catalog.py
popd

# other tests
python3 -m doctest -v cu_catalog/models/wiki_search.py
python3 -m doctest -v scripts/wiki_search_train.py
