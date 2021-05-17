#!/bin/bash
set -e -x

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

export CU_TESTENV=true
export PYTHONPATH=$PYTHONPATH:$DIR:$DIR/columbia_crawler

if [ $# -eq 0 ]; then
    # crawler tests
    pushd $DIR/columbia_crawler
    scrapy check
    python3 -m doctest -v columbia_crawler/pipelines.py
    python3 -m doctest -v columbia_crawler/util.py

    python3 -m unittest columbia_crawler/spiders/test_catalog.py
    python3 -m unittest columbia_crawler/test_pipelines.py
    popd

    # other tests
    # python3 -m doctest -v scripts/wiki_search_train.py
else
    # run command with setup environment if arguments provided
    $*
fi

