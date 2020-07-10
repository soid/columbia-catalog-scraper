#!/bin/bash -e

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

cd $DIR/columbia_crawler
python3 -m doctest -v columbia_crawler/util.py
scrapy check
