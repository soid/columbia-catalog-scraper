#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

export PYTHONPATH=$PYTHONPATH:$DIR

cd $DIR/columbia_crawler
scrapy crawl catalog --logfile columbia_crawler.log
