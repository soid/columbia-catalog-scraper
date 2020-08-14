#!/usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

export PYTHONPATH=$PYTHONPATH:$DIR:$DIR/columbia_crawler

cd $DIR/columbia_crawler
scrapy shell "$1"
