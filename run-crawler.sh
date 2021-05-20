#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

export PYTHONPATH=$PYTHONPATH:$DIR

usage()
{
cat << EOF
usage: $0 [options]

Run scrapy crawler.

OPTIONS:
   -h      Show this message
   -t      Test run (limited number of downloads)
   -p      Production run
EOF
}

cd $DIR/columbia_crawler
while getopts “htp” OPTION
do
    case $OPTION in
        h)
            usage
            exit 1
            ;;
        t)
            scrapy crawl catalog -a test_run=True
            scrapy crawl wiki_search -a test_run=True
            scrapy crawl culpa_search -a test_run=True
            exit 0
            ;;
        p)
            ../run-script.sh columbia_crawler/run_spiders.py
            exit 0
            ;;
        ?)
            usage
            exit 1
            ;;
    esac
done

usage
exit 1
