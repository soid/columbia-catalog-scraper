import pprint
import shutil
import time
from twisted.internet import reactor, defer
from scrapy.crawler import CrawlerRunner
from scrapy.utils.log import configure_logging
import logging

from columbia_crawler.spiders.catalog import CatalogSpider
from columbia_crawler.spiders.culpa_search import CulpaSearchSpider
from columbia_crawler.spiders.wiki_search import WikiSearchSpider
from scrapy.utils.project import get_project_settings

from cu_catalog import config

settings = get_project_settings()
settings.update({
    "LOG_FILE": config.LOG_DIR + "/columbia_crawlers.log"
})

configure_logging(settings)
logger = logging.getLogger()
start = time.time()
logger.info("Starting all spiders")

runner = CrawlerRunner(settings)


# check disk space
total, used, free = shutil.disk_usage(config.DATA_CLASSES_DIR)
if free < 300*1024*1024:  # one gig
    logger.error("Not enough space on device to run the crawler. "
                 "Need 300MB. Available: %s Mb", free//(1024*1024))
    exit(3)


@defer.inlineCallbacks
def crawl():
    crawler_catalog = runner.create_crawler(CatalogSpider)
    yield runner.crawl(crawler_catalog)

    crawler_wiki_search = runner.create_crawler(WikiSearchSpider)
    yield runner.crawl(crawler_wiki_search)

    # crawler_culpa_search = runner.create_crawler(CulpaSearchSpider)
    # yield runner.crawl(crawler_culpa_search)

    reactor.stop()

    logger.info("Catalog crawling results:\n%s", pprint.pformat(crawler_catalog.stats.get_stats()))
    logger.info("Wikipedia crawling results:\n%s", pprint.pformat(crawler_wiki_search.stats.get_stats()))
    # logger.info("CULPA crawling results:\n%s", pprint.pformat(crawler_culpa_search.stats.get_stats()))


crawl()

reactor.run()
end = time.time()
secs = end - start
logger.info("Finished all spiders. Elapsed time: %d secs (~ " + str(int(secs / 60)) + " minutes)", secs)
