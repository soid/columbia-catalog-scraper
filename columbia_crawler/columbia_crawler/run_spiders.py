import pprint
import time
from twisted.internet import reactor, defer
from scrapy.crawler import CrawlerRunner
from scrapy.utils.log import configure_logging
import logging

from columbia_crawler.spiders.catalog import CatalogSpider
from columbia_crawler.spiders.wiki_search import WikiSearchSpider
from scrapy.utils.project import get_project_settings

settings = get_project_settings()
settings.update({
    "LOG_FILE": "columbia_crawlers.log"
})

configure_logging(settings)
logger = logging.getLogger()
start = time.time()
logger.info("Starting all spiders")

runner = CrawlerRunner(settings)


@defer.inlineCallbacks
def crawl():
    crawler_catalog = runner.create_crawler(CatalogSpider)
    yield runner.crawl(crawler_catalog)
    crawler_wiki_search = runner.create_crawler(WikiSearchSpider)
    yield runner.crawl(crawler_wiki_search)
    reactor.stop()
    logger.info("Catalog crawling results:\n%s", pprint.pformat(crawler_catalog.stats.get_stats()))
    logger.info("Wikipedia crawling results:\n%s", pprint.pformat(crawler_wiki_search.stats.get_stats()))


crawl()

reactor.run()
end = time.time()
secs = end - start
logger.info("Finished all spiders. Elapsed time: %d secs (~ " + str(int(secs / 60)) + " minutes)", secs)
