import scrapy
from scrapy import Request
from urllib.parse import urlparse
import logging

from columbia_crawler import util
from columbia_crawler.items import ColumbiaDepartmentListing

logger = logging.getLogger(__name__)


class CatalogSpider(scrapy.Spider):
    name = 'catalog'

    start_urls = ["http://www.columbia.edu/cu/bulletin/uwb/sel/departments.html"]

    custom_settings = {
        'HTTPCACHE_ENABLED': True,
        'ITEM_PIPELINES': {
            'columbia_crawler.pipelines.StoreRawListeningPipeline': 300,
        }
    }

    """ Starting with department list, crawl all listings by each department.
    """
    def parse(self, response):
        logger.info('Parsing URL=%s Status=%d', response.url, response.status)

        domain = '{uri.scheme}://{uri.netloc}/'.format(uri=urlparse(response.url))

        for dep_url in response.css('a::attr(href)').getall():
            if dep_url.startswith("/cu/bulletin/uwb/sel/"):
                follow_url = domain + dep_url
                yield Request(follow_url, callback=self.parse_department_listing)

    def parse_department_listing(self, response):
        logger.info('Parsing department URL=%s Status=%d', response.url, response.status)
        filename = response.url.split('/')[-1]
        filename2 = filename.split('.')[0]  # no .html extension
        department_code, term_url = filename2.split('_')
        term_month, term_year = util.split_term(term_url)

        yield ColumbiaDepartmentListing(
            department_code=department_code,
            term=term_month,
            term_year=term_year,
            raw_content=response.body_as_unicode()
        )
