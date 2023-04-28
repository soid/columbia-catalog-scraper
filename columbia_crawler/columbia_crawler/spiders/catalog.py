import logging
import re
from urllib.parse import urlparse
from urllib.parse import urljoin

import scrapy
from scrapy import Request

from columbia_crawler import util
from columbia_crawler.items import ColumbiaDepartmentListing, ColumbiaClassListing
from cu_catalog import config

logger = logging.getLogger(__name__)


class CatalogSpider(scrapy.Spider):
    name = 'catalog'

    start_urls = ["http://www.columbia.edu/cu/bulletin/uwb/sel/departments.html"]

    custom_settings = {
        'HTTPCACHE_ENABLED': config.HTTP_CACHE_ENABLED,
        'HTTPCACHE_DIR': config.HTTPCACHE_DIR,
        'LOG_LEVEL': config.LOG_LEVEL,
        'ITEM_PIPELINES': {
            'columbia_crawler.pipelines.StoreRawListeningPipeline': 300,
            'columbia_crawler.pipelines.StoreClassPipeline': 500,
        }
    }

    empty_string = re.compile("^[ \\n]*$")

    def parse(self, response):
        """ Starting with department list, crawl all listings by each department.

        @url http://www.columbia.edu/cu/bulletin/uwb/sel/departments.html
        @returns items 0 0
        @returns requests 300 700
        """
        logger.info('Parsing URL=%s Status=%d', response.url, response.status)

        test_run = getattr(self, 'test_run', False)

        i = 0
        dep_url: str
        for dep_url in response.css('a::attr(href)').getall():
            if "/sel/" in dep_url:
                # resolve relative URL path
                follow_url = urljoin(response.url, dep_url)
                yield Request(follow_url, callback=self.parse_department_listing)
                i += 1
                if test_run and i > 20:
                    break

    def parse_department_listing(self, response):
        logger.debug('Parsing department URL=%s Status=%d', response.url, response.status)
        self.crawler.stats.inc_value('catalog_parsed_departments')
        filename = response.url.split('/')[-1]
        filename2 = filename.split('.')[0]  # no .html extension
        department_code, term_url = filename2.rsplit('_', maxsplit=1)
        term_month, term_year = util.split_term(term_url)

        department_listing = ColumbiaDepartmentListing(
            department_code=department_code,
            term_month=term_month,
            term_year=term_year,
            raw_content=response.text
        )
        yield department_listing

        for cls_section in response.xpath('//tr/td/a[contains(@href, "/subj/")]/../..'):
            class_url = cls_section.xpath('.//a[contains(@href, "/subj/")]/@href').get()
            follow_url = urljoin(response.url, class_url)
            yield Request(follow_url, callback=self.parse_class_listing,
                          meta={
                              'department_listing': department_listing,
                          })

    def parse_class_listing(self, response):
        """ Starting with department list, crawl all listings by each department.

        @url http://www.columbia.edu/cu/bulletin/uwb/subj/COMS/W3157-20203-001/
        @returns items 1 1
        @returns requests 0 0
        """
        logger.debug('Parsing class from department %s URL=%s Status=%d',
                    ColumbiaDepartmentListing.get_from_response_meta(response)['department_code'],
                    response.url, response.status)
        self.crawler.stats.inc_value('catalog_parsed_classes')

        class_listing = ColumbiaClassListing.get_from_response(response)
        yield class_listing
