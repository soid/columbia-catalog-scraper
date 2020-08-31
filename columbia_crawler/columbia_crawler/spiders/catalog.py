import logging
import re
from urllib.parse import urlparse

import scrapy
from scrapy import Request

from columbia_crawler import util
from columbia_crawler.items import ColumbiaDepartmentListing, ColumbiaClassListing
from columbia_crawler.spiders.culpa_search import CulpaSearch
from columbia_crawler.spiders.wiki_search import WikiSearch

logger = logging.getLogger(__name__)


class CatalogSpider(scrapy.Spider, WikiSearch, CulpaSearch):
    name = 'catalog'

    start_urls = ["http://www.columbia.edu/cu/bulletin/uwb/sel/departments.html"]

    custom_settings = {
        'HTTPCACHE_ENABLED': True,
        # 'HTTPCACHE_ENABLED': False,
        'ITEM_PIPELINES': {
            'columbia_crawler.pipelines.StoreRawListeningPipeline': 300,
            'columbia_crawler.pipelines.StoreWikiSearchResultsPipeline': 400,
            'columbia_crawler.pipelines.StoreClassPipeline': 500,
        }
    }

    empty_string = re.compile("^[ \\n]*$")

    def get_domain(self, response):
        return '{uri.scheme}://{uri.netloc}/'.format(uri=urlparse(response.url))

    def parse(self, response):
        """ Starting with department list, crawl all listings by each department.

        @url http://www.columbia.edu/cu/bulletin/uwb/sel/departments.html
        @returns items 0 0
        @returns requests 300 500
        """
        logger.info('Parsing URL=%s Status=%d', response.url, response.status)

        test_run = getattr(self, 'test_run', False)

        i = 0
        for dep_url in response.css('a::attr(href)').getall():
            if dep_url.startswith("/cu/bulletin/uwb/sel/"):
                follow_url = self.get_domain(response) + dep_url
                yield Request(follow_url, callback=self.parse_department_listing)
                i += 1
                if test_run and i > 20:
                    break

    def parse_department_listing(self, response):
        logger.info('Parsing department URL=%s Status=%d', response.url, response.status)
        filename = response.url.split('/')[-1]
        filename2 = filename.split('.')[0]  # no .html extension
        department_code, term_url = filename2.split('_')
        term_month, term_year = util.split_term(term_url)

        department_listing = ColumbiaDepartmentListing(
            department_code=department_code,
            term_month=term_month,
            term_year=term_year,
            raw_content=response.body_as_unicode()
        )
        yield department_listing

        for class_url in response.css('a::attr(href)').getall():
            if class_url.startswith("/cu/bulletin/uwb/subj/"):
                follow_url = self.get_domain(response) + class_url
                yield Request(follow_url, callback=self.parse_class_listing,
                              meta={
                                  'department_listing': department_listing,
                              })

    def parse_class_listing(self, response):
        """ Starting with department list, crawl all listings by each department.

        @url http://www.columbia.edu/cu/bulletin/uwb/subj/COMS/W3157-20203-001/
        @returns items 1 1
        @returns requests 2 2
        """
        logger.info('Parsing class from department %s URL=%s Status=%d',
                    ColumbiaDepartmentListing.get_from_response_meta(response)['department_code'],
                    response.url, response.status)

        class_listing = ColumbiaClassListing.get_from_response(response)
        yield class_listing

        if class_listing['instructor']:
            yield self._follow_culpa_instructor(class_listing['instructor'],
                                                ColumbiaDepartmentListing.get_from_response_meta(response))
            yield self._follow_search_wikipedia_instructor(class_listing['instructor'], 
                                                           class_listing)
