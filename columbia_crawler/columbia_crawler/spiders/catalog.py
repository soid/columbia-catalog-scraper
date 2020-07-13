import re
import urllib

import scrapy
from scrapy import Request
from urllib.parse import urlparse
import logging

from columbia_crawler import util
from columbia_crawler.items import ColumbiaDepartmentListing, ColumbiaClassListing, CulpaInstructor

logger = logging.getLogger(__name__)


class CatalogSpider(scrapy.Spider):
    name = 'catalog'

    start_urls = ["http://www.columbia.edu/cu/bulletin/uwb/sel/departments.html"]

    custom_settings = {
        'HTTPCACHE_ENABLED': True,
        # 'HTTPCACHE_ENABLED': False,
        'ITEM_PIPELINES': {
            'columbia_crawler.pipelines.StoreRawListeningPipeline': 300,
        }
    }

    def get_domain(self, response):
        return '{uri.scheme}://{uri.netloc}/'.format(uri=urlparse(response.url))

    def parse(self, response):
        """ Starting with department list, crawl all listings by each department.

        @url http://www.columbia.edu/cu/bulletin/uwb/sel/departments.html
        @returns items 0 0
        @returns requests 400 500
        @scrapes Title Author Year Price
        """
        logger.info('Parsing URL=%s Status=%d', response.url, response.status)

        for dep_url in response.css('a::attr(href)').getall():
            if dep_url.startswith("/cu/bulletin/uwb/sel/"):
                follow_url = self.get_domain(response) + dep_url
                yield Request(follow_url, callback=self.parse_department_listing)

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
        logger.info('Parsing class from department %s URL=%s Status=%d',
                    response.meta.get('department_listing')['department_code'], response.url, response.status)
        # TODO parse the class listing into fields
        class_id = [p for p in response.url.split('/') if len(p) > 0][-1]
        content = [tr.css('td *::text').getall() for tr in response.css('tr')]

        # Parse instructor name
        tmp = [c for c in content if c[0] == 'Instructor']
        if len(tmp) > 0:
            instructor = tmp[0][1]
            instructor = re.sub(r'[\s-]+$', '', instructor)  # clean up
            yield self._follow_culpa_instructor(instructor, response.meta.get('department_listing'))
        else:
            instructor = None

        yield ColumbiaClassListing(
            class_id=class_id,
            instructor=instructor,
            department_listing=response.meta.get('department_listing'),
            raw_content=response.body_as_unicode()
        )

    # Parsing CULPA instructors

    def _follow_culpa_instructor(self, instructor, department_listing):
        url = 'http://culpa.info/search?utf8=✓&search=' \
              + urllib.parse.quote_plus(instructor) + '&commit=Search'
        return Request(url, callback=self.parse_culpa_search_instructor,
                       meta={
                           'department_listing': department_listing,
                           'instructor': instructor})

    def parse_culpa_search_instructor(self, response):
        found = response.css('.search_results .box tr td:first-child')
        if found:
            if len(found) > 1:
                logger.warning("More than 1 result for '%s' from '%s' on CULPA",
                               response.meta.get('instructor'),
                               response.meta.get('department_listing')['department_code'])
            link = found.css('a::attr(href)').get()
            url = 'http://culpa.info' + link
            nugget = found.css('img.nugget::attr(alt)').get()
            yield Request(url, callback=self.parse_culpa_instructor,
                          meta={**response.meta,
                                'link': link,
                                'nugget': nugget})

    def parse_culpa_instructor(self, response):
        # Idea: we could classify reviews sentiment if we capture review texts here

        nugget = None
        if response.meta.get('nugget'):
            if response.meta.get('nugget').upper().startswith("GOLD"):
                nugget = CulpaInstructor.NUGGET_GOLD
            if response.meta.get('nugget').upper().startswith("SILVER"):
                nugget = CulpaInstructor.NUGGET_SILVER

        yield CulpaInstructor(
            name=response.meta.get('instructor'),
            link=response.meta.get('link'),
            reviews_count=len(response.css('div.professor .review')),
            nugget=nugget
        )
