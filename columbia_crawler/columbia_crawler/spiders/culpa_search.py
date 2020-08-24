import logging
import urllib
from scrapy import Request

from columbia_crawler.items import ColumbiaDepartmentListing, CulpaInstructor

logger = logging.getLogger(__name__)


class CulpaSearch:
    def _follow_culpa_instructor(self, instructor, department_listing):
        url = 'http://culpa.info/search?utf8=✓&search=' \
              + urllib.parse.quote_plus(instructor) + '&commit=Search'
        return Request(url, callback=self.parse_culpa_search_instructor,
                       meta={
                           'department_listing': department_listing,
                           'instructor': instructor})

    def parse_culpa_search_instructor(self, response):
        """
        @url http://culpa.info/search?utf8=%E2%9C%93&search=Ismail+C+Noyan&commit=Search
        @returns items 0 0
        @returns requests 1 1
        """
        found = response.css('.search_results .box tr td:first-child')
        if found:
            if len(found) > 1:
                logger.warning("More than 1 result for '%s' from '%s' on CULPA",
                               response.meta.get('instructor'),
                               ColumbiaDepartmentListing.get_from_response_meta(response)['department_code'])
            link = found.css('a::attr(href)').get()
            url = 'http://culpa.info' + link
            nugget = found.css('img.nugget::attr(alt)').get()
            yield Request(url, callback=self.parse_culpa_instructor,
                          meta={**response.meta,
                                'link': link,
                                'nugget': nugget})

    def parse_culpa_instructor(self, response):
        """
        @url http://culpa.info/professors/3126
        @returns items 1 1
        @returns requests 0 0
        """
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
            reviews_count=int(len(response.css('div.professor .review'))),
            nugget=nugget
        )
