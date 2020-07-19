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

    empty_string = re.compile("^[ \\n]*$")

    def get_domain(self, response):
        return '{uri.scheme}://{uri.netloc}/'.format(uri=urlparse(response.url))

    def parse(self, response):
        """ Starting with department list, crawl all listings by each department.

        @url http://www.columbia.edu/cu/bulletin/uwb/sel/departments.html
        @returns items 0 0
        @returns requests 400 500
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
        """ Starting with department list, crawl all listings by each department.

        @url http://www.columbia.edu/cu/bulletin/uwb/subj/COMS/W3157-20203-001/
        @returns items 1 1
        @returns requests 1 1
        """
        logger.info('Parsing class from department %s URL=%s Status=%d',
                    self._get_department_listing(response), response.url, response.status)
        # TODO parse the class listing into fields
        class_id = [p for p in response.url.split('/') if len(p) > 0][-1]
        content = [tr.css('td *::text').getall() for tr in response.css('tr')]

        content = [tr.css('td') for tr in response.css('tr')]
        content = filter(lambda x: len(x) > 1, content)  # non-fields have only 1 td tag

        # parse all fields into a dict
        fields = {field_name.css('::text').get(): field_value for field_name, field_value in content}

        # Parse fields
        def _get_field(field_name, first_line_only=False):
            if field_name in fields:
                v = fields[field_name].css('::text')
                return v.get() if first_line_only else "\n".join(v.getall())

        instructor = _get_field('Instructor', first_line_only=True)
        if instructor:
            instructor = re.sub(r'[\s-]+$', '', instructor)  # clean up
            yield self._follow_culpa_instructor(instructor, self._get_department_listing(response))

        # TODO date &time
        datetime_ = None

        open_to = _get_field("Open To")
        if open_to:
            open_to = [s.strip() for s in open_to.split(',')]

        course_descr = _get_field("Course Description")
        points = _get_field("Points")
        class_type = _get_field("Type")
        method_of_instruction = _get_field("Method of Instruction")
        department = _get_field("Department")
        call_number = _get_field("Call Number")
        campus = _get_field("Campus")

        # Parse instructor name

        yield ColumbiaClassListing(
            class_id=class_id,
            instructor=instructor,
            course_descr=course_descr,
            datetime=datetime_,
            points=points,
            type=class_type,
            department=department,
            call_number=call_number,
            open_to=open_to,
            campus=campus,
            method_of_instruction=method_of_instruction,
            department_listing=self._get_department_listing(response),
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
                               self._get_department_listing(response)['department_code'])
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

    # End of Parsing CULPA instructors

    # helpers
    def _get_department_listing(self, response):
        return response.meta.get('department_listing',
                                 ColumbiaDepartmentListing(
                                     department_code="TEST",
                                     term_month="testing",
                                     term_year="testing",
                                     raw_content="test content")
                                 )
