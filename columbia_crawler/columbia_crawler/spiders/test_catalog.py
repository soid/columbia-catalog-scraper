from betamax import Betamax
from betamax.fixtures.unittest import BetamaxTestCase
from scrapy import Request
from scrapy.http import HtmlResponse
import os.path

from columbia_crawler.items import ColumbiaDepartmentListing
from columbia_crawler.spiders.catalog import CatalogSpider

with Betamax.configure() as config:
    # where betamax will store cassettes (http responses):
    config.cassette_library_dir = os.path.dirname(__file__) + '/cassettes'
    config.preserve_exact_body_bytes = True


# Recording Cassettes:
# import requests
# session = requests.Session()
# recorder = Betamax(session)
#
# with recorder.use_cassette('our-first-recorded-session'):
#     session.get('https://httpbin.org/get')


class TestCatalogSpider(BetamaxTestCase):
    def test_parse_department_listing(self):
        url = "http://www.columbia.edu/cu/bulletin/uwb/sel/COMS_Fall2020.html"
        response = self.session.get(url)
        scrapy_response = HtmlResponse(body=response.content, url=url, request=Request(url))
        catalog = CatalogSpider()
        result_generator = catalog.parse_department_listing(scrapy_response)
        results = list(result_generator)

        print(results)
        assert len(results) > 2

        result = results[0]
        assert result['department_code'] == 'COMS'
        assert result['term_month'] == 'Fall'
        assert result['term_year'] == '2020'

        result = results[1]
        assert type(result) == Request
        assert result.url == 'http://www.columbia.edu//cu/bulletin/uwb/subj/COMS/W1002-20203-001/'

    def test_parse_class_listing(self):
        url = "http://www.columbia.edu/cu/bulletin/uwb/subj/COMS/W4156-20203-001/"
        response = self.session.get(url)
        department_listing = ColumbiaDepartmentListing(
            department_code="COMS",
            term_month="Fall",
            term_year="2020",
            raw_content="N/A"
        )
        scrapy_response = HtmlResponse(body=response.content, url=url,
                                       request=Request(url, meta={'department_listing': department_listing}))
        catalog = CatalogSpider()
        result_generator = catalog.parse_class_listing(scrapy_response)
        results = list(result_generator)

        assert len(results) == 1
        result = results[0]
        assert result['class_id'] == 'W4156-20203-001'
        assert result['department_listing'] == {'department_code': 'COMS',
                                                'raw_content': 'N/A',
                                                'term_month': 'Fall',
                                                'term_year': '2020'}
        assert len(result['raw_content']) > 100
        assert set(result.keys()) == {'class_id', 'department_listing', 'raw_content'}
