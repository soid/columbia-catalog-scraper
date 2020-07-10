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
import requests
session = requests.Session()
recorder = Betamax(session)


class TestCatalogSpider(BetamaxTestCase):
    def test_parse_department_listing(self):
        url = "http://www.columbia.edu/cu/bulletin/uwb/sel/COMS_Fall2020.html"
        response = self.session.get(url)
        scrapy_response = HtmlResponse(body=response.content, url=url, request=Request(url))
        catalog = CatalogSpider()
        result_generator = catalog.parse_department_listing(scrapy_response)
        results = list(result_generator)

        assert len(results) > 2

        result = results[0]
        assert result['department_code'] == 'COMS'
        assert result['term_month'] == 'Fall'
        assert result['term_year'] == '2020'

        result = results[1]
        assert type(result) == Request
        assert result.url == 'http://www.columbia.edu//cu/bulletin/uwb/subj/COMS/W1002-20203-001/'

    def _get_class_listing(self, url):
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
        return results

    def test_parse_class_listing(self):
        results = self._get_class_listing("http://www.columbia.edu/cu/bulletin/uwb/subj/COMS/W4156-20203-001/")

        assert len(results) == 1
        result = results[0]
        # print(result)
        assert result['class_id'] == 'W4156-20203-001'
        assert result['department_listing'] == {'department_code': 'COMS',
                                                'raw_content': 'N/A',
                                                'term_month': 'Fall',
                                                'term_year': '2020'}
        assert len(result['raw_content']) > 100
        assert result['instructor'] == 'Gail E Kaiser'
        assert set(result.keys()) == {'class_id', 'department_listing', 'raw_content', 'instructor'}

    def test_parse_class_instructor(self):
        # TODO ideally this test should be (auto)updated every semester in order to ensure everything is working.
        urls = [  # get from scrapy log in VIM: %s/^.\+<GET \(.\+\)>.\+$/\1/g
            'http://www.columbia.edu//cu/bulletin/uwb/subj/MUSI/V3129-20201-001/',
            'http://www.columbia.edu//cu/bulletin/uwb/subj/PSYC/X3606-20201-003/',
            'http://www.columbia.edu//cu/bulletin/uwb/subj/ENGL/C1010-20201-631/',
            'http://www.columbia.edu//cu/bulletin/uwb/subj/GREK/V3998-20201-021/',
            'http://www.columbia.edu//cu/bulletin/uwb/subj/HUMA/F1002-20201-059/',
            'http://www.columbia.edu//cu/bulletin/uwb/subj/ECON/W4913-20203-004/',
            'http://www.columbia.edu//cu/bulletin/uwb/subj/JOUR/J6044-20201-009/',
            'http://www.columbia.edu//cu/bulletin/uwb/subj/COMS/W1004-20202-001/',
            'http://www.columbia.edu//cu/bulletin/uwb/subj/LATN/V3012-20203-001/',
            'http://www.columbia.edu//cu/bulletin/uwb/subj/POLS/W3671-20202-001/',
            'http://www.columbia.edu//cu/bulletin/uwb/subj/ON__/N952_-20203-004/',
            'http://www.columbia.edu//cu/bulletin/uwb/subj/CHEN/E4231-20203-001/',
            'http://www.columbia.edu//cu/bulletin/uwb/subj/MD__/N31S_-20203-002/',
            'http://www.columbia.edu//cu/bulletin/uwb/subj/PHYT/M8126-20201-081/',
            'http://www.columbia.edu//cu/bulletin/uwb/subj/ENVP/U9229-20202-005/',
            'http://www.columbia.edu//cu/bulletin/uwb/subj/BIET/K5340-20201-H03/',
            'http://www.columbia.edu//cu/bulletin/uwb/subj/SWHL/W2101-20203-001/',
            'http://www.columbia.edu//cu/bulletin/uwb/subj/DROM/B9119-20201-001/',
            'http://www.columbia.edu//cu/bulletin/uwb/subj/MDES/W4510-20203-001/',
            'http://www.columbia.edu//cu/bulletin/uwb/subj/ENGL/X3996-20201-009/',
            'http://www.columbia.edu//cu/bulletin/uwb/subj/IENT/N0101-20202-002/',
            'http://www.columbia.edu//cu/bulletin/uwb/subj/ENGL/W4621-20201-001/',
            'http://www.columbia.edu//cu/bulletin/uwb/subj/COMM/K5025-20203-003/',
            'http://www.columbia.edu//cu/bulletin/uwb/subj/BIOL/X3360-20201-001/',
            'http://www.columbia.edu//cu/bulletin/uwb/subj/ENGL/C1010-20201-425/',
            'http://www.columbia.edu//cu/bulletin/uwb/subj/SOCW/T660B-20201-D35/',
            'http://www.columbia.edu//cu/bulletin/uwb/subj/MD__/N17P_-20203-002/',
            'http://www.columbia.edu//cu/bulletin/uwb/subj/HM__/N037_-20203-002/',
            'http://www.columbia.edu//cu/bulletin/uwb/subj/NURS/N8840-20201-001/',
            'http://www.columbia.edu//cu/bulletin/uwb/subj/OCCT/M8992-20202-006/',
            'http://www.columbia.edu//cu/bulletin/uwb/subj/PHYS/X2020-20201-001/',
            'http://www.columbia.edu//cu/bulletin/uwb/subj/JPNS/W4008-20201-001/',
            'http://www.columbia.edu//cu/bulletin/uwb/subj/OCCT/M8106-20202-006/',
            'http://www.columbia.edu//cu/bulletin/uwb/subj/RELI/S2305-20202-002/',
            'http://www.columbia.edu//cu/bulletin/uwb/subj/PHED/C1001-20203-046/',
            'http://www.columbia.edu//cu/bulletin/uwb/subj/ENGI/E5009-20203-031/',
            'http://www.columbia.edu//cu/bulletin/uwb/subj/SCNC/C1100-20201-011/',
            'http://www.columbia.edu//cu/bulletin/uwb/subj/THTR/V3004-20203-001/',
            'http://www.columbia.edu//cu/bulletin/uwb/subj/EESC/X3017-20203-001/',
            'http://www.columbia.edu//cu/bulletin/uwb/subj/ELEN/E9002-20201-027/',
            'http://www.columbia.edu//cu/bulletin/uwb/subj/ASST/X3999-20201-001/',
            'http://www.columbia.edu//cu/bulletin/uwb/subj/GE__/N701_-20203-011/',
            'http://www.columbia.edu//cu/bulletin/uwb/subj/GREK/V3998-20201-021/',
            'http://www.columbia.edu//cu/bulletin/uwb/subj/ANTH/G6070-20203-001/',
            'http://www.columbia.edu//cu/bulletin/uwb/subj/PHYS/W1404-20203-006/',
            'http://www.columbia.edu//cu/bulletin/uwb/subj/HUMA/F1002-20201-059/',
            'http://www.columbia.edu//cu/bulletin/uwb/subj/NMED/K5996-20202-004/',
            'http://www.columbia.edu//cu/bulletin/uwb/subj/PSYC/W3950-20203-004/',
            'http://www.columbia.edu//cu/bulletin/uwb/subj/ECON/W4913-20203-004/',
            'http://www.columbia.edu//cu/bulletin/uwb/subj/ELEN/E3998-20201-022/',
            'http://www.columbia.edu//cu/bulletin/uwb/subj/POLS/W3961-20203-002/',
            'http://www.columbia.edu//cu/bulletin/uwb/subj/POPF/P8688-20203-001/',
            'http://www.columbia.edu//cu/bulletin/uwb/subj/TMGT/K5136-20202-D01/',
            'http://www.columbia.edu//cu/bulletin/uwb/subj/LAW_/L6121-20201-029/',
            'http://www.columbia.edu//cu/bulletin/uwb/subj/BMEN/E3998-20201-005/',
            'http://www.columbia.edu//cu/bulletin/uwb/subj/ARCH/A4432-20201-001/',
            'http://www.columbia.edu//cu/bulletin/uwb/subj/ENGL/Z0015-20202-001/',
            'http://www.columbia.edu//cu/bulletin/uwb/subj/ANTH/W3997-20203-012/',
            'http://www.columbia.edu//cu/bulletin/uwb/subj/ECPS/W4921-20201-001/',
            'http://www.columbia.edu//cu/bulletin/uwb/subj/SOCW/T7305-20201-003/',
        ]

        instructors = []
        for url in urls:
            # print('Testing URL:', url)
            results = self._get_class_listing(url)
            assert len(results) == 1
            result = results[0]
            instructors.append(result['instructor'])
        print(instructors)
        assert instructors == ['Julia Doe', 'Tovah Klein', 'Kristie Schlauraff', 'Nancy Worman', 'Michael S Paulson',
                               None, 'Rob Gebeloff', 'Adam H Cannon', 'Gareth Williams', 'Michael C Beckley', None,
                               'Daniel Esposito', None, None, 'Robert A Cook', 'Arthur Kuflik', 'Abdul Nanji',
                               'Assaf Zeevi', 'Naama Harel', 'Peter Platt', 'Stephane Goldsand', "Robert G O'Meally",
                               'Don Waisanen', 'John Glendinning', 'Rachel P Finn-Lohmann', 'David Bolt', None, None,
                               'Maribeth L Massie', 'Dawn Nilsen', 'Janna Levin', 'David B Lurie', 'Dawn Nilsen', None,
                               None, 'Hyoseon Lee', 'Caroline W Leland', 'Sharon Fogarty', 'Elizabeth Cook',
                               'John Wright', 'Rachel McDermott', None, 'Nancy Worman', None, 'Emlyn W Hughes',
                               'Michael S Paulson', 'Maura L Spiegel', 'Geraldine Downey', None, 'James Teherani',
                               'Virginia Page Fortna', 'Monette Zard', 'Arthur Langer', 'Alberto Rodriguez',
                               'Andreas Hielscher', 'Lindy Roy', 'Jeanne K Lambert', 'Rosalind Morris',
                               'W. Bentley Macleod', 'Ericka Hart']
