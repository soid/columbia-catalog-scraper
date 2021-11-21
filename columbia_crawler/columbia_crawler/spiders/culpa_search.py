import datetime
import logging
from typing import List

import pandas as pd
import random
import urllib
import scrapy
from scrapy import Request
from parsel.selector import SelectorList

from columbia_crawler import util
from columbia_crawler.items import CulpaInstructor
from cu_catalog import config
from cu_catalog.models.util import words_match2

logger = logging.getLogger(__name__)


class CulpaSearchSpider(scrapy.Spider):
    name = 'culpa_search'
    #SITE = 'localhost:8801'
    SITE = 'culpa.info'

    custom_settings = {
        'HTTPCACHE_ENABLED': config.HTTP_CACHE_ENABLED,
        'HTTPCACHE_DIR': config.HTTPCACHE_DIR,
        'LOG_LEVEL': config.LOG_LEVEL,
        'ITEM_PIPELINES': {
            'columbia_crawler.pipelines.StoreCulpaSearchPipeline': 100,
        }
    }

    def __init__(self, *args, **kwargs):
        super(CulpaSearchSpider, self).__init__(*args, **kwargs)
        self.instructors_internal_db = None

    def start_requests(self):
        self.crawler.stats.set_value('culpa_searches', 0)
        self.crawler.stats.set_value('culpa_profiles_loaded', 0)

        df = pd.read_json(config.DATA_INSTRUCTORS_JSON, lines=True)

        # join internal db to store last check and don't check too often
        self.instructors_internal_db = util.InstructorsInternalDb(['last_culpa_search', 'last_culpa_profile'])
        df = df.join(self.instructors_internal_db.df_internal.set_index('name'), on="name")

        # select instructors without link
        df_nolink = df[df['culpa_link'].isnull()]

        # check every few days
        def recent_threshold(x):
            if pd.isna(x):
                return True
            return x < datetime.datetime.now() - datetime.timedelta(days=random.randint(3, 7))
        df_nolink = df_nolink[df_nolink['last_culpa_search'].apply(recent_threshold)]

        # yield instructors without links
        def _yield(x):
            _, row = x
            self.instructors_internal_db.update_instructor(row['name'], 'last_culpa_search')
            return self._search_culpa_instructor(row['name'], row['departments'])
        yield from util.spider_run_loop(self, df_nolink.iterrows(), _yield)

        # check profiles of instructors with links
        df_link = df[df['culpa_link'].notnull()]

        def recent_threshold(x):
            if pd.isna(x):
                return True
            return x < datetime.datetime.now() - datetime.timedelta(days=random.randint(3, 7))
        df_link = df_link[
            df_link['last_culpa_profile'].apply(recent_threshold)
            ]

        def _yield(x):
            _, row = x
            self.instructors_internal_db.update_instructor(row['name'], 'last_culpa_profile')
            return self._check_culpa_instructor_profile(row['name'], row['culpa_link'])
        yield from util.spider_run_loop(self, df_link.iterrows(), _yield)

    def close(self, reason):
        if self.instructors_internal_db is not None:
            self.instructors_internal_db.store()

    def _search_culpa_instructor(self, instructor: str, departments: List[str], catalog_name=None):
        if catalog_name is None:
            catalog_name = instructor
        url = 'http://' + CulpaSearchSpider.SITE + '/search?utf8=✓&search=' \
              + urllib.parse.quote_plus(instructor) + '&commit=Search'
        return Request(url, callback=self.parse_culpa_search_instructor,
                       meta={'instructor': catalog_name,
                             'departments': departments})

    def _check_culpa_instructor_profile(self, instructor: str, url: str):
        return Request(url, callback=self.parse_culpa_instructor,
                      meta={'instructor': instructor,
                            'link': url})

    def parse_culpa_search_instructor(self, response):
        """
        @url http://culpa.info/search?utf8=%E2%9C%93&search=Ismail+C+Noyan&commit=Search
        @returns items 0 0
        @returns requests 1 1
        """
        self.crawler.stats.inc_value('culpa_searches')
        instructor = response.meta.get('instructor')
        if config.IN_TEST and not instructor:
            instructor = "Ismail V Noyan"

        # find professors search section
        found = None
        for box in response.css('.search_results .box'):
            section_name = box.css('th::text').get()
            if section_name.lower() == 'professors':
                found = box.css('tr td:first-child')

        # sort out not matching names
        if found:
            found_matching_names = SelectorList([])
            for result in found:
                found_name = result.css('a::text').get()
                if found_name and words_match2(instructor, found_name):
                    found_matching_names.append(result)
            if len(found_matching_names) > 1:
                logger.warning("More than 1 result for '%s' from %s on CULPA",
                               instructor,
                               response.meta.get('departments'))
            if len(found_matching_names) > 0:
                link = found_matching_names.css('a::attr(href)').get()
                url = 'http://' + CulpaSearchSpider.SITE + link
                yield self._check_culpa_instructor_profile(response.meta.get('instructor'), url)
                return

        # if not found try to remove middle name if exists and search again
        name = instructor.split()
        name2 = [w for w in name if len(w) > 1]
        if len(name) > len(name2):
            yield self._search_culpa_instructor(" ".join(name2), response.meta.get('departments'),
                                                catalog_name=instructor)

    def parse_culpa_instructor(self, response):
        """
        @url http://culpa.info/professors/3126
        @returns items 1 1
        @returns requests 0 0
        """
        # Idea: we could classify reviews sentiment if we capture review texts here
        self.crawler.stats.inc_value('culpa_profiles_loaded')

        nugget_html = response.css('h1 img.nugget::attr(alt)').get()
        nugget = None
        if nugget_html:
            if nugget_html.upper().startswith("GOLD"):
                nugget = CulpaInstructor.NUGGET_GOLD
            if nugget_html.upper().startswith("SILVER"):
                nugget = CulpaInstructor.NUGGET_SILVER

        # extract reviews
        reviews = []
        reviews_html = response.css('div.professor .review')
        for review_html in reviews_html:
            course_codes = []
            for meta in review_html.css('.meta .subject a'):
                course_name = meta.css('span.course_name ::text').get()
                course_code = meta.css('span.course_number ::text').get()
                crs = {}
                if course_code:
                    # remove [ ]
                    if course_code.startswith("[") and course_code.endswith("]"):
                        course_code = course_code[1:-1]
                    course_code = course_code.split(' ')
                    # transform MATH UN 1207 to MATH UN1207
                    course_code = course_code[0] + " " + "".join(course_code[1:])
                    crs['c'] = course_code
                if course_name:
                    crs['t'] = course_name

                course_codes.append(crs)

            review_extracted = review_html.css('.review_content ::text').getall()
            review_extracted = [r.strip() for r in review_extracted if len(r.strip()) > 0]

            if 'Workload:' in review_extracted:
                text = review_extracted[:review_extracted.index('Workload:')]
                workload = review_extracted[review_extracted.index('Workload:')+1:]
                workload = "\n".join(workload)
            else:
                text = review_extracted
                workload = None
            text = "\n".join(text)

            pub_date = review_html.css('p.date::text').get().strip()
            pub_date = datetime.datetime.strptime(pub_date, '%B %d, %Y').date()

            def _get_counter(counter_name: str) -> int:
                counter = review_html.css('input.' + counter_name + '::attr(value)').get()
                count = ''.join(filter(lambda i: i.isdigit(), counter))
                return int(count) if count else 0
            agree_count = _get_counter('agree')
            disagree_count = _get_counter('disagree')
            funny_count = _get_counter('funny')

            review = CulpaInstructor.Review(
                text=text,
                workload=workload,
                course_codes=course_codes,
                publish_date=pub_date,
                agree_count=agree_count,
                disagree_count=disagree_count,
                funny_count=funny_count
            )
            reviews.append(review.to_dict())

        yield CulpaInstructor(
            name=response.meta.get('instructor'),
            link=response.meta.get('link'),
            reviews=reviews,
            nugget=nugget
        )
