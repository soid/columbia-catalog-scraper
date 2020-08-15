# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html
from __future__ import annotations

import logging
import re

import scrapy
from scrapy.http import TextResponse
from w3lib.html import remove_tags

from cu_catalog import config

logger = logging.getLogger(__name__)


class ColumbiaDepartmentListing(scrapy.Item):
    department_code = scrapy.Field()
    term_month = scrapy.Field()  # fall / spring
    term_year = scrapy.Field()
    raw_content = scrapy.Field()  # html

    def term_str(self):
        return self['term_year'] + '-' + self['term_month']

    def describe(self):
        return self.term_str() + "-" + self['department_code']

    @staticmethod
    def get_from_response_meta(response):
        return response.meta.get('department_listing',
                                 ColumbiaDepartmentListing(
                                     department_code="TEST",
                                     term_month="testing",
                                     term_year="testing",
                                     raw_content="test content")
                                 if config.IN_TEST else None)


class ColumbiaClassListing(scrapy.Item):
    department_listing = scrapy.Field()
    class_id = scrapy.Field()

    instructor = scrapy.Field()
    course_title = scrapy.Field()
    course_descr = scrapy.Field()
    scheduled_days = scrapy.Field()
    scheduled_time = scrapy.Field()
    location = scrapy.Field()
    points = scrapy.Field()
    type = scrapy.Field()
    department = scrapy.Field()
    call_number = scrapy.Field()
    open_to = scrapy.Field()
    campus = scrapy.Field()
    method_of_instruction = scrapy.Field()

    raw_content = scrapy.Field()  # html

    def describe(self):
        return self['class_id']

    class _Parser:
        def __init__(self, response: TextResponse):
            self.response = response

            # parse rows of information
            self.class_id = [p for p in response.url.split('/') if len(p) > 0][-1]
            self.content_all = [tr.css('td') for tr in response.css('tr')]
            self.content = filter(lambda x: len(x) > 1, self.content_all)  # non-fields have only 1 td tag
            self.fields = {field_name.css('::text').get(): field_value
                           for field_name, field_value in self.content}

            # parse all fields
            self.instructor = self._get_field('Instructor', first_line_only=True)
            if self.instructor:
                self.instructor = re.sub(r'[\s-]+$', '', self.instructor)  # clean up
            self.course_title = self._get_course_title()

            # schedule
            date_and_location = self._get_field('Day & Time')
            if date_and_location:
                tmp = date_and_location.split("\n")  # e.g.: TR 4:10pm-5:25pm\n825 Seeley W. Mudd Building
                date_and_time = tmp[0]  # e.g.: TR 4:10pm-5:25pm
                self.scheduled_days = date_and_time[0]  # e.g. TR
                self.scheduled_time = date_and_time[1]  # e.g. 4:10pm-5:25pm

                self.location = date_and_location.split("\n")[1]

            self.open_to = self._get_field("Open To")
            if self.open_to:
                self.open_to = [s.strip() for s in self.open_to.split(',')]

            self.course_descr = self._get_field("Course Description")
            self.points = self._get_field("Points")
            self.class_type = self._get_field("Type")
            self.method_of_instruction = self._get_field("Method of Instruction")
            self.department = self._get_field("Department")
            self.call_number = self._get_field("Call Number")
            self.campus = self._get_field("Campus")

        def _get_field(self, field_name, first_line_only=False):
            if field_name in self.fields:
                v = self.fields[field_name].css('::text')
                return v.get() if first_line_only else "\n".join(v.getall())

        def _get_course_title(self):
            rows_with_1_col = filter(lambda x: len(x) == 1, self.content_all)
            fonts = [r.css('font[size*="+2"]::text').getall() for r in rows_with_1_col]
            lines = [line for lines in fonts for line in lines]
            if len(lines) > 1:
                logger.warning("More than one line in identified course title: %s", lines)
            return lines[0]

    @staticmethod
    def get_from_response(response: TextResponse) -> ColumbiaClassListing:
        # parse the class listing into fields
        class_parser = ColumbiaClassListing._Parser(response)

        class_listing = ColumbiaClassListing(
            class_id=class_parser.class_id,
            instructor=class_parser.instructor,
            course_title=class_parser.course_title,
            course_descr=class_parser.course_descr,
            scheduled_days=class_parser.scheduled_days,
            scheduled_time=class_parser.scheduled_time,
            location=class_parser.location,
            points=class_parser.points,
            type=class_parser.class_type,
            department=class_parser.department,
            call_number=class_parser.call_number,
            open_to=class_parser.open_to,
            campus=class_parser.campus,
            method_of_instruction=class_parser.method_of_instruction,
            department_listing=ColumbiaDepartmentListing.get_from_response_meta(response),
            raw_content=response.body_as_unicode()
        )
        return class_listing

    @staticmethod
    def get_from_response_meta(response):
        return response.meta.get('class_listing',
                                 ColumbiaClassListing(
                                     department_listing=ColumbiaDepartmentListing.get_from_response_meta(response),
                                     department='test dep',
                                     instructor="testing instr",
                                     class_id="testing class",
                                     course_descr="test content")
                                 if config.IN_TEST else None)


class CulpaInstructor(scrapy.Item):
    name = scrapy.Field()
    link = scrapy.Field()
    reviews_count = scrapy.Field()
    nugget = scrapy.Field()

    NUGGET_GOLD = 'gold'
    NUGGET_SILVER = 'silver'


class WikipediaInstructorSearchResults(scrapy.Item):
    name = scrapy.Field()
    class_listing = scrapy.Field()
    search_results = scrapy.Field()  # contains 'title' and 'snippet' fields

    def to_json(self) -> dict:
        return {
            'name': self['name'],
            'course_descr': self['class_listing']['course_descr'],
            'department': self['class_listing']['department'],
            'search_results': [{'title': r['title'], 'snippet': remove_tags(r['snippet'])}
                               for r in self['search_results']]
        }


class WikipediaInstructorPotentialArticle(scrapy.Item):
    name = scrapy.Field()
    class_listing = scrapy.Field()
    wikipedia_title = scrapy.Field()
    wikipedia_raw_page = scrapy.Field()

    def to_json(self) -> dict:
        return {
            'name': self['name'],
            'department': self['class_listing']['department'],
            'course_descr': self['class_listing']['course_descr'],
            'wiki_title': self['wikipedia_title'],
            'wiki_page': self['wikipedia_raw_page'],
        }


class WikipediaInstructorArticle(scrapy.Item):
    name = scrapy.Field()
    wikipedia_title = scrapy.Field()
