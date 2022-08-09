# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html
from __future__ import annotations

import datetime
import logging
import random
import re
from typing import List

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

    def term_str(self) -> str:
        return self['term_year'] + '-' + self['term_month']

    def describe(self) -> str:
        return self.term_str() + "-" + self['department_code']

    def __repr__(self):
        return repr({name: self[name] for name in self.fields.keys()
                     if name != 'raw_content'})

    @staticmethod
    def get_test():
        item = get_test_item(ColumbiaDepartmentListing)
        item['term_month'] = random.choice(['Fall', 'Spring'])
        item['term_year'] = random.choice(['1920', '1921'])
        return item

    @staticmethod
    def get_from_response_meta(response) -> ColumbiaDepartmentListing or None:
        return response.meta.get('department_listing',
                                 ColumbiaDepartmentListing.get_test()
                                 if config.IN_TEST else None)


class ColumbiaClassListing(scrapy.Item):
    department_listing = scrapy.Field()
    class_id = scrapy.Field()
    link = scrapy.Field()
    course_code = scrapy.Field()
    section_key = scrapy.Field()

    instructor = scrapy.Field()
    course_title = scrapy.Field()
    course_subtitle = scrapy.Field()
    course_descr = scrapy.Field()
    prerequisites = scrapy.Field()
    department = scrapy.Field()
    department_code = scrapy.Field()
    scheduled_days = scrapy.Field()
    scheduled_time_start = scrapy.Field()
    scheduled_time_end = scrapy.Field()
    location = scrapy.Field()
    points = scrapy.Field()
    type = scrapy.Field()
    call_number = scrapy.Field()
    open_to = scrapy.Field()
    campus = scrapy.Field()
    method_of_instruction = scrapy.Field()
    enrollment = scrapy.Field()

    raw_content = scrapy.Field()  # html

    RE_ENROLLMENT = re.compile(r'(\d+) [^\d]+(\((\d+)[^\d]+\))?[^\d]+')

    def describe(self) -> str:
        return self['class_id']

    def __repr__(self):
        return repr({name: self[name] for name in self.fields.keys()
                     if name != 'raw_content'})

    def to_dict(self) -> dict:
        """Converts to dictionary and excludes raw_content field"""
        result = {}
        exclude_fields = ['raw_content', 'department_listing']
        for name in self.fields.keys():
            if name not in exclude_fields:
                result[name] = self[name]
        return result

    class _Parser:
        def __init__(self, response: TextResponse):
            self.response = response

            # parse the class table
            self.content_all = [tr.css('td') for tr in response.css('tr')]
            self.content = filter(lambda x: len(x) == 2, self.content_all)  # non-fields have only 1 td tag
            self.fields = {field_name.css('::text').get(): field_value
                           for field_name, field_value in self.content}

            # main fields
            self.class_id = [p for p in response.url.split('/') if len(p) > 0][-1]
            self.department_code = [p for p in response.url.split('/') if len(p) > 0][-2]
            self.course_code = self.department_code + " " + self._get_field("Number")

            # parse all fields
            self.instructor = self._get_field('Instructor', first_line_only=True)
            if self.instructor:
                self.instructor = re.sub(r'[\s-]+$', '', self.instructor)  # clean up
            self.course_title = self._get_course_title()
            self.course_subtitle = self._get_course_subtitle()

            # schedule
            self.scheduled_days = None
            self.scheduled_time_start = None
            self.scheduled_time_end = None
            self.location = None
            date_and_location = self._get_field('Day & Time')
            if date_and_location:
                tmp = date_and_location.split("\n")  # e.g.: TR 4:10pm-5:25pm\n825 Seeley W. Mudd Building
                date_and_time = tmp[0].split()  # e.g.: TR 4:10pm-5:25pm
                self.scheduled_days = date_and_time[0]  # e.g. TR
                t = date_and_time[1]  # e.g. 4:10pm-5:25pm
                self.scheduled_time_start, self.scheduled_time_end = t.split('-')
                self.location = None
                if len(tmp)>1:
                    self.location = tmp[1]

            self.section_key = self._get_field("Section key")

            self.open_to = self._get_field("Open To")
            if self.open_to:
                self.open_to = [s.strip() for s in self.open_to.split(',')]

            self.course_descr = self._get_field("Course Description")
            self.prerequisites = []
            if self.course_descr:
                self.prerequisites = ColumbiaClassListing.get_prerequisites(self.course_descr)
            self.points = self._get_field("Points")
            self.class_type = self._get_field("Type")
            self.method_of_instruction = self._get_field("Method of Instruction")
            self.department = self._get_field("Department")
            self.call_number = self._get_field("Call Number")
            self.campus = self._get_field("Campus")

            enrollment_string = self._get_field('Enrollment')
            self.enrollment_date, self.enrollment_current, self.enrollment_max \
                = self._get_enrollment(enrollment_string)

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
            if len(lines) == 0:
                return ''  # no title
            course_title = lines[0]
            return course_title

        def _get_course_subtitle(self):
            rows_with_1_col = filter(lambda x: len(x) == 1, self.content_all)
            itag = [r.css('i::text').getall() for r in rows_with_1_col]
            sub_lines = [line for lines in itag for line in lines]
            if len(sub_lines) > 0:
                return sub_lines[0]
            return None

        @staticmethod
        def _get_enrollment(enrollment_string: str):
            """

            >>> ColumbiaClassListing._Parser._get_enrollment( \
                "0 students (10 max) as of 12:04PM Thursday, May 20, 2021")
            (datetime.date(2021, 5, 20), 0, 10)
            >>> ColumbiaClassListing._Parser._get_enrollment( \
                "0 students as of  8:04AM Saturday, May 22, 2021")
            (datetime.date(2021, 5, 22), 0, None)
            >>> ColumbiaClassListing._Parser._get_enrollment("")
            (None, None, None)
            >>> ColumbiaClassListing._Parser._get_enrollment("axcvse")
            (None, None, None)
            """
            if enrollment_string:
                m = ColumbiaClassListing.RE_ENROLLMENT.match(enrollment_string)
                if m:
                    enrollment_current = int(m.group(1))
                    if m.group(3):
                        enrollment_max = int(m.group(3))
                    else:
                        enrollment_max = None
                    date_str = enrollment_string.split('as of', 1)[1].strip()
                    enrollment_date = datetime.datetime.strptime(date_str, '%I:%M%p %A, %B %d, %Y').date()
                    enrollment_date = enrollment_date
                    return enrollment_date, enrollment_current, enrollment_max
                else:
                    logger.warning("Didn't match enrollment string: %s", enrollment_string)
            return None, None, None

    PREREQ_PATTERN = re.compile(r'([A-Z]{4} [A-Z][A-Z]?[0-9]{4}|[A-Z][A-Z]?[0-9]{4}|[oO][rR]|[aA][nN][dD])')

    @staticmethod
    def get_prerequisites(course_descr: str) -> List[List[str]]:
        """Returns a list of prerequisites from a course description as a list of class codes.
        The list is conjunction of other lists that contain a disjunction of classes.
        E.g. [['A', 'B'], ['C'], ['D']] means (A or B) and C and D.

        >>> course_descr = "Prerequisites: (COMS W3134) or (COMS W3137) C programming language and "\
        "Unix systems programming. Also covers Git, Make, TCP/IP networking basics, C++ fundamentals."
        >>> ColumbiaClassListing.get_prerequisites(course_descr)
        [['COMS W3134', 'COMS W3137']]
        >>> course_descr = "Prerequisites: (COMS W3134) and (COMS W3137) blah blah blah"
        >>> ColumbiaClassListing.get_prerequisites(course_descr)
        [['COMS W3134'], ['COMS W3137']]
        """
        matches = ColumbiaClassListing.PREREQ_PATTERN.findall(course_descr)

        prereqs = []
        mode = "and"
        for m in matches:
            if m.lower() == "or" and len(prereqs) > 0:
                mode = "or"
            elif m.lower() == "and":
                mode = "and"
            else:
                if mode == "and":
                    if m.lower() != 'and' and m.lower() != 'or':
                        prereqs.append([m])
                        mode = "and"
                else:
                    last = prereqs[-1]
                    if type(last) == list:
                        last.append(m)
                    else:
                        prereqs[-1] = [prereqs[-1], m]
                    mode = "and"

        return prereqs

    @staticmethod
    def get_from_response(response: TextResponse) -> ColumbiaClassListing:
        # parse the class listing into fields
        class_parser = ColumbiaClassListing._Parser(response)

        class_listing = ColumbiaClassListing(
            class_id=class_parser.class_id,
            link=response.url,
            course_code=class_parser.course_code,
            section_key=class_parser.section_key,
            instructor=class_parser.instructor,
            course_title=class_parser.course_title,
            course_subtitle=class_parser.course_subtitle,
            course_descr=class_parser.course_descr,
            prerequisites=class_parser.prerequisites,
            department=class_parser.department,
            department_code=class_parser.department_code,
            scheduled_days=class_parser.scheduled_days,
            scheduled_time_start=class_parser.scheduled_time_start,
            scheduled_time_end=class_parser.scheduled_time_end,
            location=class_parser.location,
            points=class_parser.points,
            type=class_parser.class_type,
            call_number=class_parser.call_number,
            open_to=class_parser.open_to,
            campus=class_parser.campus,
            method_of_instruction=class_parser.method_of_instruction,
            department_listing=ColumbiaDepartmentListing.get_from_response_meta(response),
            enrollment=ColumbiaClassListing._get_enrollment(class_parser.enrollment_date,
                                                            class_parser.enrollment_current,
                                                            class_parser.enrollment_max),
            raw_content=response.text
        )
        return class_listing

    @staticmethod
    def get_test():
        test_item = get_test_item(ColumbiaClassListing)
        test_item['enrollment'] = ColumbiaClassListing._get_enrollment(datetime.date.today(),
                                                                       random.randint(1, 100),
                                                                       random.randint(100, 150))
        test_item['department_listing'] = ColumbiaDepartmentListing.get_test()
        return test_item

    @staticmethod
    def get_from_response_meta(response):
        return response.meta.get('class_listing',
                                 ColumbiaClassListing.get_test()
                                 if config.IN_TEST else None)

    @staticmethod
    def _get_enrollment(scan_date: datetime.date, current: int, max: int):
        return {
            scan_date.isoformat(): {
                'cur': current,
                'max': max,
            }
        }


class CulpaInstructor(scrapy.Item):
    name = scrapy.Field()
    link = scrapy.Field()
    reviews = scrapy.Field()
    reviews_count = scrapy.Field()
    nugget = scrapy.Field()

    NUGGET_GOLD = 'gold'
    NUGGET_SILVER = 'silver'

    class Review(scrapy.Item):
        text = scrapy.Field()
        workload = scrapy.Field()
        course_codes = scrapy.Field()
        publish_date = scrapy.Field()
        agree_count = scrapy.Field()
        disagree_count = scrapy.Field()
        funny_count = scrapy.Field()

        def to_dict(self) -> dict:
            return {
                'course_codes': self['course_codes'],
                'text':     self['text'],
                'workload': self['workload'],
                'publish_date': self['publish_date'].isoformat(),
                'agree_count':  self['agree_count'],
                'disagree_count':   self['disagree_count'],
                'funny_count':      self['funny_count'],
            }

    @staticmethod
    def get_test():
        test_item = get_test_item(CulpaInstructor)
        test_item['reviews_count'] = random.randint(1, 999)
        test_item['nugget'] = random.choice([None, CulpaInstructor.NUGGET_GOLD, CulpaInstructor.NUGGET_SILVER])
        return test_item


class WikipediaInstructorSearchResults(scrapy.Item):
    name = scrapy.Field()
    department = scrapy.Field()
    search_results = scrapy.Field()  # contains 'title' and 'snippet' fields

    def __repr__(self):
        return repr({name: self[name] for name in self.fields.keys()
                     if name != 'search_results'})

    def to_dict(self) -> dict:
        return {
            'name': self['name'],
            'course_descr': '',  # TODO: add course info
            'department': self['department'],
            'search_results': [{'title': r['title'], 'snippet': remove_tags(r['snippet'])}
                               for r in self['search_results']]
        }


class WikipediaInstructorPotentialArticle(scrapy.Item):
    name = scrapy.Field()
    department = scrapy.Field()
    wikipedia_title = scrapy.Field()
    wikipedia_raw_page = scrapy.Field()

    def __repr__(self):
        return repr({name: self[name] for name in self.fields.keys()
                     if name != 'wikipedia_raw_page'})

    def to_dict(self, exclude_fields=None) -> dict:
        if exclude_fields is None:
            exclude_fields = []
        result = {
            'name': self['name'],
            'department': self['department'],
            'course_descr': '',  # TODO: add courses description (taught by instructor)
            'wiki_title': self['wikipedia_title'],
            'wiki_page': self['wikipedia_raw_page'],
        }
        return {name: val for name, val in result.items() if name not in exclude_fields}


class WikipediaInstructorArticle(scrapy.Item):
    name = scrapy.Field()
    department = scrapy.Field()
    wikipedia_title = scrapy.Field()

    @staticmethod
    def get_test():
        return get_test_item(WikipediaInstructorArticle)


def get_test_item(cls):
    d = {}
    suffix = '_' + str(random.randint(1, 999))
    for name in cls.fields.keys():
        d[name] = "test " + name + suffix
    return cls(d)
