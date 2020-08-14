# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy
from w3lib.html import remove_tags


class ColumbiaDepartmentListing(scrapy.Item):
    department_code = scrapy.Field()
    term_month = scrapy.Field()  # fall / spring
    term_year = scrapy.Field()
    raw_content = scrapy.Field()  # html

    def term_str(self):
        return self['term_year'] + '-' + self['term_month']

    def describe(self):
        return self.term_str() + "-" + self['department_code']


class ColumbiaClassListing(scrapy.Item):
    department_listing = scrapy.Field()
    class_id = scrapy.Field()

    instructor = scrapy.Field()
    course_title = scrapy.Field()
    course_descr = scrapy.Field()
    datetime = scrapy.Field()
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
