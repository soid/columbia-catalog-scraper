# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


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
    raw_content = scrapy.Field()  # html

    def describe(self):
        return self['class_id']
