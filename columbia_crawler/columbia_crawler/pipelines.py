# -*- coding: utf-8 -*-

import datetime
import json
import os
import difflib
import logging

from w3lib.html import remove_tags

from cu_catalog import config
from columbia_crawler.items import ColumbiaClassListing, ColumbiaDepartmentListing, WikipediaInstructorSearchResults, \
    WikipediaInstructorPotentialArticle

logger = logging.getLogger(__name__)


class StoreRawListeningPipeline(object):

    @staticmethod
    def _get_last_file(path):
        """ Gets the last file from directory (sorted alphabetically)

        >>> last_file = StoreRawListeningPipeline._get_last_file(config.get_testcase_dir("content-diff"))
        >>> type(last_file) == str and len(last_file) > 5
        True
        >>> last_file.endswith("/content-diff/2020-07-10_18:35_UTC.html")
        True
        """
        files = [fn for fn in os.listdir(path) if os.path.isfile(path + '/' + fn)]
        if len(files) > 0:
            return path + '/' + sorted(files)[-1]
        else:
            return None

    @staticmethod
    def _is_different(content1, content2):
        """ Checks if content is different.

        >>> first_file = config.get_testcase_dir("content-diff") + "/2020-06-28_19:37_UTC.html"
        >>> same_file = config.get_testcase_dir("content-diff") + "/2020-07-10_07:03_UTC.html"
        >>> last_file = StoreRawListeningPipeline._get_last_file(config.get_testcase_dir("content-diff"))
        >>> assert first_file != last_file and same_file != last_file and same_file != first_file
        >>> first_content = open(first_file, "r").read()
        >>> same_content = open(same_file, "r").read()
        >>> last_content = open(last_file, "r").read()
        >>> StoreRawListeningPipeline._is_different(first_content, same_content)
        False
        >>> StoreRawListeningPipeline._is_different(first_content, last_content)
        True
        """
        seq_differ = difflib.SequenceMatcher()
        seq_differ.set_seqs(content1.split("\n"), content2.split("\n"))
        if seq_differ.ratio() == 1.0:
            # TODO: check it's not only generation date is different
            return False
        return True

    def process_item(self, item, spider):
        if isinstance(item, ColumbiaDepartmentListing):
            self.process_department_listing(item)
        if isinstance(item, ColumbiaClassListing):
            self.process_class_listing(item)
        return item

    def process_department_listing(self, item):
        out_dir = config.DATA_RAW_DIR + "/" + item.term_str() + "/" + item['department_code']
        self._store(out_dir, item['raw_content'], item.describe())
        return item

    def process_class_listing(self, item):
        department_listing = item['department_listing']
        out_dir = config.DATA_RAW_DIR + "/" + department_listing.term_str() \
                  + "/" + department_listing['department_code'] + "/" + item['class_id']
        self._store(out_dir, item['raw_content'], item.describe())
        return item

    def _store(self, out_dir, raw_content, description):
        os.makedirs(out_dir, exist_ok=True)
        out_file = out_dir + "/" + datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d_%H:%M_%Z') + '.html'

        last_file = StoreRawListeningPipeline._get_last_file(out_dir)
        if last_file is not None:
            lf = open(last_file, "r")
            if not self._is_different(lf.read(), raw_content):
                logger.info("%s listing has the same content. Skipping storing", description)
                return

        f = open(out_file, "w")
        f.write(raw_content)
        f.close()


class StoreWikiSearchResultsPipeline(object):
    def open_spider(self, spider):
        os.makedirs(config.DATA_WIKI_DIR, exist_ok=True)
        if config.IN_TEST:
            self.file_wiki_search = open('/tmp/file_wiki_search.test', 'w')
            self.file_wiki_article = open('/tmp/file_wiki_article.test', 'w')
        else:
            self.file_wiki_search = open(config.DATA_WIKI_SEARCH_FILENAME, 'w')
            self.file_wiki_article = open(config.DATA_WIKI_ARTICLE_FILENAME, 'w')

    def close_spider(self, spider):
        self.file_wiki_search.close()

    def process_item(self, item, spider):
        if isinstance(item, WikipediaInstructorSearchResults):
            s = json.dumps({
                'name': item['name'],
                'course_descr': item['class_listing']['course_descr'],
                'department': item['class_listing']['department'],
                'search_results': [{'title': r['title'], 'snippet': remove_tags(r['snippet'])}
                                   for r in item['search_results']]
            })
            self.file_wiki_search.write(s + '\n')

        if isinstance(item, WikipediaInstructorPotentialArticle):
            s = json.dumps({
                'name': item['name'],
                'department': item['class_listing']['department'],
                'course_descr': item['class_listing']['course_descr'],
                'wiki_title': item['wikipedia_title'],
                'wiki_page': item['wikipedia_raw_page'],
            })
            self.file_wiki_article.write(s + '\n')
