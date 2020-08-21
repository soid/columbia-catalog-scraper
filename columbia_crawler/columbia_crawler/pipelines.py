# -*- coding: utf-8 -*-

import datetime
import json
import os
import difflib
import logging
from urllib.parse import quote_plus
from collections import defaultdict
import numpy as np
import pandas as pd
from typing import Dict, List

from cu_catalog import config
from columbia_crawler.items import ColumbiaClassListing, ColumbiaDepartmentListing, WikipediaInstructorSearchResults, \
    WikipediaInstructorPotentialArticle, WikipediaInstructorArticle, CulpaInstructor

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
        self.file_wiki_search = open(config.DATA_WIKI_SEARCH_FILENAME, 'w')
        self.file_wiki_article = open(config.DATA_WIKI_ARTICLE_FILENAME, 'w')

    def close_spider(self, spider):
        self.file_wiki_search.close()

    def process_item(self, item, spider):
        if isinstance(item, WikipediaInstructorSearchResults):
            s = json.dumps(item.to_dict())
            self.file_wiki_search.write(s + '\n')

        if isinstance(item, WikipediaInstructorPotentialArticle):
            s = json.dumps(item.to_dict())
            self.file_wiki_article.write(s + '\n')
        return item


class StoreClassPipeline(object):
    def open_spider(self, spider):
        self.classes_in_term: Dict[List[object]] = defaultdict(lambda: [], {})
        self.instructors = defaultdict(lambda: {}, {})

        # mapper from instructor name to classes ref in self.classes_in_term
        self.instr2classes = defaultdict(lambda: [], {})

    def process_item(self, item, spider):
        if isinstance(item, ColumbiaClassListing):
            department_listing = item['department_listing']
            term = department_listing.term_str()
            cls = item.to_dict()
            self.classes_in_term[term].append(cls)

            if item['instructor']:
                instr = self.instructors[item['instructor']]
                instr['name'] = item['instructor']
                instr.setdefault('departments', set())
                instr['departments'].add(item['department'])
                instr.setdefault('classes', [])
                instr['classes'].append([term, item['course_code']])

                self.instr2classes[item['instructor']].append(cls)

        if isinstance(item, WikipediaInstructorArticle):
            instr = self.instructors[item['name']]
            instr['name'] = item['name']
            wikipedia_link = 'https://en.wikipedia.org/wiki/' \
                             + quote_plus(item['wikipedia_title'].replace(' ', '_'))
            instr['wikipedia_link'] = wikipedia_link

            for cls in self.instr2classes[instr['name']]:
                cls['instructor_wikipedia_link'] = wikipedia_link

        if isinstance(item, CulpaInstructor):
            instr = self.instructors[item['name']]
            instr['name'] = item['name']
            instr['culpa_link'] = 'http://culpa.info' + item['link']
            instr['culpa_nugget'] = item['nugget']
            instr['culpa_reviews_count'] = int(item['reviews_count'])

            for cls in self.instr2classes[instr['name']]:
                cls['instructor_culpa_link'] = 'http://culpa.info' + item['link']
                cls['instructor_culpa_nugget'] = item['nugget']
                cls['instructor_culpa_reviews_count'] = int(item['reviews_count'])

        return item

    def close_spider(self, spider):
        # store classes
        os.makedirs(config.DATA_CLASSES_DIR, exist_ok=True)
        for term, classes in self.classes_in_term.items():
            df = pd.DataFrame(classes)
            df.sort_values(by=['course_code', 'course_title'], inplace=True)

            # reorder columns
            df = StoreClassPipeline\
                ._change_cols_order(df, ['course_code', 'course_title', 'course_descr', 'instructor',
                                         'scheduled_time_start', 'scheduled_time_end'])
            df['open_to'] = df['open_to'] \
                .apply(lambda x: "\n".join(sorted(x)) if np.all(pd.notna(x)) else x)
            df['prerequisites'] = df['prerequisites'] \
                .apply(lambda prereqs: "\n".join([c for cls in prereqs for c in cls])
                                       if np.all(pd.notna(prereqs)) else prereqs)

            StoreClassPipeline._store_df(config.DATA_CLASSES_DIR + '/' + term, df)

        # store instructors
        os.makedirs(config.DATA_INSTRUCTORS_DIR, exist_ok=True)
        df = pd.DataFrame(self.instructors.values())
        df['departments'] = df['departments']\
            .apply(lambda x: "\n".join(sorted(x)) if pd.notna(x) else x)
        df['classes'] = df['classes']\
            .apply(lambda x: ("\n".join([" ".join(cls) for cls in x]) if np.all(pd.notna(x)) else x))
        df.sort_values(by=['name'], inplace=True)
        StoreClassPipeline._store_df(config.DATA_INSTRUCTORS_DIR + '/instructors', df)

    @staticmethod
    def _change_cols_order(df, prioritized_cols):
        cols = sorted(df.columns)
        for n in prioritized_cols:
            cols.remove(n)
        cols = prioritized_cols + cols
        df = df.reindex(cols, axis=1)
        return df

    @staticmethod
    def _store_df(filename: str, df: pd.DataFrame):
        # store json
        file_json = open(filename + '.json', 'w')
        df.to_json(path_or_buf=file_json, orient="records", lines=True)
        file_json.close()

        # store csv
        file_csv = open(filename + '.csv', 'w')
        df.to_csv(path_or_buf=file_csv, index=False)
        file_csv.close()
